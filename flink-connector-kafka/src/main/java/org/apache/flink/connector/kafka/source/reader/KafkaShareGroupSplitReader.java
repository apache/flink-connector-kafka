/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplit;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced SplitReader implementation for Kafka Share Groups with at-least-once semantics.
 * 
 * <p>This reader provides checkpoint-based acknowledgment using metadata-only checkpointing
 * following the Pulsar connector pattern. Records are cached with configurable memory bounds and only
 * acknowledged to Kafka after successful Flink checkpoint completion.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Metadata-only checkpointing for memory efficiency</li>
 *   <li>Configurable memory-bounded record caching (default: 10MB, configurable via 'flink.share.group.cache.max.memory.bytes')</li>
 *   <li>Checkpoint-coordinated acknowledgments</li>
 *   <li>Automatic record release on checkpoint failures</li>
 *   <li>Record release for redelivery under memory pressure (maintains at-least-once)</li>
 *   <li>Automatic cache cleanup to prevent memory leaks</li>
 * </ul>
 * 
 * <p>Memory pressure handling:
 * When cache memory limit is exceeded, records are released back to Kafka using {@code AcknowledgeType.RELEASE}
 * for immediate redelivery to maintain at-least-once delivery guarantees. This prevents OOM errors while
 * ensuring no data loss.
 * 
 * <p>Configuration:
 * <ul>
 *   <li><code>flink.share.group.cache.max.memory.bytes</code> - Maximum memory for record cache (default: 10485760 = 10MB)</li>
 * </ul>
 */
@Internal
public class KafkaShareGroupSplitReader implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaShareGroupSplit> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSplitReader.class);
    private static final long POLL_TIMEOUT_MS = 100L;
    private static final Duration CLEANUP_AGE_THRESHOLD = Duration.ofMinutes(10);
    
    private final KafkaShareConsumer<byte[], byte[]> shareConsumer;
    private final String shareGroupId;
    private final int readerId;
    private final Map<String, KafkaShareGroupSplit> assignedSplits;
    private final Set<String> subscribedTopics;
    private final KafkaShareGroupSourceMetrics metrics;
    
    // At-least-once semantics state - Pulsar-style metadata only
    private final Map<Long, Map<TopicPartition, AcknowledgmentMetadata>> acknowledgmentsToCommit;
    private final ConcurrentHashMap<RecordKey, ConsumerRecord<byte[], byte[]>> recordCache;
    private volatile long currentCheckpointId = -1;
    private volatile long recordLockDurationMs;
    private volatile long currentCacheMemoryBytes = 0;
    
    private static final long DEFAULT_MAX_CACHE_MEMORY_BYTES = 10 * 1024 * 1024; // 10MB default cache limit
    private static final Duration CACHE_CLEANUP_AGE = Duration.ofMinutes(5);
    
    private final long maxCacheMemoryBytes;
    
    /**
     * Container for acknowledgment metadata following Pulsar pattern.
     */
    private static class AcknowledgmentMetadata {
        private final Set<Long> offsetsToAcknowledge;
        private final int recordCount;
        private final long timestamp;
        
        AcknowledgmentMetadata(Set<Long> offsetsToAcknowledge, int recordCount, long timestamp) {
            this.offsetsToAcknowledge = Collections.unmodifiableSet(new HashSet<>(offsetsToAcknowledge));
            this.recordCount = recordCount;
            this.timestamp = timestamp;
        }
        
        public Set<Long> getOffsetsToAcknowledge() {
            return offsetsToAcknowledge;
        }
        
        public int getRecordCount() {
            return recordCount;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("AcknowledgmentMetadata{offsets=%d, records=%d, timestamp=%d}", 
                    offsetsToAcknowledge.size(), recordCount, timestamp);
        }
    }
    
    /**
     * Key for record cache entries.
     */
    private static class RecordKey {
        private final String topic;
        private final int partition;
        private final long offset;
        private final int hashCode;
        
        RecordKey(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.hashCode = Objects.hash(topic, partition, offset);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            
            RecordKey recordKey = (RecordKey) obj;
            return partition == recordKey.partition &&
                   offset == recordKey.offset &&
                   Objects.equals(topic, recordKey.topic);
        }
        
        @Override
        public int hashCode() {
            return hashCode;
        }
        
        @Override
        public String toString() {
            return String.format("%s-%d-%d", topic, partition, offset);
        }
    }

    /**
     * Creates a share group split reader with at-least-once semantics.
     *
     * @param props consumer properties configured for share groups
     * @param context the source reader context
     * @param metrics metrics collector for share group operations
     */
    public KafkaShareGroupSplitReader(
            Properties props,
            SourceReaderContext context,
            @Nullable KafkaShareGroupSourceMetrics metrics) {
        
        this.readerId = context.getIndexOfSubtask();
        this.metrics = metrics;
        this.assignedSplits = new HashMap<>();
        this.subscribedTopics = new HashSet<>();
        
        // Configure share consumer properties
        Properties shareConsumerProps = new Properties();
        shareConsumerProps.putAll(props);
        
        // CRITICAL: Enable explicit acknowledgment mode for at-least-once semantics
        shareConsumerProps.setProperty("share.acknowledgement.mode", "explicit");
        shareConsumerProps.setProperty("group.type", "share");
        this.shareGroupId = shareConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        
        Preconditions.checkNotNull(shareGroupId, "Share group ID (group.id) must be specified");
        
        // Extract record lock duration for monitoring
        this.recordLockDurationMs = Long.parseLong(
            shareConsumerProps.getProperty("group.share.record.lock.duration.ms", "30000"));
        
        // Configure maximum cache memory (configurable)
        this.maxCacheMemoryBytes = Long.parseLong(
            shareConsumerProps.getProperty("flink.share.group.cache.max.memory.bytes", 
                                         String.valueOf(DEFAULT_MAX_CACHE_MEMORY_BYTES)));
        
        // Initialize Pulsar-style metadata tracking
        this.acknowledgmentsToCommit = new ConcurrentHashMap<>();
        this.recordCache = new ConcurrentHashMap<>();
        
        // Configure client ID for this reader
        shareConsumerProps.setProperty(
            ConsumerConfig.CLIENT_ID_CONFIG, 
            createShareConsumerClientId(shareConsumerProps)
        );
        
        // Remove properties not supported by share groups
        shareConsumerProps.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        
        // Create KafkaShareConsumer instance
        this.shareConsumer = createShareConsumer(shareConsumerProps);
        
        LOG.info("Created KafkaShareGroupSplitReader for share group '{}' reader {} with metadata-only checkpointing (lockDuration={}ms, cacheLimit={}MB)", 
                shareGroupId, readerId, recordLockDurationMs, maxCacheMemoryBytes / (1024 * 1024));
    }
    
    /**
     * Factory method for creating share consumer - allows mocking in tests.
     */
    protected KafkaShareConsumer<byte[], byte[]> createShareConsumer(Properties props) {
        return new KafkaShareConsumer<>(props);
    }

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        try {
            // Wait until at least one split is assigned
            if (assignedSplits.isEmpty()) {
                LOG.debug("Share group '{}' reader {} waiting for split assignment", shareGroupId, readerId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // Poll for records using KafkaShareConsumer API
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            
            if (consumerRecords.isEmpty()) {
                LOG.trace("Share group '{}' reader {} - no records returned from poll", shareGroupId, readerId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // Store records for checkpoint-based acknowledgment - DON'T acknowledge immediately!
            List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                recordList.add(record);
                if (metrics != null) {
                    metrics.recordMessageReceived();
                }
                LOG.trace("Share group '{}' reader {} received record - topic: {}, partition: {}, offset: {}", 
                        shareGroupId, readerId, record.topic(), record.partition(), record.offset());
            }
            
            // Store acknowledgment metadata instead of full records (following Pulsar pattern)
            if (currentCheckpointId >= 0 && !recordList.isEmpty()) {
                storeAcknowledgmentMetadata(currentCheckpointId, recordList);
                LOG.debug("Stored acknowledgment metadata for {} records in checkpoint {} for share group '{}'", 
                        recordList.size(), currentCheckpointId, shareGroupId);
            }
            
            LOG.debug("Share group '{}' reader {} fetched {} records", shareGroupId, readerId, recordList.size());
            
            // Use the first available split ID for all records
            String splitId = assignedSplits.keySet().iterator().next();
            return new ShareGroupRecordsWithSplitIds(consumerRecords.iterator(), splitId);
            
        } catch (WakeupException e) {
            LOG.debug("Share consumer '{}' reader {} woken up", shareGroupId, readerId);
            return ShareGroupRecordsWithSplitIds.empty();
        } catch (Exception e) {
            LOG.error("Error polling from KafkaShareConsumer for share group '{}' reader {}: {}", 
                    shareGroupId, readerId, e.getMessage(), e);
            throw new IOException("Failed to fetch records from share group: " + shareGroupId, e);
        }
    }
    
    /**
     * Called when Flink starts a checkpoint - associates subsequent records with checkpoint ID.
     */
    public void snapshotState(long checkpointId) {
        this.currentCheckpointId = checkpointId;
        LOG.debug("Share group '{}' reader {} starting checkpoint {}", shareGroupId, readerId, checkpointId);
    }
    
    /**
     * Called when Flink checkpoint completes successfully - acknowledge records using metadata.
     */
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Map<TopicPartition, AcknowledgmentMetadata> ackMetadata = acknowledgmentsToCommit.get(checkpointId);
        
        if (ackMetadata != null) {
            try {
                // Acknowledge records using cached records and metadata
                for (Map.Entry<TopicPartition, AcknowledgmentMetadata> entry : ackMetadata.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    AcknowledgmentMetadata metadata = entry.getValue();
                    
                    // Acknowledge cached records for this TopicPartition
                    for (Long offset : metadata.getOffsetsToAcknowledge()) {
                        RecordKey key = new RecordKey(tp.topic(), tp.partition(), offset);
                        ConsumerRecord<byte[], byte[]> record = recordCache.get(key);
                        
                        if (record != null) {
                            shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                            // Remove from cache after acknowledgment
                            ConsumerRecord<byte[], byte[]> removed = recordCache.remove(key);
                            if (removed != null) {
                                currentCacheMemoryBytes -= estimateRecordSize(removed);
                            }
                        } else {
                            LOG.debug("Record not found in cache for acknowledgment: {}", key);
                        }
                    }
                }
                
                // Commit acknowledgments asynchronously
                shareConsumer.commitAsync();
                
                // Clean up metadata
                acknowledgmentsToCommit.remove(checkpointId);
                
                LOG.debug("Acknowledged {} topic-partitions for checkpoint {} in share group '{}' (cache size: {} bytes)", 
                        ackMetadata.size(), checkpointId, shareGroupId, currentCacheMemoryBytes);
                
            } catch (Exception e) {
                LOG.error("Failed to acknowledge records for checkpoint {}: {}", checkpointId, e.getMessage());
                throw e;
            }
        }
    }
    
    /**
     * Called when Flink checkpoint fails - release records for redelivery using metadata.
     */
    public void notifyCheckpointAborted(long checkpointId, Throwable cause) {
        Map<TopicPartition, AcknowledgmentMetadata> ackMetadata = acknowledgmentsToCommit.remove(checkpointId);
        
        if (ackMetadata != null) {
            try {
                // Release records using cached records and metadata
                for (Map.Entry<TopicPartition, AcknowledgmentMetadata> entry : ackMetadata.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    AcknowledgmentMetadata metadata = entry.getValue();
                    
                    // Release cached records for this TopicPartition
                    for (Long offset : metadata.getOffsetsToAcknowledge()) {
                        RecordKey key = new RecordKey(tp.topic(), tp.partition(), offset);
                        ConsumerRecord<byte[], byte[]> record = recordCache.get(key);
                        
                        if (record != null) {
                            shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                            // Remove from cache after release
                            ConsumerRecord<byte[], byte[]> removed = recordCache.remove(key);
                            if (removed != null) {
                                currentCacheMemoryBytes -= estimateRecordSize(removed);
                            }
                        }
                    }
                }
                
                shareConsumer.commitAsync();
                
                LOG.info("Released {} topic-partitions for redelivery after checkpoint {} failure in share group '{}' (cache size: {} bytes)", 
                        ackMetadata.size(), checkpointId, shareGroupId, currentCacheMemoryBytes);
                        
            } catch (Exception e) {
                LOG.warn("Failed to release records after checkpoint failure: {}", e.getMessage());
                // Records will eventually timeout and be auto-released
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KafkaShareGroupSplit> splitsChanges) {
        LOG.info("Share group '{}' reader {} handling splits changes: {}", 
                shareGroupId, readerId, splitsChanges.getClass().getSimpleName());
        
        if (splitsChanges instanceof SplitsAddition) {
            handleSplitsAddition((SplitsAddition<KafkaShareGroupSplit>) splitsChanges);
        } else if (splitsChanges instanceof SplitsRemoval) {
            handleSplitsRemoval((SplitsRemoval<KafkaShareGroupSplit>) splitsChanges);
        } else {
            LOG.warn("Unsupported splits change type: {}", splitsChanges.getClass());
        }
    }
    
    private void handleSplitsAddition(SplitsAddition<KafkaShareGroupSplit> splitsAddition) {
        Collection<KafkaShareGroupSplit> newSplits = splitsAddition.splits();
        Set<String> newTopics = new HashSet<>();
        
        LOG.info("Share group '{}' reader {} received {} splits for topic subscription", 
                shareGroupId, readerId, newSplits.size());
        
        // Process splits and extract topics
        for (KafkaShareGroupSplit split : newSplits) {
            assignedSplits.put(split.splitId(), split);
            String topic = split.getTopicName();
            newTopics.add(topic);
            
            LOG.info("Share group '{}' reader {} assigned split: {} for topic: {}", 
                    shareGroupId, readerId, split.splitId(), topic);
        }
        
        // Update topic subscription
        subscribedTopics.addAll(newTopics);
        
        // Subscribe to topics using share group semantics
        if (!subscribedTopics.isEmpty()) {
            try {
                LOG.info("Share group '{}' reader {} subscribing to topics: {}", 
                        shareGroupId, readerId, subscribedTopics);
                        
                shareConsumer.subscribe(subscribedTopics);
                
                LOG.info("Share group '{}' reader {} successfully subscribed to topics {} - coordinator will distribute messages", 
                        shareGroupId, readerId, subscribedTopics);
            } catch (Exception e) {
                LOG.error("Failed to subscribe to topics for share group '{}' reader {}: {}", 
                        shareGroupId, readerId, e.getMessage(), e);
            }
        }
        
        LOG.info("Share group '{}' reader {} completed split addition - {} topics subscribed, {} total splits", 
                shareGroupId, readerId, subscribedTopics.size(), assignedSplits.size());
    }
    
    private void handleSplitsRemoval(SplitsRemoval<KafkaShareGroupSplit> splitsRemoval) {
        for (KafkaShareGroupSplit split : splitsRemoval.splits()) {
            String splitId = split.splitId();
            KafkaShareGroupSplit removedSplit = assignedSplits.remove(splitId);
            if (removedSplit != null) {
                LOG.debug("Share group '{}' reader {} removed split: {}", shareGroupId, readerId, splitId);
            }
        }
        
        LOG.debug("Share group '{}' reader {} removed {} splits, remaining: {}", 
                shareGroupId, readerId, splitsRemoval.splits().size(), assignedSplits.size());
    }

    @Override
    public void wakeUp() {
        shareConsumer.wakeup();
        LOG.debug("KafkaShareConsumer for share group '{}' reader {} woken up", shareGroupId, readerId);
    }

    @Override
    public void close() throws Exception {
        try {
            // Release all unacknowledged records for redistribution
            releaseUnacknowledgedRecords();
            
            shareConsumer.close(Duration.ofSeconds(5));
            LOG.info("KafkaShareConsumer for share group '{}' reader {} closed successfully", shareGroupId, readerId);
        } catch (Exception e) {
            LOG.warn("Error closing KafkaShareConsumer for share group '{}' reader {}: {}", 
                    shareGroupId, readerId, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Releases all unacknowledged records when closing the consumer.
     */
    private void releaseUnacknowledgedRecords() {
        for (ConsumerRecord<byte[], byte[]> record : recordCache.values()) {
            try {
                shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
            } catch (Exception e) {
                LOG.debug("Failed to release record on close: {}", e.getMessage());
                // Records will timeout anyway
            }
        }
        recordCache.clear();
        currentCacheMemoryBytes = 0;
        acknowledgmentsToCommit.clear();
    }
    
    /**
     * Stores acknowledgment metadata following Pulsar pattern.
     */
    private void storeAcknowledgmentMetadata(long checkpointId, List<ConsumerRecord<byte[], byte[]>> records) {
        Map<TopicPartition, AcknowledgmentMetadata> checkpointAcks = 
            acknowledgmentsToCommit.computeIfAbsent(checkpointId, k -> new ConcurrentHashMap<>());
        
        // Group records by TopicPartition and store in cache
        Map<TopicPartition, Set<Long>> offsetsByPartition = new LinkedHashMap<>();
        
        for (ConsumerRecord<byte[], byte[]> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            offsetsByPartition.computeIfAbsent(tp, k -> new HashSet<>()).add(record.offset());
            
            // Store record in bounded cache for acknowledgment
            storeRecordInCache(record, checkpointId);
        }
        
        // Create acknowledgment metadata for each TopicPartition
        for (Map.Entry<TopicPartition, Set<Long>> entry : offsetsByPartition.entrySet()) {
            TopicPartition tp = entry.getKey();
            Set<Long> offsets = entry.getValue();
            
            AcknowledgmentMetadata metadata = new AcknowledgmentMetadata(
                offsets, offsets.size(), System.currentTimeMillis());
            checkpointAcks.put(tp, metadata);
        }
    }
    
    /**
     * Stores record in memory-bounded cache.
     */
    private void storeRecordInCache(ConsumerRecord<byte[], byte[]> record, long checkpointId) {
        RecordKey key = new RecordKey(record.topic(), record.partition(), record.offset());
        long recordSize = estimateRecordSize(record);
        
        // Check memory bounds - if exceeded, release record for redelivery to maintain at-least-once
        if (currentCacheMemoryBytes + recordSize > maxCacheMemoryBytes) {
            try {
                // Release record back to Kafka for redelivery (maintains at-least-once semantics)
                shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                LOG.warn("Memory pressure detected - releasing record for redelivery: {} (cache: {} bytes, limit: {} bytes)", 
                        key, currentCacheMemoryBytes, maxCacheMemoryBytes);
                return;
            } catch (Exception e) {
                LOG.error("Failed to release record under memory pressure: {}, falling back to caching", e.getMessage());
                // Continue to cache anyway as fallback - risk OOM but maintain correctness
            }
        }
        
        // Store in cache for checkpoint-based acknowledgment
        ConsumerRecord<byte[], byte[]> existing = recordCache.put(key, record);
        if (existing == null) {
            currentCacheMemoryBytes += recordSize;
        }
        
        // Cleanup old cache entries periodically
        if (recordCache.size() % 1000 == 0) {
            cleanupOldCacheEntries();
        }
    }
    
    /**
     * Estimates memory size of a ConsumerRecord.
     */
    private long estimateRecordSize(ConsumerRecord<byte[], byte[]> record) {
        // Base object overhead + key + value + headers
        long size = 200; // Base overhead
        if (record.key() != null) {
            size += record.key().length;
        }
        if (record.value() != null) {
            size += record.value().length;
        }
        if (record.headers() != null) {
            size += record.headers().toString().length();
        }
        return size;
    }
    
    /**
     * Cleans up old cache entries to prevent memory leaks.
     */
    private void cleanupOldCacheEntries() {
        long cutoffTime = System.currentTimeMillis() - CACHE_CLEANUP_AGE.toMillis();
        
        recordCache.entrySet().removeIf(entry -> {
            ConsumerRecord<byte[], byte[]> record = entry.getValue();
            if (record.timestamp() < cutoffTime) {
                currentCacheMemoryBytes -= estimateRecordSize(record);
                LOG.debug("Removed stale cache entry: {}", entry.getKey());
                return true;
            }
            return false;
        });
    }
    
    /**
     * Cleanup old acknowledgment metadata to prevent memory leaks.
     */
    public void cleanupOldAcknowledgmentMetadata(Duration maxAge) {
        long cutoffTime = System.currentTimeMillis() - maxAge.toMillis();
        
        acknowledgmentsToCommit.entrySet().removeIf(entry -> {
            boolean shouldRemove = false;
            for (AcknowledgmentMetadata metadata : entry.getValue().values()) {
                if (metadata.getTimestamp() < cutoffTime) {
                    shouldRemove = true;
                    break;
                }
            }
            if (shouldRemove) {
                LOG.warn("Removing stale acknowledgment metadata for checkpoint {} - possible acknowledgment issue", entry.getKey());
            }
            return shouldRemove;
        });
    }
    
    /**
     * Cleanup old acknowledgment metadata with default age threshold.
     */
    public void cleanupOldAcknowledgmentMetadata() {
        cleanupOldAcknowledgmentMetadata(CLEANUP_AGE_THRESHOLD);
    }

    // Utility and getter methods for testing

    /**
     * Gets the share group ID.
     */
    public String getShareGroupId() {
        return shareGroupId;
    }
    
    /**
     * Gets the reader ID.
     */
    public int getReaderId() {
        return readerId;
    }
    
    /**
     * Gets currently subscribed topics.
     */
    public Set<String> getSubscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }
    
    /**
     * Gets the number of cached records (for testing).
     */
    public int getCachedRecordsCount() {
        return recordCache.size();
    }
    
    /**
     * Gets the current cache memory usage (for testing).
     */
    public long getCurrentCacheMemoryBytes() {
        return currentCacheMemoryBytes;
    }
    
    /**
     * Gets the number of pending acknowledgment checkpoints (for testing).
     */
    public int getPendingAcknowledgmentCheckpointsCount() {
        return acknowledgmentsToCommit.size();
    }
    
    /**
     * Gets the configured maximum cache memory limit (for testing/monitoring).
     */
    public long getMaxCacheMemoryBytes() {
        return maxCacheMemoryBytes;
    }
    
    /**
     * Gets the record lock duration in milliseconds.
     */
    public long getRecordLockDurationMs() {
        return recordLockDurationMs;
    }
    
    private String createShareConsumerClientId(Properties props) {
        String baseClientId = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG, "flink-share-consumer");
        return String.format("%s-%s-reader-%d", baseClientId, shareGroupId, readerId);
    }
    
    /**
     * Implementation of RecordsWithSplitIds for share group records.
     */
    private static class ShareGroupRecordsWithSplitIds implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {
        
        private static final ShareGroupRecordsWithSplitIds EMPTY = 
                new ShareGroupRecordsWithSplitIds(Collections.emptyIterator(), "empty");
        
        private final Iterator<ConsumerRecord<byte[], byte[]>> recordIterator;
        private final String splitId;
        private boolean hasReturnedSplit = false;
        
        private ShareGroupRecordsWithSplitIds(Iterator<ConsumerRecord<byte[], byte[]>> recordIterator, String splitId) {
            this.recordIterator = recordIterator;
            this.splitId = splitId;
        }
        
        public static ShareGroupRecordsWithSplitIds empty() {
            return EMPTY;
        }
        
        @Override
        public String nextSplit() {
            if (!hasReturnedSplit && recordIterator.hasNext()) {
                hasReturnedSplit = true;
                return splitId;
            }
            return null;
        }
        
        @Override
        public ConsumerRecord<byte[], byte[]> nextRecordFromSplit() {
            return recordIterator.hasNext() ? recordIterator.next() : null;
        }
        
        @Override
        public void recycle() {
            // No recycling needed for this implementation
        }
        
        @Override
        public Set<String> finishedSplits() {
            // Share groups don't have traditional finished splits
            return Collections.emptySet();
        }
    }
}