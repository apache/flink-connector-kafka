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
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced SplitReader implementation for Kafka Share Groups with at-least-once semantics.
 * 
 * <p>This reader provides checkpoint-based acknowledgment to ensure at-least-once delivery
 * guarantees. Records are only acknowledged to Kafka after successful Flink checkpoint completion.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Explicit acknowledgment mode for precise control</li>
 *   <li>Checkpoint-coordinated acknowledgments</li>
 *   <li>Automatic record release on checkpoint failures</li>
 *   <li>Graceful handling of record state exceptions</li>
 *   <li>Memory leak prevention for stale records</li>
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
    
    // At-least-once semantics state
    private final Map<Long, PendingCheckpointRecords> pendingCheckpoints;
    private volatile long currentCheckpointId = -1;
    private volatile long recordLockDurationMs;
    
    /**
     * Container for records pending acknowledgment for a specific checkpoint.
     */
    private static class PendingCheckpointRecords {
        private final List<ConsumerRecord<byte[], byte[]>> records;
        private final long timestamp;
        private volatile boolean acknowledged = false;
        private volatile boolean commitInProgress = false;
        
        PendingCheckpointRecords(List<ConsumerRecord<byte[], byte[]>> records) {
            this.records = new ArrayList<>(records);
            this.timestamp = System.currentTimeMillis();
        }
        
        public List<ConsumerRecord<byte[], byte[]>> getRecords() {
            return new ArrayList<>(records);
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public boolean isAcknowledged() {
            return acknowledged;
        }
        
        public void setAcknowledged(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }
        
        public boolean isCommitInProgress() {
            return commitInProgress;
        }
        
        public void setCommitInProgress(boolean commitInProgress) {
            this.commitInProgress = commitInProgress;
        }
        
        public boolean matchesOffsets(Map<TopicIdPartition, Set<Long>> committedOffsets) {
            // Simple implementation - in real scenario would match record offsets
            return !records.isEmpty() && !committedOffsets.isEmpty();
        }
        
        public void markCompleted() {
            this.acknowledged = true;
            this.commitInProgress = false;
        }
        
        public void markFailed(Exception exception) {
            this.acknowledged = false;
            this.commitInProgress = false;
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
        this.pendingCheckpoints = new ConcurrentHashMap<>();
        
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
        
        // Set up global acknowledgment callback
        setupAcknowledgmentCallback();
        
        LOG.info("Created KafkaShareGroupSplitReader for share group '{}' reader {} with at-least-once semantics (lockDuration={}ms)", 
                shareGroupId, readerId, recordLockDurationMs);
    }
    
    /**
     * Factory method for creating share consumer - allows mocking in tests.
     */
    protected KafkaShareConsumer<byte[], byte[]> createShareConsumer(Properties props) {
        return new KafkaShareConsumer<>(props);
    }
    
    /**
     * Sets up the global acknowledgment callback to handle commit results.
     */
    private void setupAcknowledgmentCallback() {
        shareConsumer.setAcknowledgementCommitCallback(new AcknowledgementCommitCallback() {
            @Override
            public void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception) {
                handleAcknowledgmentComplete(offsets, exception);
            }
        });
    }
    
    /**
     * Handles acknowledgment commit completion - success or failure.
     */
    private void handleAcknowledgmentComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception) {
        // Find the matching checkpoint for these offsets
        for (Map.Entry<Long, PendingCheckpointRecords> entry : pendingCheckpoints.entrySet()) {
            Long checkpointId = entry.getKey();
            PendingCheckpointRecords pending = entry.getValue();
            
            if (pending.isCommitInProgress() && pending.matchesOffsets(offsets)) {
                if (exception != null) {
                    handleCommitFailure(checkpointId, pending, exception);
                } else {
                    handleCommitSuccess(checkpointId, pending);
                }
                break; // Found the matching checkpoint
            }
        }
    }
    
    /**
     * Handles successful acknowledgment commit.
     */
    private void handleCommitSuccess(Long checkpointId, PendingCheckpointRecords pending) {
        LOG.debug("Acknowledgment succeeded for checkpoint {} ({} records)", 
                checkpointId, pending.getRecords().size());
        
        pending.markCompleted();
        pendingCheckpoints.remove(checkpointId);
        
        if (metrics != null) {
            metrics.recordSuccessfulCommit();
        }
    }
    
    /**
     * Handles failed acknowledgment commit with appropriate error handling.
     */
    private void handleCommitFailure(Long checkpointId, PendingCheckpointRecords pending, Exception exception) {
        if (exception instanceof InvalidRecordStateException) {
            // Records were likely already processed or timed out - this is acceptable
            LOG.warn("Records for checkpoint {} were already processed or timed out: {}", 
                   checkpointId, exception.getMessage());
            pendingCheckpoints.remove(checkpointId);
        } else {
            LOG.error("Acknowledgment failed for checkpoint {}: {}", checkpointId, exception.getMessage());
            pending.markFailed(exception);
        }
        
        if (metrics != null) {
            metrics.recordFailedCommit();
        }
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
            
            // Associate records with current checkpoint
            if (currentCheckpointId >= 0 && !recordList.isEmpty()) {
                pendingCheckpoints.put(currentCheckpointId, new PendingCheckpointRecords(recordList));
                LOG.debug("Stored {} records for checkpoint {} in share group '{}'", 
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
     * Called when Flink checkpoint completes successfully - acknowledge records.
     */
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        PendingCheckpointRecords pending = pendingCheckpoints.get(checkpointId);
        
        if (pending != null && !pending.isAcknowledged() && !pending.isCommitInProgress()) {
            pending.setCommitInProgress(true);
            
            try {
                List<ConsumerRecord<byte[], byte[]>> records = pending.getRecords();
                
                // Acknowledge all records as successfully processed
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                }
                
                pending.setAcknowledged(true);
                
                // Commit acknowledgments asynchronously (callback handles the result)
                shareConsumer.commitAsync();
                
                LOG.debug("Initiated acknowledgment for {} records in checkpoint {} for share group '{}'", 
                        records.size(), checkpointId, shareGroupId);
                
            } catch (Exception e) {
                pending.setCommitInProgress(false);
                pending.setAcknowledged(false);
                LOG.error("Failed to acknowledge records for checkpoint {}: {}", checkpointId, e.getMessage());
                throw e;
            }
        }
    }
    
    /**
     * Called when Flink checkpoint fails - release records for redelivery.
     */
    public void notifyCheckpointAborted(long checkpointId, Throwable cause) {
        PendingCheckpointRecords pending = pendingCheckpoints.remove(checkpointId);
        
        if (pending != null && !pending.isAcknowledged()) {
            try {
                List<ConsumerRecord<byte[], byte[]>> records = pending.getRecords();
                
                // Release records for redelivery since processing failed
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                }
                
                shareConsumer.commitAsync();
                
                LOG.info("Released {} records for redelivery after checkpoint {} failure in share group '{}'", 
                        records.size(), checkpointId, shareGroupId);
                        
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
        for (PendingCheckpointRecords pending : pendingCheckpoints.values()) {
            if (!pending.isAcknowledged()) {
                try {
                    for (ConsumerRecord<byte[], byte[]> record : pending.getRecords()) {
                        shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                    }
                } catch (Exception e) {
                    LOG.debug("Failed to release record on close: {}", e.getMessage());
                    // Records will timeout anyway
                }
            }
        }
        pendingCheckpoints.clear();
    }
    
    /**
     * Cleanup old pending records to prevent memory leaks.
     */
    public void cleanupOldPendingRecords(Duration maxAge) {
        long cutoffTime = System.currentTimeMillis() - maxAge.toMillis();
        
        pendingCheckpoints.entrySet().removeIf(entry -> {
            if (entry.getValue().getTimestamp() < cutoffTime) {
                LOG.warn("Removing stale checkpoint {} - possible acknowledgment issue", entry.getKey());
                return true;
            }
            return false;
        });
    }
    
    /**
     * Cleanup old pending records with default age threshold.
     */
    public void cleanupOldPendingRecords() {
        cleanupOldPendingRecords(CLEANUP_AGE_THRESHOLD);
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
     * Gets the number of pending checkpoints (for testing).
     */
    public int getPendingCheckpointsCount() {
        return pendingCheckpoints.size();
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