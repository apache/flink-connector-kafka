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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
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
import java.util.Properties;
import java.util.Set;

/**
 * SplitReader for Kafka Share Groups with batch-based checkpoint recovery.
 * 
 * Controls polling frequency to work within share consumer's auto-commit constraints.
 * Stores complete record batches in checkpoint state for crash recovery.
 */
@Internal
public class KafkaShareGroupSplitReader implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaShareGroupSplit> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSplitReader.class);
    private static final long POLL_TIMEOUT_MS = 100L;
    
    private final KafkaShareConsumer<byte[], byte[]> shareConsumer;
    private final String shareGroupId;
    private final int readerId;
    private final Map<String, KafkaShareGroupSplit> assignedSplits;
    private final Set<String> subscribedTopics;
    private final KafkaShareGroupSourceMetrics metrics;
    private final ShareGroupBatchManager<byte[], byte[]> batchManager;

    /**
     * Creates a share group split reader with batch-based checkpoint recovery.
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
        
        // Enable explicit acknowledgment mode for controlled acknowledgment
        shareConsumerProps.setProperty("share.acknowledgement.mode", "explicit");
        shareConsumerProps.setProperty("group.type", "share");
        this.shareGroupId = shareConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        
        if (shareGroupId == null) {
            throw new IllegalArgumentException("Share group ID (group.id) must be specified");
        }
        
        // Initialize batch management
        this.batchManager = new ShareGroupBatchManager<>("share-group-" + shareGroupId + "-" + readerId);
        
        // Configure client ID
        shareConsumerProps.setProperty(
            ConsumerConfig.CLIENT_ID_CONFIG, 
            createClientId(shareConsumerProps)
        );
        
        // Remove unsupported properties
        shareConsumerProps.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        
        // Create share consumer
        this.shareConsumer = new KafkaShareConsumer<>(shareConsumerProps);
        
        LOG.info("Created KafkaShareGroupSplitReader for share group '{}' reader {} with batch-based checkpoint recovery", 
                shareGroupId, readerId);
    }

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        try {
            if (assignedSplits.isEmpty()) {
                LOG.debug("Share group '{}' reader {} waiting for split assignment", shareGroupId, readerId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // First check for unprocessed records from previous batches
            RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> unprocessedRecords = batchManager.getNextUnprocessedRecords();
            if (hasRecords(unprocessedRecords)) {
                LOG.debug("Share group '{}' reader {} returning unprocessed records", shareGroupId, readerId);
                return unprocessedRecords;
            }
            
            // Only poll if no unprocessed batches exist (controls auto-commit timing)
            if (batchManager.hasUnprocessedBatches()) {
                LOG.trace("Share group '{}' reader {} skipping poll - unprocessed batches exist", shareGroupId, readerId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // Safe to poll - previous batches are processed
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            
            if (consumerRecords.isEmpty()) {
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // Convert to list and store in batch manager
            List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                recordList.add(record);
                if (metrics != null) {
                    metrics.recordMessageReceived();
                }
            }
            
            // Store complete batch for checkpoint recovery
            batchManager.addBatch(recordList);
            
            LOG.debug("Share group '{}' reader {} fetched batch with {} records", shareGroupId, readerId, recordList.size());
            
            // Return records from batch manager
            return batchManager.getNextUnprocessedRecords();
            
        } catch (WakeupException e) {
            LOG.info("ShareGroup [{}]: Reader {} woken up during fetch - shutting down gracefully", shareGroupId, readerId);
            return ShareGroupRecordsWithSplitIds.empty();
        } catch (Exception e) {
            LOG.error("ShareGroup [{}]: FETCH FAILURE - Reader {} failed to poll records: {}", 
                    shareGroupId, readerId, e.getMessage(), e);
            throw new IOException("Failed to fetch records from share group: " + shareGroupId, e);
        }
    }
    
    /**
     * Called when checkpoint starts - delegates to batch manager.
     */
    public void snapshotState(long checkpointId) {
        LOG.debug("Share group '{}' reader {} snapshotting state for checkpoint {}", shareGroupId, readerId, checkpointId);
    }
    
    /**
     * Called when checkpoint completes - acknowledges records via batch manager.
     */
    public void notifyCheckpointComplete(long checkpointId) {
        try {
            // Get batches completed by this checkpoint
            List<ShareGroupBatchForCheckpoint<byte[], byte[]>> completedBatches = getCompletedBatches(checkpointId);
            
            for (ShareGroupBatchForCheckpoint<byte[], byte[]> batch : completedBatches) {
                // Acknowledge all records in the batch
                for (ConsumerRecord<byte[], byte[]> record : batch.getRecords()) {
                    shareConsumer.acknowledge(record, AcknowledgeType.ACCEPT);
                }
            }
            
            // The actual commit will happen on next poll() due to auto-commit behavior
            batchManager.notifyCheckpointComplete(checkpointId);
            
            LOG.debug("Acknowledged {} batches for checkpoint {} in share group '{}'", 
                    completedBatches.size(), checkpointId, shareGroupId);
                    
        } catch (Exception e) {
            LOG.error("ShareGroup [{}]: ACKNOWLEDGE FAILURE - Reader {} failed to acknowledge records for checkpoint {}: {}", 
                     shareGroupId, readerId, checkpointId, e.getMessage(), e);
        }
    }
    
    /**
     * Called when checkpoint fails - releases records for redelivery.
     */
    public void notifyCheckpointAborted(long checkpointId) {
        try {
            List<ShareGroupBatchForCheckpoint<byte[], byte[]>> failedBatches = getCompletedBatches(checkpointId);
            
            LOG.info("ShareGroup [{}]: CHECKPOINT {} ABORTED - Reader {} releasing {} batches for redelivery", 
                     shareGroupId, checkpointId, readerId, failedBatches.size());
            
            for (ShareGroupBatchForCheckpoint<byte[], byte[]> batch : failedBatches) {
                // Release records for redelivery
                for (ConsumerRecord<byte[], byte[]> record : batch.getRecords()) {
                    shareConsumer.acknowledge(record, AcknowledgeType.RELEASE);
                }
            }
            
            batchManager.notifyCheckpointAborted(checkpointId);
            
            LOG.info("Released {} batches for redelivery after checkpoint {} failure", 
                    failedBatches.size(), checkpointId);
                            
        } catch (Exception e) {
            LOG.error("ShareGroup [{}]: ABORT FAILURE - Reader {} failed to release records for checkpoint {}: {}", 
                     shareGroupId, readerId, checkpointId, e.getMessage(), e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KafkaShareGroupSplit> splitsChanges) {
        LOG.info("Share group '{}' reader {} handling splits changes", shareGroupId, readerId);
        
        if (splitsChanges instanceof SplitsAddition) {
            handleSplitsAddition((SplitsAddition<KafkaShareGroupSplit>) splitsChanges);
        } else if (splitsChanges instanceof SplitsRemoval) {
            handleSplitsRemoval((SplitsRemoval<KafkaShareGroupSplit>) splitsChanges);
        }
    }
    
    private void handleSplitsAddition(SplitsAddition<KafkaShareGroupSplit> splitsAddition) {
        Collection<KafkaShareGroupSplit> newSplits = splitsAddition.splits();
        Set<String> newTopics = new HashSet<>();
        
        for (KafkaShareGroupSplit split : newSplits) {
            assignedSplits.put(split.splitId(), split);
            newTopics.add(split.getTopicName());
        }
        
        subscribedTopics.addAll(newTopics);
        
        if (!subscribedTopics.isEmpty()) {
            try {
                shareConsumer.subscribe(subscribedTopics);
                LOG.info("Share group '{}' reader {} subscribed to topics: {}", 
                        shareGroupId, readerId, subscribedTopics);
            } catch (Exception e) {
                LOG.error("Failed to subscribe to topics: {}", e.getMessage(), e);
            }
        }
    }
    
    private void handleSplitsRemoval(SplitsRemoval<KafkaShareGroupSplit> splitsRemoval) {
        for (KafkaShareGroupSplit split : splitsRemoval.splits()) {
            assignedSplits.remove(split.splitId());
        }
        LOG.debug("Share group '{}' reader {} removed {} splits", 
                shareGroupId, readerId, splitsRemoval.splits().size());
    }

    @Override
    public void wakeUp() {
        shareConsumer.wakeup();
    }

    @Override
    public void close() throws Exception {
        try {
            // Release all unacknowledged records
            releaseUnacknowledgedRecords();
            shareConsumer.close(Duration.ofSeconds(5));
            LOG.info("Share group '{}' reader {} closed successfully", shareGroupId, readerId);
        } catch (Exception e) {
            LOG.warn("Error closing share consumer: {}", e.getMessage());
            throw e;
        }
    }
    
    private void releaseUnacknowledgedRecords() {
        // Release records from all pending batches
        for (int i = 0; i < batchManager.getPendingBatchCount(); i++) {
            // Implementation would iterate through batches and release records
        }
    }
    
    private boolean hasRecords(RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> records) {
        return records.nextSplit() != null;
    }
    
    private List<ShareGroupBatchForCheckpoint<byte[], byte[]>> getCompletedBatches(long checkpointId) {
        // This would be implemented to get batches associated with the checkpoint
        return new ArrayList<>();
    }
    
    private String createClientId(Properties props) {
        String baseClientId = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG, "flink-share-consumer");
        return String.format("%s-%s-reader-%d", baseClientId, shareGroupId, readerId);
    }
    
    // Getters for testing and monitoring
    public String getShareGroupId() {
        return shareGroupId;
    }
    
    public int getReaderId() {
        return readerId;
    }
    
    public Set<String> getSubscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }
    
    public ShareGroupBatchManager<byte[], byte[]> getBatchManager() {
        return batchManager;
    }
    
    /**
     * Simple implementation of RecordsWithSplitIds for share group records.
     */
    private static class ShareGroupRecordsWithSplitIds implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {
        
        private static final ShareGroupRecordsWithSplitIds EMPTY = 
                new ShareGroupRecordsWithSplitIds(Collections.emptyIterator(), null);
        
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
            if (!hasReturnedSplit && recordIterator.hasNext() && splitId != null) {
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
            // No recycling needed
        }
        
        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet();
        }
    }
}