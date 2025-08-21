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
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaShareGroupFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplit;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplitState;
import org.apache.flink.configuration.Configuration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Source reader for Kafka share groups using proper Flink connector architecture.
 * 
 * <p>This reader extends SingleThreadMultiplexSourceReaderBase to leverage Flink's
 * proven connector patterns while implementing share group semantics. It uses:
 * 
 * <ul>
 *   <li>Topic-based splits instead of partition-based splits</li>
 *   <li>Share group consumer subscription instead of partition assignment</li>
 *   <li>Proper integration with Flink's split management</li>
 *   <li>Built-in support for checkpointing, backpressure, and metrics</li>
 * </ul>
 * 
 * <p>The reader manages share group splits that represent topics rather than partitions.
 * Multiple readers can be assigned the same topic, and Kafka's share group coordinator
 * distributes messages at the record level across all consumers in the share group.
 */
@Internal
public class KafkaShareGroupSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<
        ConsumerRecord<byte[], byte[]>, T, KafkaShareGroupSplit, KafkaShareGroupSplitState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSourceReader.class);
    
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final KafkaShareGroupSourceMetrics shareGroupMetrics;
    private final String shareGroupId;
    private final Map<String, KafkaShareGroupSplitState> splitStates;
    
    // Pulsar-style metadata-only checkpointing
    private final SortedMap<Long, Map<String, AcknowledgmentMetadata>> acknowledgmentsToCommit;
    private final ConcurrentMap<String, AcknowledgmentMetadata> acknowledgementsOfFinishedSplits;
    private final AtomicReference<Throwable> acknowledgmentCommitThrowable;
    
    /**
     * Creates a share group source reader using Flink's connector architecture.
     *
     * @param consumerProps consumer properties configured for share groups
     * @param deserializationSchema schema for deserializing Kafka records
     * @param context source reader context
     * @param shareGroupMetrics metrics collector for share group operations
     */
    public KafkaShareGroupSourceReader(
            Properties consumerProps,
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            SourceReaderContext context,
            KafkaShareGroupSourceMetrics shareGroupMetrics) {
        
        super(
            new KafkaShareGroupFetcherManager(consumerProps, context, shareGroupMetrics),
            new KafkaShareGroupRecordEmitter<>(deserializationSchema),
            new Configuration(),
            context
        );
        
        this.deserializationSchema = deserializationSchema;
        this.shareGroupId = consumerProps.getProperty("group.id", "unknown-share-group");
        this.splitStates = new ConcurrentHashMap<>();
        this.shareGroupMetrics = shareGroupMetrics;
        
        // Initialize Pulsar-style metadata tracking
        this.acknowledgmentsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.acknowledgementsOfFinishedSplits = new ConcurrentHashMap<>();
        this.acknowledgmentCommitThrowable = new AtomicReference<>();
        
        LOG.info("*** SOURCE READER: Created KafkaShareGroupSourceReader for share group '{}' on subtask {} using Flink connector architecture", 
                shareGroupId, context.getIndexOfSubtask());
    }
    
    @Override
    protected void onSplitFinished(Map<String, KafkaShareGroupSplitState> finishedSplitIds) {
        // Following Pulsar pattern: store metadata of finished splits for acknowledgment
        if (LOG.isDebugEnabled()) {
            LOG.debug("onSplitFinished event: {}", finishedSplitIds);
        }
        
        for (Map.Entry<String, KafkaShareGroupSplitState> entry : finishedSplitIds.entrySet()) {
            String splitId = entry.getKey();
            KafkaShareGroupSplitState state = entry.getValue();
            AcknowledgmentMetadata metadata = state.getLatestAcknowledgmentMetadata();
            if (metadata != null) {
                acknowledgementsOfFinishedSplits.put(splitId, metadata);
            }
            
            // Remove from active splits
            splitStates.remove(splitId);
            LOG.debug("Share group '{}' finished processing split: {}", shareGroupId, splitId);
        }
    }
    
    @Override
    protected KafkaShareGroupSplitState initializedState(KafkaShareGroupSplit split) {
        // For share groups, state is minimal since offset tracking is handled by coordinator
        KafkaShareGroupSplitState state = new KafkaShareGroupSplitState(split);
        splitStates.put(split.splitId(), state);
        
        LOG.info("*** SOURCE READER: Share group '{}' initialized state for split: {} (topic: {})", 
                shareGroupId, split.splitId(), split.getTopicName());
        return state;
    }
    
    @Override
    protected KafkaShareGroupSplit toSplitType(String splitId, KafkaShareGroupSplitState splitState) {
        return splitState.toKafkaShareGroupSplit();
    }
    
    @Override
    public List<KafkaShareGroupSplit> snapshotState(long checkpointId) {
        // Get splits from parent - this handles the basic split state
        List<KafkaShareGroupSplit> splits = super.snapshotState(checkpointId);
        
        // Following Pulsar pattern: store acknowledgment metadata for checkpoint
        Map<String, AcknowledgmentMetadata> acknowledgments = 
            acknowledgmentsToCommit.computeIfAbsent(checkpointId, id -> new ConcurrentHashMap<>());
        
        // Store acknowledgment metadata of active splits
        for (KafkaShareGroupSplit split : splits) {
            String splitId = split.splitId();
            KafkaShareGroupSplitState splitState = splitStates.get(splitId);
            if (splitState != null) {
                AcknowledgmentMetadata metadata = splitState.getLatestAcknowledgmentMetadata();
                if (metadata != null) {
                    acknowledgments.put(splitId, metadata);
                }
            }
        }
        
        // Store acknowledgment metadata of finished splits
        acknowledgments.putAll(acknowledgementsOfFinishedSplits);
        
        // Notify split readers about checkpoint start (for association)
        notifySplitReadersCheckpointStart(checkpointId);
        
        LOG.info("ShareGroup [{}]: CHECKPOINT {} - Snapshot state for {} splits with {} acknowledgments", 
                shareGroupId, checkpointId, splits.size(), acknowledgments.size());
        
        return splits;
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Following Pulsar pattern: acknowledge based on stored metadata
        LOG.info("ShareGroup [{}]: CHECKPOINT {} COMPLETE - Committing acknowledgments for {} splits", 
                 shareGroupId, checkpointId, acknowledgments != null ? acknowledgments.size() : 0);
        
        Map<String, AcknowledgmentMetadata> acknowledgments = acknowledgmentsToCommit.get(checkpointId);
        if (acknowledgments == null) {
            LOG.debug("Acknowledgments for checkpoint {} have already been committed.", checkpointId);
            return;
        }
        
        try {
            // Acknowledge messages using metadata instead of full records
            KafkaShareGroupFetcherManager fetcherManager = (KafkaShareGroupFetcherManager) splitFetcherManager;
            fetcherManager.acknowledgeMessages(acknowledgments);
            
            LOG.debug("Successfully acknowledged {} splits for checkpoint {}", acknowledgments.size(), checkpointId);
            
            // Clean up acknowledgments - following Pulsar cleanup pattern
            acknowledgementsOfFinishedSplits.keySet().removeAll(acknowledgments.keySet());
            acknowledgmentsToCommit.headMap(checkpointId + 1).clear();
            
        } catch (Exception e) {
            LOG.error("Failed to acknowledge messages for checkpoint {}", checkpointId, e);
            acknowledgmentCommitThrowable.compareAndSet(null, e);
            throw e;
        }
        
        // Call parent implementation
        super.notifyCheckpointComplete(checkpointId);
        
        LOG.info("ShareGroup [{}]: CHECKPOINT {} SUCCESS - Acknowledgments committed to Kafka coordinator", 
                 shareGroupId, checkpointId);
    }
    
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // Notify split readers to release records for this checkpoint
        notifySplitReadersCheckpointAborted(checkpointId, null);
        
        // Call parent implementation
        super.notifyCheckpointAborted(checkpointId);
        
        LOG.info("ShareGroup [{}]: CHECKPOINT {} ABORTED - {} records released for redelivery", 
                 shareGroupId, checkpointId, acknowledgments != null ? acknowledgments.size() : 0);
    }
    
    /**
     * Notifies all split readers that a checkpoint has started.
     */
    private void notifySplitReadersCheckpointStart(long checkpointId) {
        KafkaShareGroupFetcherManager fetcherManager = (KafkaShareGroupFetcherManager) splitFetcherManager;
        fetcherManager.notifyCheckpointStart(checkpointId);
    }
    
    /**
     * Notifies all split readers that a checkpoint has completed successfully.
     */
    private void notifySplitReadersCheckpointComplete(long checkpointId) throws Exception {
        KafkaShareGroupFetcherManager fetcherManager = (KafkaShareGroupFetcherManager) splitFetcherManager;
        fetcherManager.notifyCheckpointComplete(checkpointId);
    }
    
    /**
     * Notifies all split readers that a checkpoint has been aborted.
     */
    private void notifySplitReadersCheckpointAborted(long checkpointId, Throwable cause) {
        KafkaShareGroupFetcherManager fetcherManager = (KafkaShareGroupFetcherManager) splitFetcherManager;
        fetcherManager.notifyCheckpointAborted(checkpointId, cause);
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            
            if (shareGroupMetrics != null) {
                shareGroupMetrics.reset();
            }
            
            LOG.info("KafkaShareGroupSourceReader for share group '{}' closed", shareGroupId);
        } catch (Exception e) {
            LOG.warn("Error closing KafkaShareGroupSourceReader for share group '{}': {}", 
                    shareGroupId, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Gets the share group ID for this reader.
     */
    public String getShareGroupId() {
        return shareGroupId;
    }
    
    /**
     * Gets the share group metrics collector.
     */
    public KafkaShareGroupSourceMetrics getShareGroupMetrics() {
        return shareGroupMetrics;
    }
    
    /**
     * Gets current split states (for debugging/monitoring).
     */
    public Map<String, KafkaShareGroupSplitState> getSplitStates() {
        return new java.util.HashMap<>(splitStates);
    }
    
    /**
     * Acknowledgment metadata class following Pulsar pattern.
     * Stores lightweight metadata instead of full records.
     */
    public static class AcknowledgmentMetadata {
        private final Set<TopicPartition> topicPartitions;
        private final Map<TopicPartition, Set<Long>> offsetsToAcknowledge;
        private final long timestamp;
        private final int recordCount;
        
        public AcknowledgmentMetadata(Set<TopicPartition> topicPartitions, 
                                    Map<TopicPartition, Set<Long>> offsetsToAcknowledge,
                                    int recordCount) {
            this.topicPartitions = Collections.unmodifiableSet(new HashSet<>(topicPartitions));
            this.offsetsToAcknowledge = Collections.unmodifiableMap(new HashMap<>(offsetsToAcknowledge));
            this.timestamp = System.currentTimeMillis();
            this.recordCount = recordCount;
        }
        
        public Set<TopicPartition> getTopicPartitions() {
            return topicPartitions;
        }
        
        public Map<TopicPartition, Set<Long>> getOffsetsToAcknowledge() {
            return offsetsToAcknowledge;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public int getRecordCount() {
            return recordCount;
        }
        
        @Override
        public String toString() {
            return String.format("AcknowledgmentMetadata{partitions=%d, records=%d, timestamp=%d}", 
                    topicPartitions.size(), recordCount, timestamp);
        }
    }
}