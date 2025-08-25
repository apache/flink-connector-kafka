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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

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
        
        LOG.info("*** SOURCE READER: Created KafkaShareGroupSourceReader for share group '{}' on subtask {} using Flink connector architecture", 
                shareGroupId, context.getIndexOfSubtask());
    }
    
    @Override
    protected void onSplitFinished(Map<String, KafkaShareGroupSplitState> finishedSplitIds) {
        // For share groups, splits don't "finish" in the traditional sense
        // Topics remain subscribed as long as the source is active
        for (String splitId : finishedSplitIds.keySet()) {
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
        // Notify split reader to associate upcoming records with this checkpoint
        notifySplitReadersCheckpointStart(checkpointId);
        
        // For share groups, minimal state is needed since coordinator handles message delivery
        LOG.debug("Share group '{}' snapshot state for checkpoint: {} ({} splits)", 
                shareGroupId, checkpointId, splitStates.size());
        
        return splitStates.values().stream()
                .map(KafkaShareGroupSplitState::toKafkaShareGroupSplit)
                .collect(java.util.stream.Collectors.toList());
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // First notify split readers to acknowledge records for this checkpoint
        notifySplitReadersCheckpointComplete(checkpointId);
        
        // Then call parent implementation
        super.notifyCheckpointComplete(checkpointId);
        
        LOG.debug("Share group '{}' checkpoint {} completed with acknowledgments sent", shareGroupId, checkpointId);
    }
    
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // Notify split readers to release records for this checkpoint
        notifySplitReadersCheckpointAborted(checkpointId, null);
        
        // Call parent implementation
        super.notifyCheckpointAborted(checkpointId);
        
        LOG.info("Share group '{}' checkpoint {} aborted, records released for redelivery", shareGroupId, checkpointId);
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
}