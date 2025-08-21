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
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaRecordEmitter;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaShareGroupFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Source reader implementation specifically for Kafka share groups using KafkaShareConsumer API.
 * 
 * <p>This reader extends Flink's base source reader to support Kafka 4.1.0+ share group semantics,
 * providing message-level load balancing across consumers rather than partition-based assignment.
 * 
 * <p><strong>Key differences from KafkaSourceReader:</strong>
 * <ul>
 *   <li>Uses KafkaShareConsumer instead of KafkaConsumer internally</li>
 *   <li>No partition-level offset tracking (messages distributed automatically)</li>
 *   <li>Specialized metrics for share group operations</li>
 *   <li>Queue-like semantics with automatic acknowledgment</li>
 *   <li>Better horizontal scalability beyond partition count</li>
 * </ul>
 * 
 * <p>Compatible with all existing Flink source interfaces and checkpointing mechanisms.
 */
@Internal
public class KafkaShareGroupSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<
        ConsumerRecord<byte[], byte[]>, T, KafkaPartitionSplit, KafkaPartitionSplitState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSourceReader.class);
    
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final KafkaRecordEmitter<T> recordEmitter;
    private final KafkaShareGroupSourceMetrics shareGroupMetrics;
    private final String shareGroupId;
    private final Map<String, KafkaPartitionSplitState> splitStates;
    
    /**
     * Creates a new share group source reader.
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
            new KafkaShareGroupFetcherManager(consumerProps, context),
            new KafkaRecordEmitter<>(deserializationSchema),
            new org.apache.flink.configuration.Configuration(),
            context
        );
        
        this.deserializationSchema = deserializationSchema;
        this.recordEmitter = new KafkaRecordEmitter<>(deserializationSchema);
        this.shareGroupId = consumerProps.getProperty("group.id", "unknown-share-group");
        this.splitStates = new ConcurrentHashMap<>();
        this.shareGroupMetrics = shareGroupMetrics;
        
        LOG.info("Created KafkaShareGroupSourceReader for share group '{}' on subtask {}", 
                shareGroupId, context.getIndexOfSubtask());
    }
    
    @Override
    protected void onSplitFinished(Map<String, KafkaPartitionSplitState> finishedSplitIds) {
        // For share groups, splits don't "finish" in the traditional sense
        // Messages are distributed continuously by the coordinator
        for (String splitId : finishedSplitIds.keySet()) {
            splitStates.remove(splitId);
            LOG.debug("Share group '{}' finished processing split: {}", shareGroupId, splitId);
        }
    }
    
    @Override
    protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
        // For share groups, we don't track offsets like traditional consumers
        // The state is primarily for compatibility with Flink's split system
        KafkaPartitionSplitState state = new KafkaPartitionSplitState(split);
        splitStates.put(split.splitId(), state);
        
        LOG.debug("Share group '{}' initialized state for split: {}", shareGroupId, split.splitId());
        return state;
    }
    
    @Override
    protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
        return splitState.toKafkaPartitionSplit();
    }
    
    @Override
    public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
        // For share groups, we don't need to track offsets for checkpointing
        // The share group coordinator handles message delivery guarantees
        LOG.debug("Share group '{}' snapshot state for checkpoint: {}", shareGroupId, checkpointId);
        
        return splitStates.values().stream()
                .map(KafkaPartitionSplitState::toKafkaPartitionSplit)
                .collect(java.util.stream.Collectors.toList());
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        
        // Share groups handle acknowledgment automatically, but we can log for debugging
        LOG.debug("Share group '{}' checkpoint {} completed", shareGroupId, checkpointId);
    }
    
    @Override
    public void close() throws Exception {
        try {
            super.close();
            
            if (shareGroupMetrics != null) {
                shareGroupMetrics.reset();
            }
            
            LOG.info("KafkaShareGroupSourceReader '{}' closed", shareGroupId);
        } catch (Exception e) {
            LOG.warn("Error closing KafkaShareGroupSourceReader '{}': {}", shareGroupId, e.getMessage());
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
    public Map<String, KafkaPartitionSplitState> getSplitStates() {
        return new HashMap<>(splitStates);
    }
}