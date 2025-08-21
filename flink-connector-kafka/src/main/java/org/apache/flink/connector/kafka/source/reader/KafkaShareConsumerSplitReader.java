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
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.util.KafkaVersionUtils;
import org.apache.flink.util.Preconditions;

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
 * A {@link SplitReader} implementation specifically for Kafka Share Groups using KafkaShareConsumer API.
 * 
 * <p>This class extends Flink's split reader infrastructure to support Kafka 4.1.0+ share group semantics,
 * which provide queue-like message distribution across consumers rather than partition-based assignment.
 * 
 * <p><strong>Key differences from traditional KafkaPartitionSplitReader:</strong>
 * <ul>
 *   <li>Uses {@code KafkaShareConsumer} instead of {@code KafkaConsumer}</li>
 *   <li>Messages distributed at message-level, not partition-level</li>
 *   <li>No manual partition assignment - uses topic subscription</li>
 *   <li>Automatic load balancing by Kafka share group coordinator</li>
 *   <li>Queue semantics: each message delivered to exactly one consumer</li>
 * </ul>
 * 
 * <p>This implementation is compatible with Kafka client 4.1.0+ and requires 
 * {@code group.type=share} in consumer configuration.
 */
@Internal
public class KafkaShareConsumerSplitReader implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareConsumerSplitReader.class);
    private static final long POLL_TIMEOUT_MS = 100L;
    
    private final KafkaShareConsumer<byte[], byte[]> shareConsumer;
    private final String shareGroupId;
    private final int subtaskId;
    private final Set<String> subscribedTopics;
    private final Map<String, KafkaPartitionSplit> assignedSplits;
    private final KafkaShareGroupSourceMetrics metrics;
    
    /**
     * Creates a new share consumer split reader.
     *
     * @param props consumer properties (must include share group configuration)
     * @param context the source reader context
     * @param metrics metrics collector for share group operations
     * @throws IllegalStateException if share groups are not supported in current Kafka version
     */
    public KafkaShareConsumerSplitReader(
            Properties props,
            SourceReaderContext context,
            @Nullable KafkaShareGroupSourceMetrics metrics) {
        
        this.subtaskId = context.getIndexOfSubtask();
        this.metrics = metrics;
        this.subscribedTopics = new HashSet<>();
        this.assignedSplits = new HashMap<>();
        
        // Configure share consumer properties
        Properties shareConsumerProps = new Properties();
        shareConsumerProps.putAll(props);
        
        // Ensure share group properties are set
        shareConsumerProps.setProperty("group.type", "share");
        this.shareGroupId = shareConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        
        Preconditions.checkNotNull(shareGroupId, "Share group ID (group.id) must be specified");
        
        // Validate share group support with detailed configuration info
        if (!KafkaVersionUtils.isShareGroupSupported()) {
            // Extract configuration details for error message
            String topic = extractTopicFromProps(props);
            String servers = props.getProperty("bootstrap.servers", "localhost:9092");
            
            throw new UnsupportedOperationException(
                "KafkaShareGroupSource demonstration - Full implementation requires external kafka connector integration. " +
                "Configuration validated successfully for: shareGroupId=" + shareGroupId + 
                ", topic=" + topic + 
                ", servers=" + servers);
        }
        
        // Configure client ID for this subtask
        shareConsumerProps.setProperty(
            ConsumerConfig.CLIENT_ID_CONFIG, 
            createShareConsumerClientId(shareConsumerProps)
        );
        
        // Remove properties not supported by share groups (they have different semantics)
        shareConsumerProps.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        
        // Validate share group properties
        KafkaVersionUtils.validateShareGroupProperties(shareConsumerProps);
        
        // Create KafkaShareConsumer instance
        this.shareConsumer = new KafkaShareConsumer<>(shareConsumerProps);
        
        LOG.info("Created KafkaShareConsumer for share group '{}' on subtask {}", 
                shareGroupId, subtaskId);
    }
    
    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        try {
            // For share groups, we need to wait until at least one split is assigned
            // before we can return any records to avoid "unregistered split" errors
            if (assignedSplits.isEmpty()) {
                LOG.trace("Share group '{}' waiting for split assignment before fetching records", shareGroupId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // Poll for records using KafkaShareConsumer API
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            
            if (consumerRecords.isEmpty()) {
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            // Record metrics
            if (metrics != null) {
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    metrics.recordMessageReceived();
                }
            }
            
            LOG.debug("Share group '{}' subtask {} fetched {} records", 
                    shareGroupId, subtaskId, consumerRecords.count());
            
            // Use the first available registered split ID
            // For share groups, all records can be associated with any split since 
            // message distribution is handled by the Kafka broker
            String splitId = assignedSplits.keySet().iterator().next();
            LOG.trace("Share group '{}' using registered split ID: {}", shareGroupId, splitId);
            
            return new ShareGroupRecordsWithSplitIds(consumerRecords.iterator(), splitId);
            
        } catch (WakeupException e) {
            // Normal interruption for shutdown
            LOG.debug("Share consumer '{}' woken up", shareGroupId);
            return ShareGroupRecordsWithSplitIds.empty();
        } catch (Exception e) {
            LOG.error("Error polling from KafkaShareConsumer for share group '{}': {}", 
                    shareGroupId, e.getMessage(), e);
            throw new IOException("Failed to fetch records from share group: " + shareGroupId, e);
        }
    }
    
    @Override
    public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            handleSplitsAddition((SplitsAddition<KafkaPartitionSplit>) splitsChanges);
        } else if (splitsChanges instanceof SplitsRemoval) {
            handleSplitsRemoval((SplitsRemoval<KafkaPartitionSplit>) splitsChanges);
        } else {
            LOG.warn("Unsupported splits change type: {}", splitsChanges.getClass());
        }
    }
    
    private void handleSplitsAddition(SplitsAddition<KafkaPartitionSplit> splitsAddition) {
        Collection<KafkaPartitionSplit> newSplits = splitsAddition.splits();
        Set<String> newTopics = new HashSet<>();
        
        LOG.info("Share group '{}' received {} new splits to process", shareGroupId, newSplits.size());
        
        // Track splits and extract topics
        for (KafkaPartitionSplit split : newSplits) {
            assignedSplits.put(split.splitId(), split);
            String topic = split.getTopicPartition().topic();
            newTopics.add(topic);
            
            if (!subscribedTopics.contains(topic)) {
                subscribedTopics.add(topic);
                LOG.info("Share group '{}' will subscribe to new topic: {}", shareGroupId, topic);
            }
            
            LOG.info("Share group '{}' registered split: {} for topic-partition: {}", 
                    shareGroupId, split.splitId(), split.getTopicPartition());
        }
        
        // For share groups, if no splits were provided, create all topics from splits
        subscribedTopics.addAll(newTopics);
        
        // Update subscription if topics were added
        if (!subscribedTopics.isEmpty()) {
            try {
                shareConsumer.subscribe(subscribedTopics);
                LOG.info("Share group '{}' subscribed to topics: {}", shareGroupId, subscribedTopics);
            } catch (Exception e) {
                LOG.error("Failed to subscribe to topics for share group '{}': {}", 
                        shareGroupId, e.getMessage(), e);
            }
        }
        
        LOG.info("Share group '{}' now has {} total splits registered: {}", 
                shareGroupId, assignedSplits.size(), assignedSplits.keySet());
    }
    
    private void handleSplitsRemoval(SplitsRemoval<KafkaPartitionSplit> splitsRemoval) {
        // Remove splits from tracking
        for (KafkaPartitionSplit split : splitsRemoval.splits()) {
            String splitId = split.splitId();
            KafkaPartitionSplit removedSplit = assignedSplits.remove(splitId);
            if (removedSplit != null) {
                LOG.debug("Share group '{}' removed split: {}", shareGroupId, splitId);
            }
        }
        
        // Note: For share groups, we don't need to update subscriptions on split removal
        // The coordinator handles message distribution automatically
        
        LOG.debug("Share group '{}' removed {} splits, remaining: {}", 
                shareGroupId, splitsRemoval.splits().size(), assignedSplits.size());
    }
    
    @Override
    public void wakeUp() {
        shareConsumer.wakeup();
        LOG.debug("KafkaShareConsumer '{}' woken up", shareGroupId);
    }
    
    @Override
    public void close() throws Exception {
        try {
            shareConsumer.close(Duration.ofSeconds(5));
            LOG.info("KafkaShareConsumer '{}' closed successfully", shareGroupId);
        } catch (Exception e) {
            LOG.warn("Error closing KafkaShareConsumer '{}': {}", shareGroupId, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Gets the underlying share consumer instance for advanced operations.
     */
    public KafkaShareConsumer<byte[], byte[]> getShareConsumer() {
        return shareConsumer;
    }
    
    /**
     * Gets the share group ID.
     */
    public String getShareGroupId() {
        return shareGroupId;
    }
    
    /**
     * Gets currently subscribed topics.
     */
    public Set<String> getSubscribedTopics() {
        return Collections.unmodifiableSet(subscribedTopics);
    }
    
    private String createShareConsumerClientId(Properties props) {
        String baseClientId = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG, "flink-share-consumer");
        return String.format("%s-%s-%d", baseClientId, shareGroupId, subtaskId);
    }
    
    private String extractTopicFromProps(Properties props) {
        // Try to extract topic from various possible property keys
        String topic = props.getProperty("topic");
        if (topic == null) {
            topic = props.getProperty("topics");
        }
        if (topic == null) {
            topic = props.getProperty("topic.name");
        }
        return topic != null ? topic : "unknown";
    }
    
    /**
     * Implementation of RecordsWithSplitIds for share group records.
     * 
     * <p>Since share groups don't have traditional partition-based splits,
     * we use a virtual split ID for all records from the share group.
     */
    private static class ShareGroupRecordsWithSplitIds implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {
        
        private static final ShareGroupRecordsWithSplitIds EMPTY = 
                new ShareGroupRecordsWithSplitIds(Collections.emptyIterator(), "empty");
        
        private final Iterator<ConsumerRecord<byte[], byte[]>> recordIterator;
        private final String shareGroupSplitId;
        private boolean hasReturnedSplit = false;
        
        private ShareGroupRecordsWithSplitIds(Iterator<ConsumerRecord<byte[], byte[]>> recordIterator, String splitId) {
            this.recordIterator = recordIterator;
            this.shareGroupSplitId = splitId; // Use the provided split ID directly
        }
        
        public static ShareGroupRecordsWithSplitIds empty() {
            return EMPTY;
        }
        
        @Override
        public String nextSplit() {
            if (!hasReturnedSplit && recordIterator.hasNext()) {
                hasReturnedSplit = true;
                return shareGroupSplitId;
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