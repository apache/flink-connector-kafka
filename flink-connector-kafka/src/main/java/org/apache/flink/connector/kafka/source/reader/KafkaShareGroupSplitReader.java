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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * SplitReader implementation for Kafka Share Groups that properly integrates with Flink's
 * connector architecture while using share group semantics.
 * 
 * <p>This reader subscribes to topics (not partitions) using KafkaShareConsumer and lets
 * Kafka's share group coordinator distribute messages across multiple readers at the record level.
 * 
 * <p>Key characteristics:
 * <ul>
 *   <li>Uses topic subscription instead of partition assignment</li>
 *   <li>Multiple readers can subscribe to the same topic</li>
 *   <li>Kafka coordinator distributes different messages to each reader</li>
 *   <li>Integrates with Flink's split management for proper lifecycle</li>
 * </ul>
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
    
    /**
     * Creates a share group split reader.
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
        
        // Ensure share group properties are set
        shareConsumerProps.setProperty("group.type", "share");
        this.shareGroupId = shareConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        
        Preconditions.checkNotNull(shareGroupId, "Share group ID (group.id) must be specified");
        
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
        this.shareConsumer = new KafkaShareConsumer<>(shareConsumerProps);
        
        LOG.info("Created KafkaShareGroupSplitReader for share group '{}' reader {} with proper Flink integration", 
                shareGroupId, readerId);
    }
    
    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        try {
            // Wait until at least one split is assigned
            if (assignedSplits.isEmpty()) {
                LOG.info("*** SPLIT READER: Share group '{}' reader {} waiting for split assignment (no splits assigned yet)", 
                        shareGroupId, readerId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            LOG.trace("*** SPLIT READER: Share group '{}' reader {} polling for records. Subscribed topics: {} (assigned splits: {})", 
                    shareGroupId, readerId, subscribedTopics, assignedSplits.keySet());
            
            // Poll for records using KafkaShareConsumer API
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
            
            if (consumerRecords.isEmpty()) {
                LOG.trace("*** SPLIT READER: Share group '{}' reader {} - no records returned from poll", shareGroupId, readerId);
                return ShareGroupRecordsWithSplitIds.empty();
            }
            
            LOG.info("*** SPLIT READER: Share group '{}' reader {} fetched {} records from topics: {} (partitions involved: {})", 
                    shareGroupId, readerId, consumerRecords.count(), subscribedTopics, 
                    consumerRecords.partitions());
            
            // Record metrics
            if (metrics != null) {
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    metrics.recordMessageReceived();
                    LOG.debug("*** SPLIT READER: Record received - topic: {}, partition: {}, offset: {}, key: {}", 
                            record.topic(), record.partition(), record.offset(), record.key());
                }
            }
            
            // Use the first available split ID for all records
            // In share groups, records can come from any partition, so we use any split ID
            String splitId = assignedSplits.keySet().iterator().next();
            
            return new ShareGroupRecordsWithSplitIds(consumerRecords.iterator(), splitId);
            
        } catch (WakeupException e) {
            LOG.debug("Share consumer '{}' reader {} woken up", shareGroupId, readerId);
            return ShareGroupRecordsWithSplitIds.empty();
        } catch (Exception e) {
            LOG.error("*** SPLIT READER ERROR: Error polling from KafkaShareConsumer for share group '{}' reader {}: {}", 
                    shareGroupId, readerId, e.getMessage(), e);
            throw new IOException("Failed to fetch records from share group: " + shareGroupId, e);
        }
    }
    
    @Override
    public void handleSplitsChanges(SplitsChange<KafkaShareGroupSplit> splitsChanges) {
        LOG.info("*** SPLIT READER: Share group '{}' reader {} handling splits changes: {}", 
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
        
        LOG.info("*** SPLIT READER: Share group '{}' reader {} received {} splits for topic subscription", 
                shareGroupId, readerId, newSplits.size());
        
        // Process splits and extract topics
        for (KafkaShareGroupSplit split : newSplits) {
            assignedSplits.put(split.splitId(), split);
            String topic = split.getTopicName();
            newTopics.add(topic);
            
            LOG.info("*** SPLIT READER: Share group '{}' reader {} assigned split: {} for topic: {}", 
                    shareGroupId, readerId, split.splitId(), topic);
        }
        
        // Update topic subscription
        subscribedTopics.addAll(newTopics);
        
        // Subscribe to topics using share group semantics
        if (!subscribedTopics.isEmpty()) {
            try {
                LOG.info("*** SPLIT READER: Share group '{}' reader {} SUBSCRIBING to topics: {}", 
                        shareGroupId, readerId, subscribedTopics);
                        
                shareConsumer.subscribe(subscribedTopics);
                
                LOG.info("*** SPLIT READER: Share group '{}' reader {} SUCCESS: Subscribed to topics {} - coordinator will distribute messages", 
                        shareGroupId, readerId, subscribedTopics);
            } catch (Exception e) {
                LOG.error("*** SPLIT READER ERROR: FAILED to subscribe to topics for share group '{}' reader {}: {}", 
                        shareGroupId, readerId, e.getMessage(), e);
            }
        }
        
        LOG.info("*** SPLIT READER: Share group '{}' reader {} completed split addition - {} topics subscribed, {} total splits", 
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
            shareConsumer.close(Duration.ofSeconds(5));
            LOG.info("KafkaShareConsumer for share group '{}' reader {} closed successfully", shareGroupId, readerId);
        } catch (Exception e) {
            LOG.warn("Error closing KafkaShareConsumer for share group '{}' reader {}: {}", 
                    shareGroupId, readerId, e.getMessage());
            throw e;
        }
    }
    
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