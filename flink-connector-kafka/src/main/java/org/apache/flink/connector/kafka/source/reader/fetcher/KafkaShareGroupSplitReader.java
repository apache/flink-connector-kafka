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

package org.apache.flink.connector.kafka.source.reader.fetcher;

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A {@link SplitReader} implementation that handles Kafka share group consumption using KafkaShareConsumer.
 * This implementation uses the Kafka 4.1.0+ KafkaShareConsumer API to support true share group semantics.
 *
 * <p>Key differences from traditional consumer groups:
 * <ul>
 *   <li><strong>Uses KafkaShareConsumer instead of KafkaConsumer</strong> - The new API for share groups</li>
 *   <li><strong>Messages distributed at message level, not partition level</strong> - Better load balancing</li>
 *   <li><strong>No manual partition assignment</strong> - Topics are subscribed to, partitions handled automatically</li>
 *   <li><strong>Automatic load balancing</strong> - Kafka coordinator handles message distribution</li>
 *   <li><strong>Queue-like semantics</strong> - Each message delivered to exactly one consumer in the group</li>
 * </ul>
 *
 * <p>This is the correct implementation for Kafka 4.1.0+ share groups, replacing the traditional
 * partition-based assignment model with message-level distribution.
 */
public class KafkaShareGroupSplitReader implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSplitReader.class);
    
    private final KafkaShareConsumer<byte[], byte[]> shareConsumer;
    private final Map<String, KafkaPartitionSplit> assignedSplits;
    private final String shareGroupId;
    private final List<String> subscribedTopics;

    /**
     * Creates a new KafkaShareGroupSplitReader that uses the KafkaShareConsumer API.
     * 
     * @param consumerProps consumer properties
     * @param shareGroupId the share group identifier
     * @param topics topics to subscribe to
     */
    public KafkaShareGroupSplitReader(
            Properties consumerProps, 
            String shareGroupId,
            Collection<String> topics) {
        // Ensure share group properties are set correctly
        Properties shareGroupProps = new Properties();
        shareGroupProps.putAll(consumerProps);
        shareGroupProps.setProperty("group.type", "share");
        shareGroupProps.setProperty("group.id", shareGroupId);
        // Disable auto-commit as Flink handles acknowledgment
        shareGroupProps.setProperty("enable.auto.commit", "false");
        
        // Create KafkaShareConsumer instead of traditional KafkaConsumer
        this.shareConsumer = new KafkaShareConsumer<>(shareGroupProps);
        this.assignedSplits = new HashMap<>();
        this.shareGroupId = shareGroupId;
        this.subscribedTopics = new ArrayList<>(topics);
        
        // Subscribe to topics (share groups use subscription, not assignment)
        if (!topics.isEmpty()) {
            this.shareConsumer.subscribe(topics);
            LOG.info("KafkaShareConsumer for share group '{}' subscribed to topics: {}", shareGroupId, topics);
        }
        
        LOG.info("Created KafkaShareConsumer for share group '{}' with {} topics", shareGroupId, topics.size());
    }

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        try {
            // Use KafkaShareConsumer.poll() - this fetches messages distributed by the share group coordinator
            // Messages are automatically balanced across consumers in the share group at the message level
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(Duration.ofMillis(100));
            
            if (consumerRecords.isEmpty()) {
                return new KafkaRecordsWithSplitIds(Collections.emptyIterator());
            }

            LOG.debug("KafkaShareConsumer '{}' fetched {} records from share group", shareGroupId, consumerRecords.count());
            
            // Note: In share groups, records are automatically acknowledged unless explicit acknowledgment is configured
            // The KafkaShareConsumer handles this based on the acknowledgment mode configuration
            return new KafkaRecordsWithSplitIds(consumerRecords.iterator());
            
        } catch (Exception e) {
            LOG.error("Error fetching records from KafkaShareConsumer for share group '{}': {}", shareGroupId, e.getMessage());
            throw new IOException("Error fetching records from KafkaShareConsumer for share group: " + shareGroupId, e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {
        // Share groups handle subscription automatically, but we track splits for compatibility with Flink's split system
        if (splitsChanges instanceof SplitsAddition) {
            handleSplitsAddition((SplitsAddition<KafkaPartitionSplit>) splitsChanges);
        } else if (splitsChanges instanceof SplitsRemoval) {
            handleSplitsRemoval((SplitsRemoval<KafkaPartitionSplit>) splitsChanges);
        }
        
        LOG.debug("Share group '{}' handled split changes: {}", shareGroupId, splitsChanges.getClass().getSimpleName());
    }

    private void handleSplitsAddition(SplitsAddition<KafkaPartitionSplit> splitsAddition) {
        // In share groups, we don't manually assign partitions like traditional consumer groups
        // Instead, we track splits for metadata and compatibility with Flink's split system
        for (KafkaPartitionSplit split : splitsAddition.splits()) {
            assignedSplits.put(split.splitId(), split);
            
            // Extract topics from splits to ensure subscription includes all necessary topics
            String topic = split.getTopicPartition().topic();
            if (!subscribedTopics.contains(topic)) {
                subscribedTopics.add(topic);
                // Update subscription to include new topic
                shareConsumer.subscribe(subscribedTopics);
                LOG.info("Share group '{}' subscription updated to include topic: {}", shareGroupId, topic);
            }
        }
        
        LOG.debug("Share group '{}' added {} splits, total tracked: {}", 
                shareGroupId, splitsAddition.splits().size(), assignedSplits.size());
    }

    private void handleSplitsRemoval(SplitsRemoval<KafkaPartitionSplit> splitsRemoval) {
        // Remove splits from tracking (share group coordinator handles the actual unsubscription)
        for (String splitId : splitsRemoval.splitIds()) {
            assignedSplits.remove(splitId);
        }
        
        LOG.debug("Share group '{}' removed {} splits, remaining tracked: {}", 
                shareGroupId, splitsRemoval.splitIds().size(), assignedSplits.size());
    }

    @Override
    public void wakeUp() {
        shareConsumer.wakeup();
        LOG.debug("KafkaShareConsumer '{}' woken up", shareGroupId);
    }

    @Override
    public void close() throws Exception {
        try {
            shareConsumer.close();
            LOG.info("KafkaShareConsumer '{}' closed successfully", shareGroupId);
        } catch (Exception e) {
            LOG.warn("Error closing KafkaShareConsumer '{}': {}", shareGroupId, e.getMessage());
            throw e;
        }
    }

    /**
     * Gets the underlying KafkaShareConsumer instance for advanced operations.
     * 
     * @return the share consumer instance
     */
    public KafkaShareConsumer<byte[], byte[]> getShareConsumer() {
        return shareConsumer;
    }

    /**
     * Gets the share group ID associated with this reader.
     * 
     * @return the share group ID
     */
    public String getShareGroupId() {
        return shareGroupId;
    }

    /**
     * Gets the topics currently subscribed by the share consumer.
     * 
     * @return collection of subscribed topics
     */
    public Collection<String> getSubscribedTopics() {
        return Collections.unmodifiableCollection(subscribedTopics);
    }

    /**
     * Simple implementation of RecordsWithSplitIds for share group records.
     */
    private static class KafkaRecordsWithSplitIds implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {
        private final java.util.Iterator<ConsumerRecord<byte[], byte[]>> recordIterator;
        
        public KafkaRecordsWithSplitIds(java.util.Iterator<ConsumerRecord<byte[], byte[]>> recordIterator) {
            this.recordIterator = recordIterator;
        }
        
        @Override
        public String nextSplit() {
            return recordIterator.hasNext() ? "share-group-split" : null;
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
        public Collection<String> finishedSplits() {
            return Collections.emptyList();
        }
    }
}