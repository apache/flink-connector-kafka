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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A {@link SplitReader} implementation that handles Kafka share group consumption.
 * This wrapper extends the existing Kafka split reader to support share group semantics
 * available in Kafka 4.1.0+.
 *
 * <p>Share groups provide queue-like semantics where messages are distributed automatically
 * across consumers without manual partition assignment.
 */
public class KafkaShareGroupSplitReader implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSplitReader.class);
    
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Map<String, KafkaPartitionSplit> assignedSplits;
    private final boolean isShareGroupEnabled;
    private final String shareGroupId;

    public KafkaShareGroupSplitReader(
            Properties consumerProps, 
            String shareGroupId) {
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.assignedSplits = new HashMap<>();
        this.shareGroupId = shareGroupId;
        this.isShareGroupEnabled = "share".equals(consumerProps.getProperty("group.type"));
        
        LOG.info("Created Kafka share group split reader for share group: {}, enabled: {}", 
                shareGroupId, isShareGroupEnabled);
    }

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        try {
            ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
            
            if (consumerRecords.isEmpty()) {
                return new KafkaRecordsWithSplitIds(Collections.emptyIterator());
            }

            return new KafkaRecordsWithSplitIds(consumerRecords.iterator());
            
        } catch (Exception e) {
            throw new IOException("Error fetching records from Kafka share group consumer", e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            handleSplitsAddition((SplitsAddition<KafkaPartitionSplit>) splitsChanges);
        } else if (splitsChanges instanceof SplitsRemoval) {
            handleSplitsRemoval((SplitsRemoval<KafkaPartitionSplit>) splitsChanges);
        }
    }

    @Override
    public void wakeUp() {
        consumer.wakeup();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Kafka share group split reader for share group: {}", shareGroupId);
        consumer.close();
    }

    private void handleSplitsAddition(SplitsAddition<KafkaPartitionSplit> splitsAddition) {
        Collection<KafkaPartitionSplit> newSplits = splitsAddition.splits();
        
        for (KafkaPartitionSplit split : newSplits) {
            assignedSplits.put(split.splitId(), split);
        }

        if (isShareGroupEnabled) {
            // For share groups, subscribe to topics (not individual partitions)
            Set<String> topics = extractTopicsFromSplits(newSplits);
            if (!topics.isEmpty()) {
                LOG.info("Subscribing to topics for share group {}: {}", shareGroupId, topics);
                consumer.subscribe(topics);
            }
        } else {
            // Fallback to traditional partition assignment
            Set<TopicPartition> partitions = extractPartitionsFromSplits(newSplits);
            if (!partitions.isEmpty()) {
                LOG.info("Assigning partitions: {}", partitions);
                consumer.assign(partitions);
                
                // Seek to starting offsets for each partition
                for (KafkaPartitionSplit split : newSplits) {
                    TopicPartition tp = split.getTopicPartition();
                    long startingOffset = split.getStartingOffset();
                    if (startingOffset >= 0) {
                        consumer.seek(tp, startingOffset);
                    }
                }
            }
        }
    }

    private void handleSplitsRemoval(SplitsRemoval<KafkaPartitionSplit> splitsRemoval) {
        // Note: In some Flink versions, the method might be different
        // Try to get finished split IDs safely
        try {
            Collection<String> finishedSplitIds = null;
            
            // Try different method names that might exist
            try {
                finishedSplitIds = (Collection<String>) splitsRemoval.getClass()
                    .getMethod("splitIds").invoke(splitsRemoval);
            } catch (Exception e) {
                try {
                    finishedSplitIds = (Collection<String>) splitsRemoval.getClass()
                        .getMethod("getSplitIds").invoke(splitsRemoval);
                } catch (Exception e2) {
                    LOG.warn("Could not access split IDs from SplitsRemoval: {}", e2.getMessage());
                    return;
                }
            }
            
            if (finishedSplitIds != null) {
                for (String splitId : finishedSplitIds) {
                    assignedSplits.remove(splitId);
                }
                LOG.debug("Removed {} finished splits from share group reader", finishedSplitIds.size());
            }
        } catch (Exception e) {
            LOG.warn("Error handling splits removal: {}", e.getMessage());
        }
    }

    private Set<String> extractTopicsFromSplits(Collection<KafkaPartitionSplit> splits) {
        Set<String> topics = new java.util.HashSet<>();
        for (KafkaPartitionSplit split : splits) {
            topics.add(split.getTopicPartition().topic());
        }
        return topics;
    }

    private Set<TopicPartition> extractPartitionsFromSplits(Collection<KafkaPartitionSplit> splits) {
        Set<TopicPartition> partitions = new java.util.HashSet<>();
        for (KafkaPartitionSplit split : splits) {
            partitions.add(split.getTopicPartition());
        }
        return partitions;
    }

    /**
     * Implementation of RecordsWithSplitIds for Kafka records.
     */
    private static class KafkaRecordsWithSplitIds implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {
        private final Iterator<ConsumerRecord<byte[], byte[]>> recordsIterator;
        private String currentSplitId;
        private ConsumerRecord<byte[], byte[]> currentRecord;

        public KafkaRecordsWithSplitIds(Iterator<ConsumerRecord<byte[], byte[]>> recordsIterator) {
            this.recordsIterator = recordsIterator;
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (recordsIterator.hasNext()) {
                currentRecord = recordsIterator.next();
                TopicPartition tp = new TopicPartition(currentRecord.topic(), currentRecord.partition());
                currentSplitId = KafkaPartitionSplit.toSplitId(tp);
                return currentSplitId;
            }
            return null;
        }

        @Override
        @Nullable
        public ConsumerRecord<byte[], byte[]> nextRecordFromSplit() {
            return currentRecord;
        }

        @Override
        public Set<String> finishedSplits() {
            return Collections.emptySet(); // Share groups handle split completion automatically
        }
    }
}