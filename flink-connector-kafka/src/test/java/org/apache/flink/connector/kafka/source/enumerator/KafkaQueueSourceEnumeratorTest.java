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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaQueueSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KafkaQueueSourceEnumerator} functionality.
 * Tests message-based split enumeration for queue semantics.
 */
class KafkaQueueSourceEnumeratorTest {

    @Mock
    private SplitEnumeratorContext<KafkaQueueSplit> mockContext;

    @Mock
    private KafkaSubscriber mockSubscriber;

    @Mock
    private OffsetsInitializer mockOffsetsInitializer;

    private Properties kafkaProperties;
    private static final String TOPIC = "test-queue-topic";
    private static final String SHARE_GROUP_ID = "test-share-group";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.type", "share");
        kafkaProperties.setProperty("group.id", SHARE_GROUP_ID);
        
        when(mockContext.currentParallelism()).thenReturn(2);
        when(mockContext.registeredReaders()).thenReturn(Collections.emptyMap());
    }

    @Test
    void testQueueEnumeratorCreation() {
        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        assertThat(enumerator).isNotNull();
        assertThat(enumerator.getShareGroupId()).isEqualTo(SHARE_GROUP_ID);
        assertThat(enumerator.isQueueSemanticsEnabled()).isTrue();
    }

    @Test
    void testQueueEnumeratorInitialization() {
        TopicPartition tp1 = new TopicPartition(TOPIC, 0);
        TopicPartition tp2 = new TopicPartition(TOPIC, 1);
        Set<TopicPartition> topicPartitions = Set.of(tp1, tp2);

        when(mockSubscriber.getSubscribedTopicPartitions(any())).thenReturn(topicPartitions);
        when(mockOffsetsInitializer.getPartitionOffsets(any(), any())).thenReturn(Collections.emptyMap());

        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        enumerator.start();

        // Verify enumerator discovers topic partitions for queue consumption
        Set<TopicPartition> discoveredPartitions = enumerator.getDiscoveredTopicPartitions();
        assertThat(discoveredPartitions).containsExactlyInAnyOrder(tp1, tp2);
    }

    @Test
    void testQueueSplitGeneration() {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        when(mockSubscriber.getSubscribedTopicPartitions(any())).thenReturn(Set.of(tp));

        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        // Simulate message availability for queue consumption
        List<String> availableMessages = Arrays.asList("msg1", "msg2", "msg3", "msg4", "msg5");
        enumerator.setAvailableMessages(tp, availableMessages);

        // Generate queue splits based on message availability
        List<KafkaQueueSplit> splits = enumerator.generateQueueSplits(2); // 2 readers

        assertThat(splits).hasSize(2);
        
        // Verify splits contain distributed messages
        int totalMessages = splits.stream().mapToInt(split -> split.getMessageIds().size()).sum();
        assertThat(totalMessages).isEqualTo(5);
        
        // Verify all splits belong to the same share group
        for (KafkaQueueSplit split : splits) {
            assertThat(split.getShareGroupId()).isEqualTo(SHARE_GROUP_ID);
            assertThat(split.getTopicPartition()).isEqualTo(tp);
        }
    }

    @Test
    void testReaderRegistrationAndSplitAssignment() {
        when(mockContext.registeredReaders()).thenReturn(Collections.singletonMap(1, "reader-1"));
        
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        when(mockSubscriber.getSubscribedTopicPartitions(any())).thenReturn(Set.of(tp));

        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        enumerator.start();

        // Register a new reader
        enumerator.addReader(1);

        // Simulate message availability
        List<String> messages = Arrays.asList("msg1", "msg2", "msg3");
        enumerator.setAvailableMessages(tp, messages);

        // Assign splits to the registered reader
        enumerator.handleSplitRequest(1, "reader-1");

        // Verify split assignment occurred
        assertThat(enumerator.hasAssignedSplits(1)).isTrue();
        assertThat(enumerator.getAssignedSplits(1)).isNotEmpty();
    }

    @Test
    void testQueueRebalancing() {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        when(mockSubscriber.getSubscribedTopicPartitions(any())).thenReturn(Set.of(tp));

        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        enumerator.start();

        // Register initial readers
        enumerator.addReader(1);
        enumerator.addReader(2);

        // Set available messages
        List<String> messages = Arrays.asList("msg1", "msg2", "msg3", "msg4", "msg5", "msg6");
        enumerator.setAvailableMessages(tp, messages);

        // Initial split assignment
        enumerator.handleSplitRequest(1, "reader-1");
        enumerator.handleSplitRequest(2, "reader-2");

        int initialSplitsReader1 = enumerator.getAssignedSplits(1).size();
        int initialSplitsReader2 = enumerator.getAssignedSplits(2).size();

        // Add a new reader - should trigger rebalancing
        enumerator.addReader(3);
        enumerator.rebalanceQueueSplits();

        // Verify rebalancing occurred
        assertThat(enumerator.getAssignedSplits(1).size()).isLessThanOrEqualTo(initialSplitsReader1);
        assertThat(enumerator.getAssignedSplits(2).size()).isLessThanOrEqualTo(initialSplitsReader2);
        assertThat(enumerator.getAssignedSplits(3)).isNotEmpty();
    }

    @Test
    void testSplitCompletion() {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        KafkaQueueSplit completedSplit = new KafkaQueueSplit(
                tp, SHARE_GROUP_ID, 0L, 3L, Arrays.asList("msg1", "msg2", "msg3"));
        
        // Mark all messages as processed
        completedSplit.markMessageAsProcessed("msg1");
        completedSplit.markMessageAsProcessed("msg2");
        completedSplit.markMessageAsProcessed("msg3");

        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        enumerator.addSplitsBack(Collections.singletonList(completedSplit), 1);

        // Verify completed split is not reassigned
        assertThat(enumerator.hasCompletedSplit(completedSplit.splitId())).isTrue();
        assertThat(enumerator.getPendingSplits()).doesNotContain(completedSplit);
    }

    @Test
    void testMessageDeduplication() {
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        
        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        // Add duplicate messages
        List<String> messages1 = Arrays.asList("msg1", "msg2", "msg3");
        List<String> messages2 = Arrays.asList("msg2", "msg3", "msg4"); // Contains duplicates

        enumerator.setAvailableMessages(tp, messages1);
        enumerator.addAvailableMessages(tp, messages2);

        List<String> allMessages = enumerator.getAvailableMessages(tp);
        
        // Verify deduplication occurred
        assertThat(allMessages).containsExactlyInAnyOrder("msg1", "msg2", "msg3", "msg4");
    }

    @Test
    void testQueueEnumeratorState() {
        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        // Test state serialization for checkpointing
        KafkaQueueSourceEnumState state = enumerator.snapshotState(1L);
        
        assertThat(state).isNotNull();
        assertThat(state.getShareGroupId()).isEqualTo(SHARE_GROUP_ID);
        assertThat(state.getCheckpointId()).isEqualTo(1L);
    }

    @Test
    void testQueueEnumeratorValidation() {
        Properties invalidProps = new Properties();
        invalidProps.setProperty("bootstrap.servers", "localhost:9092");
        invalidProps.setProperty("group.type", "consumer"); // Invalid for queue semantics

        assertThatThrownBy(() -> new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                invalidProps,
                mockContext,
                SHARE_GROUP_ID))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("group.type must be 'share' for queue semantics");
    }

    @Test
    void testQueueMetrics() {
        KafkaQueueSourceEnumerator enumerator = new KafkaQueueSourceEnumerator(
                mockSubscriber,
                mockOffsetsInitializer,
                kafkaProperties,
                mockContext,
                SHARE_GROUP_ID
        );

        TopicPartition tp = new TopicPartition(TOPIC, 0);
        List<String> messages = Arrays.asList("msg1", "msg2", "msg3", "msg4", "msg5");
        enumerator.setAvailableMessages(tp, messages);

        // Test queue-specific metrics
        assertThat(enumerator.getTotalAvailableMessages()).isEqualTo(5);
        assertThat(enumerator.getActiveReaderCount()).isEqualTo(0);
        assertThat(enumerator.getPendingSplitCount()).isEqualTo(0);

        // Add readers and generate splits
        enumerator.addReader(1);
        enumerator.addReader(2);
        List<KafkaQueueSplit> splits = enumerator.generateQueueSplits(2);

        assertThat(enumerator.getActiveReaderCount()).isEqualTo(2);
        assertThat(enumerator.getPendingSplitCount()).isEqualTo(splits.size());
    }
}