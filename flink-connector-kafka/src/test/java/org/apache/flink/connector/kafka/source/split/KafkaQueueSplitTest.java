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

package org.apache.flink.connector.kafka.source.split;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaQueueSplit} functionality.
 * Tests message-based split handling for queue semantics.
 */
class KafkaQueueSplitTest {

    private static final String TOPIC = "test-queue-topic";
    private static final String SHARE_GROUP_ID = "test-share-group";
    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;

    @Test
    void testQueueSplitCreation() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                0L, // startingOffset
                100L, // messageCount
                Arrays.asList("msg1", "msg2", "msg3")
        );

        assertThat(split.getTopicPartition()).isEqualTo(topicPartition);
        assertThat(split.getShareGroupId()).isEqualTo(SHARE_GROUP_ID);
        assertThat(split.getStartingOffset()).isEqualTo(0L);
        assertThat(split.getMessageCount()).isEqualTo(100L);
        assertThat(split.getMessageIds()).containsExactly("msg1", "msg2", "msg3");
        assertThat(split.splitId()).contains(TOPIC).contains(String.valueOf(PARTITION_0));
    }

    @Test
    void testQueueSplitWithNoMessages() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                0L,
                0L,
                Arrays.asList()
        );

        assertThat(split.getMessageCount()).isEqualTo(0L);
        assertThat(split.getMessageIds()).isEmpty();
        assertThat(split.isEmpty()).isTrue();
    }

    @Test
    void testQueueSplitEquality() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        List<String> messageIds = Arrays.asList("msg1", "msg2");
        
        KafkaQueueSplit split1 = new KafkaQueueSplit(topicPartition, SHARE_GROUP_ID, 0L, 2L, messageIds);
        KafkaQueueSplit split2 = new KafkaQueueSplit(topicPartition, SHARE_GROUP_ID, 0L, 2L, messageIds);
        KafkaQueueSplit split3 = new KafkaQueueSplit(
                new TopicPartition(TOPIC, PARTITION_1), SHARE_GROUP_ID, 0L, 2L, messageIds);

        assertThat(split1).isEqualTo(split2);
        assertThat(split1.hashCode()).isEqualTo(split2.hashCode());
        assertThat(split1).isNotEqualTo(split3);
    }

    @Test
    void testQueueSplitToString() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                100L,
                5L,
                Arrays.asList("msg1", "msg2", "msg3", "msg4", "msg5")
        );

        String splitString = split.toString();
        assertThat(splitString)
                .contains(TOPIC)
                .contains(String.valueOf(PARTITION_0))
                .contains(SHARE_GROUP_ID)
                .contains("100")  // startingOffset
                .contains("5");   // messageCount
    }

    @Test
    void testQueueSplitValidation() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        
        // Test null topic partition
        assertThatThrownBy(() -> 
            new KafkaQueueSplit(null, SHARE_GROUP_ID, 0L, 0L, Arrays.asList()))
            .isInstanceOf(NullPointerException.class);

        // Test null share group ID
        assertThatThrownBy(() -> 
            new KafkaQueueSplit(topicPartition, null, 0L, 0L, Arrays.asList()))
            .isInstanceOf(NullPointerException.class);

        // Test negative message count
        assertThatThrownBy(() -> 
            new KafkaQueueSplit(topicPartition, SHARE_GROUP_ID, 0L, -1L, Arrays.asList()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Message count cannot be negative");

        // Test null message IDs
        assertThatThrownBy(() -> 
            new KafkaQueueSplit(topicPartition, SHARE_GROUP_ID, 0L, 0L, null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testQueueSplitMessageIdManagement() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        List<String> initialMessages = Arrays.asList("msg1", "msg2", "msg3");
        
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                0L,
                3L,
                initialMessages
        );

        // Test adding message IDs
        split.addMessageId("msg4");
        assertThat(split.getMessageIds()).containsExactly("msg1", "msg2", "msg3", "msg4");
        assertThat(split.getMessageCount()).isEqualTo(4L);

        // Test removing processed message IDs
        split.removeProcessedMessageId("msg1");
        assertThat(split.getMessageIds()).containsExactly("msg2", "msg3", "msg4");
        assertThat(split.getUnprocessedMessageCount()).isEqualTo(3L);

        // Test marking message as processed
        split.markMessageAsProcessed("msg2");
        assertThat(split.getProcessedMessageIds()).contains("msg2");
        assertThat(split.isMessageProcessed("msg2")).isTrue();
        assertThat(split.isMessageProcessed("msg3")).isFalse();
    }

    @Test
    void testQueueSplitOffset() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                100L,
                5L,
                Arrays.asList("msg1", "msg2", "msg3", "msg4", "msg5")
        );

        // Test offset tracking for queue semantics
        assertThat(split.getStartingOffset()).isEqualTo(100L);
        
        // Update offset as messages are processed
        split.updateOffset(105L);
        assertThat(split.getCurrentOffset()).isEqualTo(105L);
        
        // Test offset validation
        assertThatThrownBy(() -> split.updateOffset(99L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Offset cannot go backwards");
    }

    @Test
    void testQueueSplitCompletion() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                0L,
                3L,
                Arrays.asList("msg1", "msg2", "msg3")
        );

        assertThat(split.isCompleted()).isFalse();
        
        // Process all messages
        split.markMessageAsProcessed("msg1");
        split.markMessageAsProcessed("msg2");
        assertThat(split.isCompleted()).isFalse();
        
        split.markMessageAsProcessed("msg3");
        assertThat(split.isCompleted()).isTrue();
        assertThat(split.getCompletionPercentage()).isEqualTo(100.0);
    }

    @Test
    void testQueueSplitSerialization() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        List<String> messageIds = Arrays.asList("msg1", "msg2", "msg3");
        
        KafkaQueueSplit originalSplit = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                100L,
                3L,
                messageIds
        );

        // Test that split can be converted to/from byte array for checkpointing
        byte[] serialized = originalSplit.toBytes();
        KafkaQueueSplit deserializedSplit = KafkaQueueSplit.fromBytes(serialized);

        assertThat(deserializedSplit).isEqualTo(originalSplit);
        assertThat(deserializedSplit.getTopicPartition()).isEqualTo(originalSplit.getTopicPartition());
        assertThat(deserializedSplit.getShareGroupId()).isEqualTo(originalSplit.getShareGroupId());
        assertThat(deserializedSplit.getMessageIds()).isEqualTo(originalSplit.getMessageIds());
    }

    @Test
    void testQueueSplitMetrics() {
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION_0);
        
        KafkaQueueSplit split = new KafkaQueueSplit(
                topicPartition, 
                SHARE_GROUP_ID,
                0L,
                10L,
                Arrays.asList("msg1", "msg2", "msg3", "msg4", "msg5", "msg6", "msg7", "msg8", "msg9", "msg10")
        );

        // Test metrics for queue split processing
        assertThat(split.getTotalMessageCount()).isEqualTo(10L);
        assertThat(split.getProcessedMessageCount()).isEqualTo(0L);
        assertThat(split.getUnprocessedMessageCount()).isEqualTo(10L);
        assertThat(split.getCompletionPercentage()).isEqualTo(0.0);

        // Process some messages
        split.markMessageAsProcessed("msg1");
        split.markMessageAsProcessed("msg2");
        split.markMessageAsProcessed("msg3");

        assertThat(split.getProcessedMessageCount()).isEqualTo(3L);
        assertThat(split.getUnprocessedMessageCount()).isEqualTo(7L);
        assertThat(split.getCompletionPercentage()).isEqualTo(30.0);
    }
}