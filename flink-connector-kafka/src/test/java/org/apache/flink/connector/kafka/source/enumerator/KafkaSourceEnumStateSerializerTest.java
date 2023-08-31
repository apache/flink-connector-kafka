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

import org.apache.flink.connector.base.source.utils.SerdeUtils;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KafkaSourceEnumStateSerializer}. */
public class KafkaSourceEnumStateSerializerTest {

    private static final int NUM_READERS = 10;
    private static final String TOPIC_PREFIX = "topic-";
    private static final int NUM_PARTITIONS_PER_TOPIC = 10;
    private static final long STARTING_OFFSET = KafkaPartitionSplit.EARLIEST_OFFSET;

    @Test
    public void testEnumStateSerde() throws IOException {
        final KafkaSourceEnumState state =
                new KafkaSourceEnumState(
                        constructTopicPartitions(0),
                        constructTopicPartitions(NUM_PARTITIONS_PER_TOPIC),
                        true);
        final KafkaSourceEnumStateSerializer serializer = new KafkaSourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final KafkaSourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restoredState.assignedPartitions()).isEqualTo(state.assignedPartitions());
        assertThat(restoredState.unassignedInitialPartitions())
                .isEqualTo(state.unassignedInitialPartitions());
        assertThat(restoredState.initialDiscoveryFinished()).isTrue();
    }

    @Test
    public void testBackwardCompatibility() throws IOException {

        final Set<TopicPartition> topicPartitions = constructTopicPartitions(0);
        final Map<Integer, Set<KafkaPartitionSplit>> splitAssignments =
                toSplitAssignments(topicPartitions);

        // Create bytes in the way of KafkaEnumStateSerializer version 0 doing serialization
        final byte[] bytesV0 =
                SerdeUtils.serializeSplitAssignments(
                        splitAssignments, new KafkaPartitionSplitSerializer());
        // Create bytes in the way of KafkaEnumStateSerializer version 1 doing serialization
        final byte[] bytesV1 =
                KafkaSourceEnumStateSerializer.serializeTopicPartitions(topicPartitions);

        // Deserialize above bytes with KafkaEnumStateSerializer version 2 to check backward
        // compatibility
        final KafkaSourceEnumState kafkaSourceEnumStateV0 =
                new KafkaSourceEnumStateSerializer().deserialize(0, bytesV0);
        final KafkaSourceEnumState kafkaSourceEnumStateV1 =
                new KafkaSourceEnumStateSerializer().deserialize(1, bytesV1);

        assertThat(kafkaSourceEnumStateV0.assignedPartitions()).isEqualTo(topicPartitions);
        assertThat(kafkaSourceEnumStateV0.unassignedInitialPartitions()).isEmpty();
        assertThat(kafkaSourceEnumStateV0.initialDiscoveryFinished()).isTrue();

        assertThat(kafkaSourceEnumStateV1.assignedPartitions()).isEqualTo(topicPartitions);
        assertThat(kafkaSourceEnumStateV1.unassignedInitialPartitions()).isEmpty();
        assertThat(kafkaSourceEnumStateV1.initialDiscoveryFinished()).isTrue();
    }

    private Set<TopicPartition> constructTopicPartitions(int startPartition) {
        // Create topic partitions for readers.
        // Reader i will be assigned with NUM_PARTITIONS_PER_TOPIC splits, with topic name
        // "topic-{i}" and
        // NUM_PARTITIONS_PER_TOPIC partitions. The starting partition number is startPartition
        // Totally NUM_READERS * NUM_PARTITIONS_PER_TOPIC partitions will be created.
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            for (int partition = startPartition;
                    partition < startPartition + NUM_PARTITIONS_PER_TOPIC;
                    partition++) {
                topicPartitions.add(new TopicPartition(TOPIC_PREFIX + readerId, partition));
            }
        }
        return topicPartitions;
    }

    private Map<Integer, Set<KafkaPartitionSplit>> toSplitAssignments(
            Collection<TopicPartition> topicPartitions) {
        // Assign splits to readers according to topic name. For example, topic "topic-5" will be
        // assigned to reader with ID=5
        Map<Integer, Set<KafkaPartitionSplit>> splitAssignments = new HashMap<>();
        topicPartitions.forEach(
                (tp) ->
                        splitAssignments
                                .computeIfAbsent(
                                        Integer.valueOf(
                                                tp.topic().substring(TOPIC_PREFIX.length())),
                                        HashSet::new)
                                .add(new KafkaPartitionSplit(tp, STARTING_OFFSET)));
        return splitAssignments;
    }
}
