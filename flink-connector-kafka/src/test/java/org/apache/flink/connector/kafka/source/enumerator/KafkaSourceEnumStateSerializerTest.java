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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KafkaSourceEnumStateSerializer}. */
class KafkaSourceEnumStateSerializerTest {

    private static final int NUM_READERS = 10;
    private static final String TOPIC_PREFIX = "topic-";
    private static final int NUM_PARTITIONS_PER_TOPIC = 10;

    @Test
    void testEnumStateSerde() throws IOException {
        final KafkaSourceEnumState state =
                new KafkaSourceEnumState(
                        constructTopicSplits(0),
                        constructTopicSplits(NUM_PARTITIONS_PER_TOPIC),
                        true);
        final KafkaSourceEnumStateSerializer serializer = new KafkaSourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final KafkaSourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restoredState.assignedSplits())
                .containsExactlyInAnyOrderElementsOf(state.assignedSplits());
        assertThat(restoredState.unassignedSplits())
                .containsExactlyInAnyOrderElementsOf(state.unassignedSplits());
        assertThat(restoredState.initialDiscoveryFinished()).isTrue();
    }

    @Test
    void testBackwardCompatibility() throws IOException {

        final Set<KafkaPartitionSplit> splits = constructTopicSplits(0);
        final Map<Integer, Collection<KafkaPartitionSplit>> splitAssignments =
                toSplitAssignments(splits);
        final List<SplitAndAssignmentStatus> splitAndAssignmentStatuses =
                splits.stream()
                        .map(
                                split ->
                                        new SplitAndAssignmentStatus(
                                                split, getAssignmentStatus(split)))
                        .collect(Collectors.toList());

        // Create bytes in the way of KafkaEnumStateSerializer version 0 doing serialization
        final byte[] bytesV0 =
                SerdeUtils.serializeSplitAssignments(
                        splitAssignments, new KafkaPartitionSplitSerializer());
        // Create bytes in the way of KafkaEnumStateSerializer version 1 doing serialization
        final byte[] bytesV1 = KafkaSourceEnumStateSerializer.serializeV1(splits);
        final byte[] bytesV2 =
                KafkaSourceEnumStateSerializer.serializeV2(splitAndAssignmentStatuses, false);

        // Deserialize above bytes with KafkaEnumStateSerializer version 2 to check backward
        // compatibility
        final KafkaSourceEnumState kafkaSourceEnumStateV0 =
                new KafkaSourceEnumStateSerializer().deserialize(0, bytesV0);
        final KafkaSourceEnumState kafkaSourceEnumStateV1 =
                new KafkaSourceEnumStateSerializer().deserialize(1, bytesV1);
        final KafkaSourceEnumState kafkaSourceEnumStateV2 =
                new KafkaSourceEnumStateSerializer().deserialize(2, bytesV2);

        assertThat(kafkaSourceEnumStateV0.assignedSplits())
                .containsExactlyInAnyOrderElementsOf(splits);
        assertThat(kafkaSourceEnumStateV0.unassignedSplits()).isEmpty();
        assertThat(kafkaSourceEnumStateV0.initialDiscoveryFinished()).isTrue();

        assertThat(kafkaSourceEnumStateV1.assignedSplits())
                .containsExactlyInAnyOrderElementsOf(splits);
        assertThat(kafkaSourceEnumStateV1.unassignedSplits()).isEmpty();
        assertThat(kafkaSourceEnumStateV1.initialDiscoveryFinished()).isTrue();

        final Map<AssignmentStatus, Set<KafkaPartitionSplit>> splitsByStatus =
                splitAndAssignmentStatuses.stream()
                        .collect(
                                Collectors.groupingBy(
                                        SplitAndAssignmentStatus::assignmentStatus,
                                        Collectors.mapping(
                                                SplitAndAssignmentStatus::split,
                                                Collectors.toSet())));
        assertThat(kafkaSourceEnumStateV2.assignedSplits())
                .containsExactlyInAnyOrderElementsOf(splitsByStatus.get(AssignmentStatus.ASSIGNED));
        assertThat(kafkaSourceEnumStateV2.unassignedSplits())
                .containsExactlyInAnyOrderElementsOf(
                        splitsByStatus.get(AssignmentStatus.UNASSIGNED));
        assertThat(kafkaSourceEnumStateV2.initialDiscoveryFinished()).isFalse();
    }

    private static AssignmentStatus getAssignmentStatus(KafkaPartitionSplit split) {
        return AssignmentStatus.values()[
                Math.abs(split.hashCode()) % AssignmentStatus.values().length];
    }

    private Set<KafkaPartitionSplit> constructTopicSplits(int startPartition) {
        // Create topic partitions for readers.
        // Reader i will be assigned with NUM_PARTITIONS_PER_TOPIC splits, with topic name
        // "topic-{i}" and
        // NUM_PARTITIONS_PER_TOPIC partitions. The starting partition number is startPartition
        // Totally NUM_READERS * NUM_PARTITIONS_PER_TOPIC partitions will be created.
        Set<KafkaPartitionSplit> topicPartitions = new HashSet<>();
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            for (int partition = startPartition;
                    partition < startPartition + NUM_PARTITIONS_PER_TOPIC;
                    partition++) {
                topicPartitions.add(
                        new KafkaPartitionSplit(
                                new TopicPartition(TOPIC_PREFIX + readerId, partition),
                                KafkaPartitionSplit.MIGRATED));
            }
        }
        return topicPartitions;
    }

    private Map<Integer, Collection<KafkaPartitionSplit>> toSplitAssignments(
            Collection<KafkaPartitionSplit> splits) {
        // Assign splits to readers according to topic name. For example, topic "topic-5" will be
        // assigned to reader with ID=5
        Map<Integer, Collection<KafkaPartitionSplit>> splitAssignments = new HashMap<>();
        for (KafkaPartitionSplit split : splits) {
            splitAssignments
                    .computeIfAbsent(
                            Integer.valueOf(split.getTopic().substring(TOPIC_PREFIX.length())),
                            HashSet::new)
                    .add(split);
        }
        return splitAssignments;
    }
}
