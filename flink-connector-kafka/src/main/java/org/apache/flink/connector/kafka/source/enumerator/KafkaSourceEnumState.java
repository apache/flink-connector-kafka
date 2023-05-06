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

import org.apache.flink.annotation.Internal;

import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** The state of Kafka source enumerator. */
@Internal
public class KafkaSourceEnumState {
    /** Partitions with status: ASSIGNED or UNASSIGNED_INITIAL. */
    private final Set<TopicPartitionAndAssignmentStatus> partitions;
    /**
     * this flag will be marked as true if inital partitions are discovered after enumerator starts.
     */
    private final boolean initialDiscoveryFinished;

    public KafkaSourceEnumState(
            Set<TopicPartitionAndAssignmentStatus> partitions, boolean initialDiscoveryFinished) {
        this.partitions = partitions;
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    KafkaSourceEnumState(
            Set<TopicPartition> assignPartitions,
            Set<TopicPartition> unassignedInitialPartitions,
            boolean initialDiscoveryFinished) {
        this.partitions = new HashSet<>();
        partitions.addAll(
                assignPartitions.stream()
                        .map(
                                topicPartition ->
                                        new TopicPartitionAndAssignmentStatus(
                                                topicPartition, AssignmentStatus.ASSIGNED))
                        .collect(Collectors.toSet()));
        partitions.addAll(
                unassignedInitialPartitions.stream()
                        .map(
                                topicPartition ->
                                        new TopicPartitionAndAssignmentStatus(
                                                topicPartition,
                                                AssignmentStatus.UNASSIGNED_INITIAL))
                        .collect(Collectors.toSet()));
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    public Set<TopicPartitionAndAssignmentStatus> partitions() {
        return partitions;
    }

    public Set<TopicPartition> assignedPartitions() {
        return filterPartitionsByAssignmentStatus(AssignmentStatus.ASSIGNED);
    }

    public Set<TopicPartition> unassignedInitialPartitions() {
        return filterPartitionsByAssignmentStatus(AssignmentStatus.UNASSIGNED_INITIAL);
    }

    public boolean initialDiscoveryFinished() {
        return initialDiscoveryFinished;
    }

    private Set<TopicPartition> filterPartitionsByAssignmentStatus(
            AssignmentStatus assignmentStatus) {
        return partitions.stream()
                .filter(
                        partitionWithStatus ->
                                partitionWithStatus.assignmentStatus().equals(assignmentStatus))
                .map(TopicPartitionAndAssignmentStatus::topicPartition)
                .collect(Collectors.toSet());
    }
}
