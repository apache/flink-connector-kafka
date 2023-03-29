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
import org.apache.flink.connector.kafka.source.TopicPartitionWithAssignStatus;

import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** The state of Kafka source enumerator. */
@Internal
public class KafkaSourceEnumState {
    /** Partitions with status: ASSIGNED or UNASSIGNED_INITIAL. */
    private final Set<TopicPartitionWithAssignStatus> partitions;
    /**
     * this flag will be marked as true if inital partitions are discovered after enumerator starts.
     */
    private final boolean initialDiscoveryFinished;

    KafkaSourceEnumState(
            Set<TopicPartition> assignPartitions,
            Set<TopicPartition> unAssignInitialPartitions,
            boolean initialDiscoveryFinished) {
        this.partitions = new HashSet<>();
        partitions.addAll(
                assignPartitions.stream()
                        .map(
                                topicPartition ->
                                        new TopicPartitionWithAssignStatus(
                                                topicPartition,
                                                TopicPartitionWithAssignStatus.ASSIGNED))
                        .collect(Collectors.toSet()));
        partitions.addAll(
                unAssignInitialPartitions.stream()
                        .map(
                                topicPartition ->
                                        new TopicPartitionWithAssignStatus(
                                                topicPartition,
                                                TopicPartitionWithAssignStatus.UNASSIGNED_INITIAL))
                        .collect(Collectors.toSet()));
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    public Set<TopicPartitionWithAssignStatus> partitions() {
        return partitions;
    }

    public Set<TopicPartition> assignedPartitions() {
        HashSet<TopicPartition> assignedPartitions = new HashSet<TopicPartition>();
        assignedPartitions.addAll(
                partitions.stream()
                        .filter(
                                partitionWithStatus ->
                                        partitionWithStatus.assignStatus()
                                                == TopicPartitionWithAssignStatus.ASSIGNED)
                        .map(TopicPartitionWithAssignStatus::topicPartition)
                        .collect(Collectors.toSet()));
        return assignedPartitions;
    }

    public Set<TopicPartition> unassignedInitialPartitons() {
        HashSet<TopicPartition> unassignedInitialPartitons = new HashSet<TopicPartition>();
        unassignedInitialPartitons.addAll(
                partitions.stream()
                        .filter(
                                partitionWithStatus ->
                                        partitionWithStatus.assignStatus()
                                                == TopicPartitionWithAssignStatus
                                                        .UNASSIGNED_INITIAL)
                        .map(TopicPartitionWithAssignStatus::topicPartition)
                        .collect(Collectors.toSet()));
        return unassignedInitialPartitons;
    }

    public boolean initialDiscoveryFinished() {
        return initialDiscoveryFinished;
    }
}
