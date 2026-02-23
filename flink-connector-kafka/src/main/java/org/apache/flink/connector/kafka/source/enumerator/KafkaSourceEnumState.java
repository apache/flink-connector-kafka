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
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** The state of Kafka source enumerator. */
@Internal
public class KafkaSourceEnumState {
    /** Splits with status: ASSIGNED or UNASSIGNED_INITIAL. */
    private final Set<SplitAndAssignmentStatus> splits;

    /**
     * this flag will be marked as true if initial partitions are discovered after enumerator
     * starts.
     */
    private final boolean initialDiscoveryFinished;

    public KafkaSourceEnumState(
            Set<SplitAndAssignmentStatus> splits, boolean initialDiscoveryFinished) {
        this.splits = splits;
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    public KafkaSourceEnumState(
            Collection<KafkaPartitionSplit> assignedSplits,
            Collection<KafkaPartitionSplit> unassignedSplits,
            boolean initialDiscoveryFinished) {
        this.splits = new HashSet<>();
        splits.addAll(
                assignedSplits.stream()
                        .map(
                                topicPartition ->
                                        new SplitAndAssignmentStatus(
                                                topicPartition, AssignmentStatus.ASSIGNED))
                        .collect(Collectors.toSet()));
        splits.addAll(
                unassignedSplits.stream()
                        .map(
                                topicPartition ->
                                        new SplitAndAssignmentStatus(
                                                topicPartition, AssignmentStatus.UNASSIGNED))
                        .collect(Collectors.toSet()));
        this.initialDiscoveryFinished = initialDiscoveryFinished;
    }

    public Set<SplitAndAssignmentStatus> splits() {
        return splits;
    }

    public Collection<KafkaPartitionSplit> assignedSplits() {
        return filterByAssignmentStatus(AssignmentStatus.ASSIGNED);
    }

    public Collection<KafkaPartitionSplit> unassignedSplits() {
        return filterByAssignmentStatus(AssignmentStatus.UNASSIGNED);
    }

    public boolean initialDiscoveryFinished() {
        return initialDiscoveryFinished;
    }

    private Collection<KafkaPartitionSplit> filterByAssignmentStatus(
            AssignmentStatus assignmentStatus) {
        return splits.stream()
                .filter(split -> split.assignmentStatus().equals(assignmentStatus))
                .map(SplitAndAssignmentStatus::split)
                .collect(Collectors.toList());
    }
}
