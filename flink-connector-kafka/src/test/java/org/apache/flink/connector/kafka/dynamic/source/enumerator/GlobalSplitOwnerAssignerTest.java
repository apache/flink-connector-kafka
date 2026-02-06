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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GlobalSplitOwnerAssigner}. */
class GlobalSplitOwnerAssignerTest {

    @Test
    void testRoundRobinAssignmentAcrossClustersWithMetadataChanges() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();

        Set<String> activeSplitIds =
                new HashSet<>(
                        Arrays.asList(
                                split("cluster-a", "topic-a", 0).splitId(),
                                split("cluster-a", "topic-a", 1).splitId(),
                                split("cluster-b", "topic-a", 0).splitId(),
                                split("cluster-b", "topic-a", 1).splitId()));
        assigner.onMetadataRefresh(activeSplitIds);

        assertThat(assigner.assignSplitOwner(split("cluster-a", "topic-a", 2).splitId(), 3))
                .as("new split after 4 active splits should use 4 %% 3")
                .isEqualTo(1);
        assertThat(assigner.assignSplitOwner(split("cluster-b", "topic-a", 2).splitId(), 3))
                .as("next split should continue global round-robin order")
                .isEqualTo(2);

        Set<String> updatedSplitIds =
                new HashSet<>(
                        Arrays.asList(
                                split("cluster-a", "topic-a", 0).splitId(),
                                split("cluster-a", "topic-a", 1).splitId(),
                                split("cluster-c", "topic-z", 0).splitId()));
        assigner.onMetadataRefresh(updatedSplitIds);

        assertThat(assigner.assignSplitOwner(split("cluster-c", "topic-z", 1).splitId(), 3))
                .as("metadata refresh should reseed round-robin by current active split count")
                .isEqualTo(0);
    }

    @Test
    void testFailureRecoveryPrefersSplitBackOwner() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();

        Set<String> activeSplitIds =
                new HashSet<>(
                        Arrays.asList(
                                split("cluster-a", "topic-a", 0).splitId(),
                                split("cluster-a", "topic-a", 1).splitId(),
                                split("cluster-b", "topic-a", 0).splitId(),
                                split("cluster-b", "topic-a", 1).splitId()));
        assigner.onMetadataRefresh(activeSplitIds);

        DynamicKafkaSourceSplit returnedSplit = split("cluster-b", "topic-a", 2);
        assigner.onSplitsBack(Collections.singletonList(returnedSplit), 2);

        assertThat(assigner.assignSplitOwner(returnedSplit.splitId(), 4))
                .as("split-back should preserve owner affinity when subtask id is valid")
                .isEqualTo(2);
    }

    @Test
    void testRepartitionFallsBackToRoundRobinWhenSplitBackOwnerOutOfRange() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();

        Set<String> activeSplitIds =
                new HashSet<>(
                        Arrays.asList(
                                split("cluster-a", "topic-a", 0).splitId(),
                                split("cluster-a", "topic-a", 1).splitId(),
                                split("cluster-a", "topic-a", 2).splitId(),
                                split("cluster-b", "topic-b", 0).splitId(),
                                split("cluster-b", "topic-b", 1).splitId()));
        assigner.onMetadataRefresh(activeSplitIds);

        DynamicKafkaSourceSplit returnedSplit = split("cluster-b", "topic-b", 2);
        assigner.onSplitsBack(Collections.singletonList(returnedSplit), 4);

        assertThat(assigner.assignSplitOwner(returnedSplit.splitId(), 3))
                .as("after downscale, invalid split-back owner should fallback to RR")
                .isEqualTo(0);

        Set<String> restoredSplitIds = new HashSet<>(activeSplitIds);
        restoredSplitIds.add(returnedSplit.splitId());
        assigner.onMetadataRefresh(restoredSplitIds);

        assertThat(assigner.assignSplitOwner(split("cluster-c", "topic-c", 0).splitId(), 2))
                .as("after restore/repartition, RR should seed from restored active split count")
                .isEqualTo(0);
    }

    @Test
    void testForwardLookingBalanceStrategy() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        final int parallelism = 5;

        // Step 1: verify baseline RR shape with 13 discovered splits over 5 readers.
        // This yields a near-balanced 3,3,3,2,2 distribution.
        Map<Integer, List<String>> assignedBeforeShrink = initAssignments(parallelism);
        Map<Integer, Integer> countsBeforeShrink = counts(assignedBeforeShrink);
        assertThat(countsBeforeShrink).containsEntry(0, 3).containsEntry(1, 3).containsEntry(2, 3);
        assertThat(countsBeforeShrink).containsEntry(3, 2).containsEntry(4, 2);

        // Step 2: emulate metadata shrink by removing 3 active splits.
        // We intentionally do NOT "move" existing assignments, mirroring forward-looking behavior.
        Set<String> activeAfterShrink =
                assignedBeforeShrink.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toCollection(HashSet::new));
        activeAfterShrink.remove(assignedBeforeShrink.get(2).get(2));
        activeAfterShrink.remove(assignedBeforeShrink.get(3).get(1));
        activeAfterShrink.remove(assignedBeforeShrink.get(4).get(1));
        assigner.onMetadataRefresh(activeAfterShrink);

        Map<Integer, List<String>> activeAssignmentsAfterShrink = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<String>> entry : assignedBeforeShrink.entrySet()) {
            activeAssignmentsAfterShrink.put(
                    entry.getKey(),
                    entry.getValue().stream()
                            .filter(activeAfterShrink::contains)
                            .collect(Collectors.toList()));
        }
        Map<Integer, Integer> countsAfterShrink = counts(activeAssignmentsAfterShrink);
        assertThat(countsAfterShrink)
                .containsEntry(0, 3)
                .containsEntry(1, 3)
                .containsEntry(2, 2)
                .containsEntry(3, 1)
                .containsEntry(4, 1);

        // Step 3: new assignments start from activeCount % parallelism after shrink.
        // This proves the strategy reseeds from current active inventory rather than preserving an
        // old "next owner" cursor from before shrink.
        int ownerAfterShrink = assigner.assignSplitOwner("split-after-shrink-0", parallelism);
        int nextOwnerAfterShrink = assigner.assignSplitOwner("split-after-shrink-1", parallelism);
        assertThat(ownerAfterShrink).isEqualTo(0);
        assertThat(nextOwnerAfterShrink).isEqualTo(1);

        // If nothing were removed, next owner after 13 initial splits would have been 13%5=3.
        assertThat(13 % parallelism).isEqualTo(3);
    }

    private static Map<Integer, List<String>> initAssignments(int parallelism) {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        Map<Integer, List<String>> assignments = new LinkedHashMap<>();
        for (int i = 0; i < parallelism; i++) {
            assignments.put(i, new ArrayList<>());
        }
        for (int i = 0; i < 13; i++) {
            String splitId = "split-" + i;
            int owner = assigner.assignSplitOwner(splitId, parallelism);
            assignments.get(owner).add(splitId);
        }
        return assignments;
    }

    private static Map<Integer, Integer> counts(Map<Integer, List<String>> assignments) {
        Map<Integer, Integer> counts = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<String>> entry : assignments.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().size());
        }
        return counts;
    }

    private static DynamicKafkaSourceSplit split(String clusterId, String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                clusterId, new KafkaPartitionSplit(new TopicPartition(topic, partition), 0L));
    }
}
