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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void testFullRecoveryBalancesReportedActiveSplitsAndSeedsNewDiscovery() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        List<DynamicKafkaSourceSplit> recovered = new ArrayList<>();
        for (int partition = 0; partition < 7; partition++) {
            recovered.add(split("active", "topic", partition));
        }
        assigner.onMetadataRefresh(
                recovered.stream()
                        .map(DynamicKafkaSourceSplit::splitId)
                        .collect(Collectors.toSet()));
        assigner.onRecoveredSplits(recovered, 3);

        Map<String, Integer> assigned = returningOwners(assigner, recovered, 3);
        assertThat(assigned).hasSize(7);
        assertThat(
                        assigned.values().stream()
                                .collect(
                                        Collectors.groupingBy(
                                                owner -> owner, Collectors.counting()))
                                .values())
                .containsExactlyInAnyOrder(3L, 2L, 2L);
        assertThat(assigner.assignSplitOwner("new-after-full-recovery", 3)).isEqualTo(1);
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

    @Test
    void testReturningSplitsUseIdleReaderWithoutMovingActiveSplits() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        Map<String, Integer> activeOwners = new HashMap<>();
        for (int partition = 0; partition < 4; partition++) {
            activeOwners.put(split("active", "topic", partition).splitId(), 0);
        }
        List<DynamicKafkaSourceSplit> returning =
                List.of(split("returning", "topic", 0), split("returning", "topic", 1));
        assigner.onRetainedSplitsReadded(returning, activeOwners, 2);

        assertThat(returningOwners(assigner, returning, 2).values()).containsOnly(1);
        assertThat(activeOwners.values()).containsOnly(0);
        assertThat(assigner.assignSplitOwner("new-after-handoff", 2)).isZero();
    }

    @Test
    void testReturningDuplicatesAndRepeatedPreparationDoNotAdvanceCursor() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        DynamicKafkaSourceSplit returning = split("returning", "topic", 0);
        Map<String, Integer> activeOwners = Map.of("active", 0);
        // Dormant inventory and an unassigned discovery must not inflate the live load or cursor.
        assigner.onMetadataRefresh(Set.of("active", returning.splitId(), "dormant", "pending"));
        assigner.onRetainedSplitsReadded(List.of(returning, returning), activeOwners, 3);
        assigner.onRetainedSplitsReadded(List.of(returning), activeOwners, 3);

        assertThat(assigner.assignSplitOwner(returning.splitId(), 3)).isEqualTo(1);
        assertThat(assigner.assignSplitOwner("new-after-handoff", 3)).isEqualTo(2);
    }

    @Test
    void testConsecutiveReturningClustersIncludeOutstandingReservations() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        DynamicKafkaSourceSplit first = split("first", "topic", 0);
        DynamicKafkaSourceSplit second = split("second", "topic", 0);
        Map<String, Integer> activeOwners = Map.of("active", 0);
        assigner.onRetainedSplitsReadded(List.of(first), activeOwners, 3);
        // The first cluster has not consumed its reservation through assignSplitOwner yet.
        assigner.onRetainedSplitsReadded(List.of(second), activeOwners, 3);

        assertThat(assigner.assignSplitOwner(second.splitId(), 3)).isEqualTo(2);
        assertThat(assigner.assignSplitOwner(first.splitId(), 3)).isEqualTo(1);
        assertThat(assigner.assignSplitOwner("new-after-handoffs", 3)).isZero();
    }

    @Test
    void testMetadataRefreshDiscardsUnconsumedReservationForRemovedCluster() {
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();
        DynamicKafkaSourceSplit removed = split("removed", "topic", 0);
        DynamicKafkaSourceSplit returning = split("returning", "topic", 0);
        Map<String, Integer> activeOwners = Map.of("active", 0);
        assigner.onRetainedSplitsReadded(List.of(removed), activeOwners, 3);
        assigner.onMetadataRefresh(activeOwners.keySet());

        assigner.onRetainedSplitsReadded(List.of(returning), activeOwners, 3);

        assertThat(assigner.assignSplitOwner(returning.splitId(), 3)).isEqualTo(1);
        assertThat(assigner.assignSplitOwner("new-after-handoff", 3)).isEqualTo(2);
    }

    @Test
    void testReturningAssignmentRejectsConflictingOrInvalidActiveOwnership() {
        DynamicKafkaSourceSplit returning = split("returning", "topic", 0);
        GlobalSplitOwnerAssigner assigner = new GlobalSplitOwnerAssigner();

        assertThatThrownBy(
                        () ->
                                assigner.onRetainedSplitsReadded(
                                        List.of(returning), Map.of(returning.splitId(), 0), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already has an active owner");
        assertThatThrownBy(
                        () ->
                                assigner.onRetainedSplitsReadded(
                                        List.of(returning), Map.of("active", 2), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid active reader");
        assertThatThrownBy(
                        () ->
                                assigner.onRetainedSplitsReadded(
                                        List.of(returning), Collections.emptyMap(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReturningPlacementPropertiesAcrossSkewedInventories() {
        Random random = new Random(279L);
        for (int parallelism = 1; parallelism <= 8; parallelism++) {
            for (int activeCount = 0; activeCount <= 17; activeCount++) {
                for (int returningCount : new int[] {0, 1, 2, 5, 9}) {
                    for (boolean skewed : new boolean[] {false, true}) {
                        Map<String, Integer> activeOwners = new LinkedHashMap<>();
                        int[] initialLoads = new int[parallelism];
                        for (int active = 0; active < activeCount; active++) {
                            int owner = skewed ? 0 : active % parallelism;
                            activeOwners.put("active-" + active, owner);
                            initialLoads[owner]++;
                        }
                        List<DynamicKafkaSourceSplit> returning = new ArrayList<>();
                        for (int partition = 0; partition < returningCount; partition++) {
                            returning.add(split("returning", "topic", partition));
                        }
                        GlobalSplitOwnerAssigner first = new GlobalSplitOwnerAssigner();
                        GlobalSplitOwnerAssigner shuffled = new GlobalSplitOwnerAssigner();
                        Set<String> noisyInventory = new HashSet<>(activeOwners.keySet());
                        noisyInventory.add("dormant");
                        noisyInventory.add("unassigned");
                        first.onMetadataRefresh(noisyInventory);
                        shuffled.onMetadataRefresh(noisyInventory);
                        first.onRetainedSplitsReadded(returning, activeOwners, parallelism);
                        List<DynamicKafkaSourceSplit> shuffledReturning =
                                new ArrayList<>(returning);
                        Collections.shuffle(shuffledReturning, random);
                        List<String> shuffledActive = new ArrayList<>(activeOwners.keySet());
                        Collections.shuffle(shuffledActive, random);
                        Map<String, Integer> shuffledOwners = new LinkedHashMap<>();
                        shuffledActive.forEach(id -> shuffledOwners.put(id, activeOwners.get(id)));
                        shuffled.onRetainedSplitsReadded(
                                shuffledReturning, shuffledOwners, parallelism);
                        Map<String, Integer> placed =
                                returningOwners(first, returning, parallelism);
                        assertThat(returningOwners(shuffled, shuffledReturning, parallelism))
                                .as("placement must be independent of input and assignment order")
                                .isEqualTo(placed);
                        assertThat(placed).hasSize(returningCount);

                        int[] finalLoads = Arrays.copyOf(initialLoads, parallelism);
                        for (int owner : placed.values()) {
                            assertThat(owner).isBetween(0, parallelism - 1);
                            finalLoads[owner]++;
                        }
                        int minimum = Arrays.stream(finalLoads).min().orElseThrow();
                        int maximum = Arrays.stream(finalLoads).max().orElseThrow();
                        int initialMaximum = Arrays.stream(initialLoads).max().orElseThrow();
                        assertThat(Arrays.stream(finalLoads).sum())
                                .isEqualTo(activeCount + returningCount);
                        assertThat(maximum)
                                .as("minimum achievable peak without moving existing active splits")
                                .isEqualTo(
                                        Math.max(
                                                initialMaximum,
                                                (activeCount + returningCount + parallelism - 1)
                                                        / parallelism));
                        for (int reader = 0; reader < parallelism; reader++) {
                            if (finalLoads[reader] > minimum + 1) {
                                assertThat(finalLoads[reader])
                                        .as("returning work must not increase an overloaded reader")
                                        .isEqualTo(initialLoads[reader]);
                            }
                        }
                        assertThat(first.assignSplitOwner("new-after-handoff", parallelism))
                                .as("ordinary discovery continues using the active-count cursor")
                                .isEqualTo((activeCount + returningCount) % parallelism);
                    }
                }
            }
        }
    }

    private static Map<String, Integer> returningOwners(
            GlobalSplitOwnerAssigner assigner,
            List<DynamicKafkaSourceSplit> returning,
            int parallelism) {
        Map<String, Integer> owners = new TreeMap<>();
        returning.forEach(
                split ->
                        assertThat(
                                        owners.put(
                                                split.splitId(),
                                                assigner.assignSplitOwner(
                                                        split.splitId(), parallelism)))
                                .isNull());
        return owners;
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
