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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.RequestRetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.RetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.SplitAndAssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public protocol invariants for moving only dormant retained partitions. */
class DynamicKafkaSourceRetainedHandoffTest {

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testDisabledRetentionRestoreUsesInitializerAndKeepsActiveProgress(
            boolean global, boolean boundedCompleted) throws Throwable {
        String retainedCluster = "retained";
        String activeCluster = "active";
        TopicPartition partition = new TopicPartition("topic", 0);
        long deadline = System.currentTimeMillis() + 60_000;
        KafkaPartitionSplit oldRetained =
                new KafkaPartitionSplit(
                        partition,
                        150,
                        boundedCompleted ? 200 : KafkaPartitionSplit.NO_STOPPING_OFFSET);
        DynamicKafkaSourceSplit activeProgress =
                new DynamicKafkaSourceSplit(activeCluster, new KafkaPartitionSplit(partition, 40));
        DynamicKafkaSourceEnumState checkpoint =
                new DynamicKafkaSourceEnumState(
                        streams(Set.of(activeCluster)),
                        Map.of(
                                activeCluster,
                                new KafkaSourceEnumState(
                                        List.of(new KafkaPartitionSplit(partition, 10)),
                                        List.of(),
                                        true)),
                        Map.of(
                                retainedCluster,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(
                                                List.of(oldRetained), List.of(), true),
                                        deadline)),
                        Map.of());
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Set<KafkaStream> metadataStreams =
                Set.of(
                        new KafkaStream(
                                "stream",
                                Map.of(
                                        activeCluster,
                                                new ClusterMetadata(
                                                        Set.of("topic"), clusterProperties),
                                        retainedCluster,
                                                new ClusterMetadata(
                                                        Set.of("topic"),
                                                        clusterProperties,
                                                        OffsetsInitializer.offsets(
                                                                Map.of(partition, 100L)),
                                                        null))));
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(metadataStreams);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, checkpoint, global, true, 0)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(activeProgress));
            register(
                    context,
                    enumerator,
                    1,
                    boundedCompleted
                            ? List.of()
                            : List.of(
                                    new DynamicKafkaSourceSplit(
                                            retainedCluster, oldRetained, deadline)));
            context.runPeriodicCallable(0);
            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .containsExactlyInAnyOrder(
                            activeProgress,
                            new DynamicKafkaSourceSplit(
                                    retainedCluster, new KafkaPartitionSplit(partition, 100)));
            assertThat(enumerator.snapshotState(10).getRetainedClusterEnumeratorStates()).isEmpty();
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testV4LegacyDeadlineNormalizationSurvivesAnotherPendingCheckpoint(
            boolean returning, boolean global) throws Throwable {
        DynamicKafkaSourceEnumState removedCheckpoint;
        Map<Integer, List<DynamicKafkaSourceSplit>> oldReaderReports = new TreeMap<>();
        Set<String> clusters;
        try (Harness previous = new Harness(2, Set.of(), global)) {
            clusters = new HashSet<>(previous.initial.keySet());
            Map<Integer, List<DynamicKafkaSourceSplit>> assigned =
                    byReader(previous.assignmentsSince(0));
            previous.setMetadata(Set.of());
            previous.discover();
            removedCheckpoint = roundTrip(previous.enumerator.snapshotState(10));
            assigned.forEach(
                    (reader, splits) ->
                            oldReaderReports.put(
                                    reader,
                                    splits.stream()
                                            // Old readers used their own clock when handling the
                                            // metadata event, so a different deadline is valid.
                                            .map(
                                                    split ->
                                                            atOffset(split, 42)
                                                                    .retainUntil(
                                                                            removedCheckpoint
                                                                                            .getRetainedClusterEnumeratorStates()
                                                                                            .get(
                                                                                                    split
                                                                                                            .getKafkaClusterId())
                                                                                            .getRetainedUntilMs()
                                                                                    + 123))
                                            .collect(Collectors.toList())));
        }
        DynamicKafkaSourceEnumState pendingCheckpoint;
        Set<KafkaStream> currentStreams = streams(returning ? clusters : Set.of());
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(currentStreams);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, removedCheckpoint, global, false)) {
            enumerator.start();
            register(context, enumerator, 0, oldReaderReports.getOrDefault(0, List.of()));
            register(context, enumerator, 1, oldReaderReports.getOrDefault(1, List.of()));
            // All readers registered, but the first discovery callback has not run. The reader
            // operator state is empty; #295's pending map is the only record of these offsets.
            pendingCheckpoint = roundTrip(enumerator.snapshotState(11));
            assertThat(pendingCheckpoint.getPendingReportedSplitsByReader())
                    .isEqualTo(oldReaderReports);
        }
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(currentStreams);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, pendingCheckpoint, global, false)) {
            enumerator.start();
            register(context, enumerator, 0, List.of());
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);
            List<DynamicKafkaSourceSplit> recovered =
                    flatten(context.getSplitsAssignmentSequence());
            assertThat(recovered)
                    .hasSize(2)
                    .allSatisfy(
                            split -> {
                                assertThat(split.getStartingOffset()).isEqualTo(42);
                                assertThat(split.isRetained()).isEqualTo(!returning);
                                if (!returning) {
                                    assertThat(split.getRetainedUntilMs())
                                            .isEqualTo(
                                                    removedCheckpoint
                                                            .getRetainedClusterEnumeratorStates()
                                                            .get(split.getKafkaClusterId())
                                                            .getRetainedUntilMs());
                                }
                            });
            assertThat(enumerator.snapshotState(12).getPendingReportedSplitsByReader()).isEmpty();
            assertThat(new DynamicKafkaSourceEnumStateSerializer().getVersion()).isEqualTo(4);
        }
    }

    @Test
    void testV4ActiveProgressWinsOverHigherRetainedShadowOnFullRestore() throws Throwable {
        DynamicKafkaSourceEnumState checkpoint;
        Map<Integer, List<DynamicKafkaSourceSplit>> reports;
        try (Harness previous = new Harness(2, Set.of())) {
            checkpoint = roundTrip(previous.enumerator.snapshotState(10));
            reports = byReader(previous.assignmentsSince(0));
        }
        reports.replaceAll(
                (reader, splits) ->
                        splits.stream()
                                .map(split -> atOffset(split, 100))
                                .collect(Collectors.toList()));
        DynamicKafkaSourceSplit active = reports.get(0).get(0);
        List<DynamicKafkaSourceSplit> other = new ArrayList<>(reports.get(1));
        other.add(atOffset(active, 150).retainUntil(System.currentTimeMillis() + 60_000));
        reports.put(1, other);
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(checkpoint.getKafkaStreams());
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, checkpoint)) {
            enumerator.start();
            register(context, enumerator, 0, reports.get(0));
            register(context, enumerator, 1, reports.get(1));
            context.runPeriodicCallable(0);
            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .hasSize(2)
                    .allSatisfy(split -> assertThat(split.getStartingOffset()).isEqualTo(100));
        }
    }

    @Test
    void testV4CompletedBoundedTransferStaysCompletedAfterFullCheckpointRestore() throws Throwable {
        DynamicKafkaSourceEnumState checkpoint;
        Map<Integer, List<DynamicKafkaSourceSplit>> checkpointReports = new TreeMap<>();
        String returning;
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner(true)) {
            returning = scenario.returning.iterator().next();
            scenario.startReadd();
            scenario.reportAll();
            scenario.completeCheckpoint(10);
            scenario.assertReturningAssigned();
            checkpoint = roundTrip(scenario.harness.enumerator.snapshotState(11));
            assertThat(scenario.harness.context.latestMetadata(0).getRetainedClusterDeadlines())
                    .doesNotContainKey(returning);
            assertThat(scenario.harness.context.latestMetadata(1).getRetainedClusterDeadlines())
                    .doesNotContainKey(returning);
            scenario.reports.forEach(
                    (reader, splits) ->
                            checkpointReports.put(
                                    reader,
                                    splits.stream()
                                            .filter(
                                                    split ->
                                                            !split.getKafkaClusterId()
                                                                    .equals(returning))
                                            .collect(Collectors.toList())));
            // A completed checkpoint after assignment must include cleanup on the former owner.
            // The new owner's bounded split starts at its stop offset and has completed as well.
            // No reader reports that partition; the coordinator keeps the ASSIGNED tombstone.
        }
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(checkpoint.getKafkaStreams());
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, checkpoint)) {
            enumerator.start();
            register(context, enumerator, 0, checkpointReports.get(0));
            register(context, enumerator, 1, checkpointReports.get(1));
            context.runPeriodicCallable(0);
            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .noneMatch(split -> split.getKafkaClusterId().equals(returning));
            assertThat(
                            enumerator
                                    .snapshotState(12)
                                    .getClusterEnumeratorStates()
                                    .get(returning)
                                    .assignedSplits())
                    .singleElement()
                    .satisfies(
                            split -> {
                                assertThat(split.getStartingOffset()).isEqualTo(20);
                                assertThat(split.getStoppingOffset()).hasValue(20L);
                            });
        }
    }

    @Test
    void testExpiryClearsReaderRetentionWhenAllClustersAreInactive() throws Throwable {
        String cluster = "inactive";
        long initialDeadline = System.currentTimeMillis() + 60_000;
        AtomicLong deadline = new AtomicLong(initialDeadline);
        KafkaPartitionSplit partition =
                new KafkaPartitionSplit(new TopicPartition("topic", 0), 123);
        DynamicKafkaSourceEnumState.RetainedClusterState retained =
                new DynamicKafkaSourceEnumState.RetainedClusterState(
                        new KafkaSourceEnumState(List.of(partition), List.of(), true),
                        initialDeadline) {
                    @Override
                    public long getRetainedUntilMs() {
                        return deadline.get();
                    }
                };
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        Set.of(), Map.of(), Map.of(cluster, retained), Map.of());
        try (RecordingContext context = new RecordingContext(1);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(Set.of());
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state)) {
            enumerator.start();
            register(
                    context,
                    enumerator,
                    0,
                    List.of(new DynamicKafkaSourceSplit(cluster, partition, initialDeadline)));
            context.runPeriodicCallable(0);
            assertThat(context.latestMetadata(0).getRetainedClusterDeadlines())
                    .containsEntry(cluster, initialDeadline);

            deadline.set(1L);
            assertThat(enumerator.snapshotState(10).getRetainedClusterEnumeratorStates()).isEmpty();
            assertThat(context.latestMetadata(0).getRetainedClusterDeadlines()).isEmpty();
            int events = context.getSentSourceEvent().get(0).size();
            enumerator.snapshotState(11);
            assertThat(context.getSentSourceEvent().get(0)).hasSize(events);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {256, 2048})
    void testRetainedRecoveryVisitsInventoryLinearly(int partitions) throws Throwable {
        String cluster = "large-retained";
        long deadline = System.currentTimeMillis() + 60_000;
        Set<SplitAndAssignmentStatus> splits = new HashSet<>();
        List<DynamicKafkaSourceSplit> reports = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            KafkaPartitionSplit split =
                    new KafkaPartitionSplit(
                            new TopicPartition("topic", partition), partition + 100);
            splits.add(new SplitAndAssignmentStatus(split, AssignmentStatus.ASSIGNED));
            reports.add(new DynamicKafkaSourceSplit(cluster, split, deadline));
        }
        CountingSplitSet inventory = new CountingSplitSet(splits);
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        Set.of(),
                        Map.of(),
                        Map.of(
                                cluster,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(inventory, true), deadline)),
                        Map.of());
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(Set.of());
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state)) {
            enumerator.start();
            register(context, enumerator, 0, reports);
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);

            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .containsExactlyInAnyOrderElementsOf(reports);
            assertThat(inventory.visits)
                    .as("retained recovery must not rescan the inventory for each partition")
                    .isLessThanOrEqualTo(3L * partitions);
        }
    }

    @Test
    void testFullRestoreReusesRetainedOffsetsBeforeLastReaderRequestsMetadata() throws Throwable {
        String cluster = "restored";
        long deadline = System.currentTimeMillis() + 60_000;
        DynamicKafkaSourceSplit first =
                new DynamicKafkaSourceSplit(
                        cluster,
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 100),
                        deadline);
        DynamicKafkaSourceSplit last =
                new DynamicKafkaSourceSplit(
                        cluster,
                        new KafkaPartitionSplit(new TopicPartition("topic", 1), 200),
                        deadline);
        DynamicKafkaSourceEnumState checkpoint =
                new DynamicKafkaSourceEnumState(
                        Set.of(),
                        Map.of(),
                        Map.of(
                                cluster,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(
                                                List.of(
                                                        first.getKafkaPartitionSplit(),
                                                        last.getKafkaPartitionSplit()),
                                                List.of(),
                                                true),
                                        deadline)),
                        Map.of());
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, checkpoint)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(first));
            enumerator.handleSourceEvent(0, new GetMetadataUpdateEvent());
            context.runPeriodicCallable(0);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // The last reader has not requested metadata. Its checkpoint report completes
            // recovery; the common restored checkpoint already fences all previous attempts.
            register(context, enumerator, 1, List.of(last));
            List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments =
                    context.getSplitsAssignmentSequence();
            assertThat(flatten(assignments))
                    .containsExactlyInAnyOrder(first.clearRetention(), last.clearRetention());
            assertThat(byReader(assignments).values())
                    .allSatisfy(splits -> assertThat(splits).hasSize(1));
            assertThat(owners(assignments)).hasSize(2);
            for (int reader = 0; reader < 2; reader++) {
                assertThat(context.latestMetadata(reader).getRetainedClusterDeadlines())
                        .doesNotContainKey(cluster);
                assertThat(context.getSentSourceEvent().get(reader))
                        .noneMatch(RequestRetainedSplitOffsetsEvent.class::isInstance);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFullRestoreRequiresReaderOffsetOnlyForAssignedRetainedInventory(boolean assigned)
            throws Throwable {
        String cluster = "returning";
        KafkaPartitionSplit known = new KafkaPartitionSplit(new TopicPartition("topic", 0), 10);
        long deadline = System.currentTimeMillis() + 60_000;
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        Set.of(),
                        Map.of(),
                        Map.of(
                                cluster,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(
                                                assigned ? List.of(known) : List.of(),
                                                assigned ? List.of() : List.of(known),
                                                true),
                                        deadline)),
                        Map.of());
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state)) {
            enumerator.start();
            register(context, enumerator, 0, List.of());
            register(context, enumerator, 1, List.of());
            if (assigned) {
                assertThatThrownBy(() -> context.runPeriodicCallable(0))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Missing restored reader offset");
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            } else {
                context.runPeriodicCallable(0);
                assertThat(flatten(context.getSplitsAssignmentSequence()))
                        .containsExactly(new DynamicKafkaSourceSplit(cluster, known));
                assertThat(context.getSentSourceEvent().values().stream().flatMap(List::stream))
                        .noneMatch(RequestRetainedSplitOffsetsEvent.class::isInstance);
            }
        }
    }

    @Test
    void testLegacyActiveRetainedCopyRequiresKnownAssignedInventory() throws Throwable {
        String cluster = "legacy";
        DynamicKafkaSourceSplit progress =
                new DynamicKafkaSourceSplit(
                        cluster,
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 123),
                        System.currentTimeMillis() + 60_000);
        DynamicKafkaSourceEnumState state =
                version4(
                        new DynamicKafkaSourceEnumState(
                                streams(Set.of(cluster)),
                                Map.of(
                                        cluster,
                                        new KafkaSourceEnumState(
                                                List.of(
                                                        new KafkaPartitionSplit(
                                                                new TopicPartition("topic", 0),
                                                                10)),
                                                List.of(),
                                                true))));
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(progress));
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);

            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .containsExactly(progress.clearRetention());
        }
    }

    @Test
    void testLegacyShadowWithoutAssignedInventoryUsesFreshInitializer() throws Throwable {
        String cluster = "legacy";
        DynamicKafkaSourceSplit shadow =
                new DynamicKafkaSourceSplit(
                        cluster,
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 123),
                        System.currentTimeMillis() + 60_000);
        DynamicKafkaSourceEnumState state =
                version4(
                        new DynamicKafkaSourceEnumState(
                                streams(Set.of(cluster)),
                                Map.of(
                                        cluster,
                                        new KafkaSourceEnumState(List.of(), List.of(), true))));
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state, true, true)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(shadow));
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);

            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .singleElement()
                    .satisfies(
                            split -> {
                                assertThat(split.splitId()).isEqualTo(shadow.splitId());
                                assertThat(split.isRetained()).isFalse();
                                assertThat(split.getStartingOffset())
                                        .isEqualTo(KafkaPartitionSplit.EARLIEST_OFFSET);
                            });
        }
    }

    @Test
    void testVersion4CompatibilityAcceptsSyntheticRetainedOnlyActiveInventory() throws Throwable {
        String cluster = "modern";
        DynamicKafkaSourceSplit shadow =
                new DynamicKafkaSourceSplit(
                        cluster,
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 123),
                        System.currentTimeMillis() + 60_000);
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        streams(Set.of(cluster)),
                        Map.of(
                                cluster,
                                new KafkaSourceEnumState(
                                        List.of(
                                                new KafkaPartitionSplit(
                                                        new TopicPartition("topic", 0), 10)),
                                        List.of(),
                                        true)));
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(shadow));
            register(context, enumerator, 1, List.of());

            // Intentional V4 compatibility policy change: this manually constructed state is
            // indistinguishable from older checkpoints. It is not evidence of a reachable modern
            // completed checkpoint with a missing active owner's state.
            context.runPeriodicCallable(0);
            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .containsExactly(shadow.clearRetention());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPendingReportsSurviveCheckpointBeforeFirstDiscovery(boolean legacy) throws Throwable {
        String cluster = "pending";
        DynamicKafkaSourceSplit active =
                new DynamicKafkaSourceSplit(
                        cluster, new KafkaPartitionSplit(new TopicPartition("topic", 0), 123));
        DynamicKafkaSourceSplit report =
                legacy ? active.retainUntil(System.currentTimeMillis() + 60_000) : active;
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        streams(Set.of(cluster)),
                        Map.of(
                                cluster,
                                new KafkaSourceEnumState(
                                        List.of(
                                                new KafkaPartitionSplit(
                                                        new TopicPartition("topic", 0), 10)),
                                        List.of(),
                                        true)));
        if (legacy) {
            state = version4(state);
        }
        DynamicKafkaSourceEnumState checkpoint;
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(report));
            register(context, enumerator, 1, List.of());
            checkpoint = roundTrip(enumerator.snapshotState(10));
            assertThat(checkpoint.getPendingReportedSplitsByReader().get(0))
                    .containsExactly(report);
        }
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, checkpoint)) {
            enumerator.start();
            register(context, enumerator, 0, List.of());
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);

            assertThat(flatten(context.getSplitsAssignmentSequence())).containsExactly(active);
            assertThat(enumerator.snapshotState(11).getPendingReportedSplitsByReader()).isEmpty();
        }
    }

    @Test
    void testExpiredRestoredRetentionUsesInitializerAndCannotResurrectShadow() throws Throwable {
        String cluster = "expired";
        long expiredDeadline = System.currentTimeMillis() - 1;
        DynamicKafkaSourceSplit shadow =
                new DynamicKafkaSourceSplit(
                        cluster,
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 123),
                        expiredDeadline);
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        Set.of(),
                        Map.of(),
                        Map.of(
                                cluster,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(
                                                List.of(shadow.getKafkaPartitionSplit()),
                                                List.of(),
                                                true),
                                        expiredDeadline)),
                        Map.of());
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(streams(Set.of(cluster)));
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state, true, true)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(shadow));
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);

            assertThat(flatten(context.getSplitsAssignmentSequence()))
                    .singleElement()
                    .satisfies(
                            split ->
                                    assertThat(split.getStartingOffset())
                                            .isEqualTo(KafkaPartitionSplit.EARLIEST_OFFSET));
            assertThat(enumerator.snapshotState(10).getRetainedClusterEnumeratorStates()).isEmpty();
        }
    }

    @Test
    void testDefaultPerClusterHandoffUsesNormalOwnerAndCheckpointFence() throws Throwable {
        try (Harness harness = new Harness(2, Set.of(), false)) {
            Map<String, Integer> initialOwners = owners(harness.assignmentsSince(0));
            String returning = "cluster-01";
            String splitId = harness.initial.get(returning).get(0).splitId();
            harness.setMetadata(Set.of("cluster-00"));
            harness.discover();
            int beforeReadd = harness.assignmentCount();
            harness.setMetadata(harness.initial.keySet());
            harness.discover();
            long round = harness.context.latestHandoff(returning);
            for (int reader = 0; reader < 2; reader++) {
                harness.enumerator.handleSourceEvent(
                        reader,
                        new RetainedSplitOffsetsEvent(
                                round,
                                returning,
                                initialOwners.get(splitId) == reader
                                        ? Map.of(splitId, 123L)
                                        : Map.of()));
            }
            assertThat(harness.assignmentsSince(beforeReadd)).isEmpty();
            harness.enumerator.snapshotState(10);
            assertThat(harness.assignmentsSince(beforeReadd)).isEmpty();
            harness.enumerator.notifyCheckpointComplete(10);

            List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments =
                    harness.assignmentsSince(beforeReadd);
            assertThat(owners(assignments))
                    .containsExactlyEntriesOf(Map.of(splitId, initialOwners.get(splitId)));
            assertThat(flatten(assignments))
                    .singleElement()
                    .satisfies(split -> assertThat(split.getStartingOffset()).isEqualTo(123));
        }
    }

    @Test
    void testReturningSplitUsesIdleReaderAfterCompletedCheckpoint() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.assertNoReturningAssignments();
            scenario.harness.enumerator.snapshotState(10);
            scenario.assertNoReturningAssignments();

            scenario.harness.enumerator.notifyCheckpointComplete(10);

            scenario.assertReturningAssigned();
            assertThat(owners(scenario.assignments()).values()).containsOnly(1);
            assertThat(flatten(scenario.assignments()))
                    .noneMatch(split -> scenario.surviving.contains(split.getKafkaClusterId()));
        }
    }

    @Test
    void testReturningCohortImprovesSkewWithoutMovingSurvivingSplits() throws Throwable {
        try (Scenario scenario = Scenario.fourSurvivingAndTwoReturning()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.completeCheckpoint(10);

            scenario.assertReturningAssigned();
            assertThat(owners(scenario.assignments()).values()).containsOnly(1);
            assertThat(flatten(scenario.assignments()))
                    .as("four existing splits stay on reader zero; only two return to reader one")
                    .hasSize(2)
                    .noneMatch(split -> scenario.surviving.contains(split.getKafkaClusterId()));
        }
    }

    @Test
    void testCheckpointBeforeLastReportCannotReleaseHandoff() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportReader(0);
            scenario.harness.enumerator.snapshotState(10);
            scenario.reportReader(1);

            scenario.harness.enumerator.notifyCheckpointComplete(10);
            scenario.assertNoReturningAssignments();

            scenario.completeCheckpoint(11);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testCompletedCheckpointBeforeHandoffCannotReleaseIt() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.completeCheckpoint(9);
            scenario.startReadd();
            scenario.reportAll();
            scenario.harness.enumerator.notifyCheckpointComplete(9);
            scenario.assertNoReturningAssignments();

            scenario.completeCheckpoint(10);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testAbortedCheckpointKeepsHandoffPendingAndLaterCompletionSubsumesIt() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.harness.enumerator.snapshotState(10);
            scenario.harness.enumerator.notifyCheckpointAborted(10);
            scenario.assertNoReturningAssignments();
            scenario.harness.enumerator.snapshotState(11);
            scenario.harness.enumerator.snapshotState(12);

            scenario.harness.enumerator.notifyCheckpointComplete(12);
            scenario.assertReturningAssigned();
            int assignments = scenario.harness.assignmentCount();
            scenario.harness.enumerator.notifyCheckpointComplete(11);
            assertThat(scenario.harness.assignmentCount()).isEqualTo(assignments);
        }
    }

    @Test
    void testEarlierEligibleCheckpointCompletesWhileLaterCheckpointIsPending() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.harness.enumerator.snapshotState(10);
            scenario.harness.enumerator.snapshotState(11);

            scenario.harness.enumerator.notifyCheckpointComplete(10);

            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testDuplicateAndStaleReportsCannotReplaceMissingReader() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportReader(0);
            scenario.reportReader(0);
            String cluster = scenario.returning.iterator().next();
            scenario.report(1, cluster, scenario.rounds.get(cluster) - 1, Collections.emptyMap());
            scenario.completeCheckpoint(10);
            scenario.assertNoReturningAssignments();

            scenario.reportReader(1);
            scenario.harness.enumerator.notifyCheckpointComplete(10);
            scenario.assertNoReturningAssignments();
            scenario.completeCheckpoint(11);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testDelayedReplyKeepsSameRoundAcrossMetadataRefreshes() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportReader(0);
            Map<String, Long> originalRounds = new TreeMap<>(scenario.rounds);
            int scheduledTasks = scenario.harness.context.getPeriodicCallables().size();
            for (int refresh = 0; refresh < 5; refresh++) {
                scenario.harness.discover();
                scenario.refreshRounds();
                assertThat(scenario.rounds).isEqualTo(originalRounds);
                scenario.completeCheckpoint(10 + refresh);
                scenario.assertNoReturningAssignments();
            }
            assertThat(scenario.harness.context.getPeriodicCallables()).hasSize(scheduledTasks);
            scenario.reportReader(1, originalRounds);
            scenario.harness.enumerator.notifyCheckpointComplete(14);
            scenario.assertNoReturningAssignments();
            scenario.completeCheckpoint(15);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testReaderRegistrationInvalidatesReportsAndCheckpointEligibility() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.harness.enumerator.snapshotState(10);
            Map<String, Long> previousRounds = new HashMap<>(scenario.rounds);
            scenario.harness.context.unregisterReader(1);
            scenario.harness.register(1, scenario.reports.get(1));
            scenario.refreshRounds();
            previousRounds.forEach(
                    (cluster, round) ->
                            assertThat(scenario.rounds.get(cluster)).isGreaterThan(round));
            scenario.reportAll(previousRounds);
            scenario.harness.enumerator.notifyCheckpointComplete(10);
            scenario.assertNoReturningAssignments();

            scenario.reportAll();
            scenario.completeCheckpoint(11);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testEmptySplitsBackInvalidatesHandoffBeforeReplacementRegisters() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.harness.enumerator.snapshotState(10);
            scenario.harness.context.unregisterReader(1);
            scenario.harness.enumerator.addSplitsBack(Collections.emptyList(), 1);

            scenario.harness.enumerator.notifyCheckpointComplete(10);
            scenario.assertNoReturningAssignments();
            scenario.harness.register(1, scenario.reports.get(1));
            scenario.refreshRounds();
            scenario.reportAll();
            scenario.completeCheckpoint(11);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testOldOwnerRecoveryCannotReassignHealthyOwnersActiveSplit() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.completeCheckpoint(10);
            scenario.assertReturningAssigned();
            int beforeFailure = scenario.harness.assignmentCount();

            scenario.harness.context.unregisterReader(0);
            scenario.harness.enumerator.addSplitsBack(Collections.emptyList(), 0);
            scenario.harness.register(0, scenario.reports.get(0));

            List<SplitsAssignment<DynamicKafkaSourceSplit>> recovered =
                    scenario.harness.assignmentsSince(beforeFailure);
            assertThat(flatten(recovered))
                    .noneMatch(split -> scenario.returning.contains(split.getKafkaClusterId()));
            assertThat(owners(recovered).values()).containsOnly(0);
            assertThat(flatten(recovered))
                    .filteredOn(split -> scenario.surviving.contains(split.getKafkaClusterId()))
                    .hasSize(1)
                    .allSatisfy(
                            split ->
                                    assertThat(split.getKafkaPartitionSplit().getStartingOffset())
                                            .isEqualTo(100));
        }
    }

    @Test
    void testNewOwnerRecoveryKeepsCurrentOwnershipAndCheckpointOffset() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.completeCheckpoint(10);
            scenario.assertReturningAssigned();
            DynamicKafkaSourceSplit transferred = flatten(scenario.assignments()).get(0);
            DynamicKafkaSourceSplit progressed = atOffset(transferred, 125);
            List<DynamicKafkaSourceSplit> report = new ArrayList<>(scenario.reports.get(1));
            report.add(progressed);
            scenario.completeCheckpoint(11);
            int beforeFailure = scenario.harness.assignmentCount();

            scenario.harness.context.unregisterReader(1);
            scenario.harness.enumerator.addSplitsBack(Collections.emptyList(), 1);
            scenario.harness.register(1, report);

            List<SplitsAssignment<DynamicKafkaSourceSplit>> recovered =
                    scenario.harness.assignmentsSince(beforeFailure);
            assertThat(flatten(recovered).stream().filter(split -> !split.isRetained()))
                    .containsExactly(progressed);
            assertThat(owners(recovered).get(progressed.splitId())).isEqualTo(1);
            assertThat(flatten(recovered))
                    .noneMatch(split -> scenario.surviving.contains(split.getKafkaClusterId()));
        }
    }

    @Test
    void testNewOwnerRestoresUncheckpointedHandoffAssignment() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.completeCheckpoint(10);
            DynamicKafkaSourceSplit transferred = flatten(scenario.assignments()).get(0);
            int beforeFailure = scenario.harness.assignmentCount();
            scenario.harness.context.unregisterReader(1);
            scenario.harness.enumerator.addSplitsBack(List.of(transferred), 1);
            scenario.harness.register(1, scenario.reports.get(1));

            List<SplitsAssignment<DynamicKafkaSourceSplit>> recovered =
                    scenario.harness.assignmentsSince(beforeFailure);
            assertThat(flatten(recovered).stream().filter(split -> !split.isRetained()))
                    .containsExactly(transferred);
            assertThat(owners(recovered).get(transferred.splitId())).isEqualTo(1);
        }
    }

    @Test
    void testMissingUnboundedOffsetFailsBeforeAssignmentInsteadOfUsingInitializer()
            throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            String cluster = scenario.returning.iterator().next();
            String splitId = scenario.harness.initial.get(cluster).get(0).splitId();

            assertThatThrownBy(
                            () -> {
                                scenario.report(0, cluster, scenario.rounds.get(cluster), Map.of());
                                scenario.report(1, cluster, scenario.rounds.get(cluster), Map.of());
                                scenario.completeCheckpoint(10);
                            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(splitId);
            scenario.assertNoReturningAssignments();
        }
    }

    @Test
    void testCompletedBoundedPartitionDoesNotRestartFromInitializer() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner(true)) {
            scenario.startReadd();
            String cluster = scenario.returning.iterator().next();
            scenario.report(0, cluster, scenario.rounds.get(cluster), Map.of());
            scenario.report(1, cluster, scenario.rounds.get(cluster), Map.of());
            scenario.completeCheckpoint(10);

            scenario.assertNoReturningAssignments();
            KafkaSourceEnumState state =
                    scenario.harness
                            .enumerator
                            .snapshotState(11)
                            .getClusterEnumeratorStates()
                            .get(cluster);
            assertThat(state).isNotNull();
            assertThat(state.assignedSplits())
                    .singleElement()
                    .satisfies(split -> assertThat(split.getStoppingOffset()).hasValue(20L));
        }
    }

    @Test
    void testRemovalDuringHandoffCancelsItAndLaterReturnUsesFreshReports() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.harness.enumerator.snapshotState(10);
            Map<String, Long> previousRounds = new HashMap<>(scenario.rounds);
            scenario.harness.setMetadata(scenario.surviving);
            scenario.harness.discover();
            scenario.reportAll(previousRounds);
            scenario.harness.enumerator.notifyCheckpointComplete(10);
            scenario.assertNoReturningAssignments();

            scenario.startReadd();
            previousRounds.forEach(
                    (cluster, round) ->
                            assertThat(scenario.rounds.get(cluster)).isGreaterThan(round));
            scenario.reportAll(previousRounds);
            scenario.completeCheckpoint(11);
            scenario.assertNoReturningAssignments();
            scenario.reportAll();
            scenario.completeCheckpoint(12);
            scenario.assertReturningAssigned();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPreviousRetentionEpochRequiresReturnedActiveAssignment(boolean returnAssignment)
            throws Throwable {
        try (Scenario scenario = Scenario.fourSurvivingAndTwoReturning()) {
            scenario.startReadd();
            scenario.reportAll();
            scenario.completeCheckpoint(10);
            List<DynamicKafkaSourceSplit> transferredSplits = flatten(scenario.assignments());
            assertThat(owners(scenario.assignments()).values()).containsOnly(1);
            String cluster = transferredSplits.get(0).getKafkaClusterId();
            List<DynamicKafkaSourceSplit> staleCheckpointCopies =
                    scenario.reports.get(1).stream()
                            .map(
                                    split ->
                                            scenario.returning.contains(split.getKafkaClusterId())
                                                    ? atOffset(split, 150)
                                                    : split)
                            .collect(Collectors.toList());
            DynamicKafkaSourceSplit previousCheckpointCopy =
                    staleCheckpointCopies.stream()
                            .filter(split -> split.getKafkaClusterId().equals(cluster))
                            .findFirst()
                            .orElseThrow();
            scenario.harness.setMetadata(scenario.surviving);
            scenario.harness.discover();
            long currentDeadline =
                    scenario.harness
                            .enumerator
                            .snapshotState(-1)
                            .getRetainedClusterEnumeratorStates()
                            .get(cluster)
                            .getRetainedUntilMs();
            assertThat(currentDeadline).isGreaterThan(previousCheckpointCopy.getRetainedUntilMs());
            int beforeFailure = scenario.harness.assignmentCount();

            // The checkpoint still contains an older removal epoch. Only the assignment tracker
            // can establish that this reader subsequently owned the returning active split.
            scenario.harness.context.unregisterReader(1);
            scenario.harness.enumerator.addSplitsBack(
                    returnAssignment ? transferredSplits : Collections.emptyList(), 1);
            if (!returnAssignment) {
                assertThatThrownBy(() -> scenario.harness.register(1, staleCheckpointCopies))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Missing restored offset for owned split");
                assertThat(scenario.harness.assignmentsSince(beforeFailure)).isEmpty();
                return;
            }
            scenario.harness.register(1, staleCheckpointCopies);
            List<DynamicKafkaSourceSplit> recovered =
                    flatten(scenario.harness.assignmentsSince(beforeFailure));
            assertThat(recovered)
                    .filteredOn(split -> scenario.returning.contains(split.getKafkaClusterId()))
                    .hasSize(2)
                    .allSatisfy(
                            split -> {
                                assertThat(split.isRetained()).isTrue();
                                assertThat(split.getRetainedUntilMs()).isEqualTo(currentDeadline);
                                assertThat(split.getStartingOffset()).isEqualTo(100);
                            });
            assertThat(recovered)
                    .noneMatch(split -> scenario.surviving.contains(split.getKafkaClusterId()));
        }
    }

    @Test
    void testExpiredHigherOffsetShadowCannotOverrideFreshReturnedAssignment() throws Throwable {
        String cluster = "expired";
        TopicPartition partition = new TopicPartition("topic", 0);
        long expiredDeadline = System.currentTimeMillis() - 1;
        DynamicKafkaSourceSplit shadow =
                new DynamicKafkaSourceSplit(
                        cluster, new KafkaPartitionSplit(partition, 150), expiredDeadline);
        DynamicKafkaSourceEnumState state =
                new DynamicKafkaSourceEnumState(
                        Set.of(),
                        Map.of(),
                        Map.of(
                                cluster,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(
                                                List.of(shadow.getKafkaPartitionSplit()),
                                                List.of(),
                                                true),
                                        expiredDeadline)),
                        Map.of());
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Set<KafkaStream> freshMetadata =
                Set.of(
                        new KafkaStream(
                                "stream",
                                Map.of(
                                        cluster,
                                        new ClusterMetadata(
                                                Set.of("topic"),
                                                clusterProperties,
                                                OffsetsInitializer.offsets(Map.of(partition, 100L)),
                                                null))));
        try (RecordingContext context = new RecordingContext(2);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(freshMetadata);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, metadata, state, true, true)) {
            enumerator.start();
            register(context, enumerator, 0, List.of(shadow));
            register(context, enumerator, 1, List.of());
            context.runPeriodicCallable(0);
            List<DynamicKafkaSourceSplit> fresh = flatten(context.getSplitsAssignmentSequence());
            assertThat(fresh)
                    .singleElement()
                    .satisfies(
                            split -> {
                                assertThat(split.isRetained()).isFalse();
                                assertThat(split.getStartingOffset()).isEqualTo(100);
                            });
            int owner = owners(context.getSplitsAssignmentSequence()).get(shadow.splitId());
            metadata.setKafkaStreams(Set.of());
            context.runPeriodicCallable(0);
            long currentDeadline =
                    enumerator
                            .snapshotState(-1)
                            .getRetainedClusterEnumeratorStates()
                            .get(cluster)
                            .getRetainedUntilMs();
            int beforeFailure = context.getSplitsAssignmentSequence().size();
            context.unregisterReader(owner);
            enumerator.addSplitsBack(fresh, owner);
            register(context, enumerator, owner, List.of(shadow));
            assertThat(
                            flatten(
                                    context.getSplitsAssignmentSequence()
                                            .subList(
                                                    beforeFailure,
                                                    context.getSplitsAssignmentSequence().size())))
                    .containsExactly(fresh.get(0).retainUntil(currentDeadline));
        }
    }

    @Test
    void testCollectedOffsetsStayPinnedPastRetentionDeadlineWhileCheckpointIsPending()
            throws Throwable {
        Harness harness = new Harness(4, Set.of(), true, 1000);
        List<DynamicKafkaSourceSplit> sameOwner = byReader(harness.assignmentsSince(0)).get(0);
        try (Scenario scenario =
                new Scenario(
                        harness,
                        Set.of(sameOwner.get(0).getKafkaClusterId()),
                        Set.of(sameOwner.get(1).getKafkaClusterId()))) {
            String cluster = scenario.returning.iterator().next();
            long deadline =
                    harness.enumerator
                            .snapshotState(-1)
                            .getRetainedClusterEnumeratorStates()
                            .get(cluster)
                            .getRetainedUntilMs();
            scenario.startReadd();
            scenario.reportAll();
            long handoff = scenario.rounds.get(cluster);
            long waitMillis = deadline - System.currentTimeMillis() + 25;
            if (waitMillis > 0) {
                Thread.sleep(waitMillis);
            }
            harness.discover();

            assertThat(harness.context.latestHandoff(cluster)).isEqualTo(handoff);
            assertThat(harness.enumerator.snapshotState(10).getRetainedClusterEnumeratorStates())
                    .containsKey(cluster);
            assertThat(harness.context.latestMetadata(0).getRetainedClusterDeadlines())
                    .containsEntry(cluster, deadline);
            scenario.assertNoReturningAssignments();
            harness.enumerator.notifyCheckpointComplete(10);
            scenario.assertReturningAssigned();
        }
    }

    @Test
    void testMetadataKeepsAuthoritativeRetentionUntilHandoffCompletes() throws Throwable {
        try (Scenario scenario = Scenario.twoSplitsOnOneOwner()) {
            String cluster = scenario.returning.iterator().next();
            long deadline =
                    scenario.harness
                            .enumerator
                            .snapshotState(-1)
                            .getRetainedClusterEnumeratorStates()
                            .get(cluster)
                            .getRetainedUntilMs();
            scenario.startReadd();
            scenario.reportAll();

            assertThat(scenario.harness.context.latestMetadata(0).getRetainedClusterDeadlines())
                    .containsEntry(cluster, deadline);
            scenario.completeCheckpoint(10);

            assertThat(scenario.harness.context.latestMetadata(0).getRetainedClusterDeadlines())
                    .doesNotContainKey(cluster);
            assertThat(scenario.harness.context.latestMetadata(1).getRetainedClusterDeadlines())
                    .doesNotContainKey(cluster);
        }
    }

    private static class Scenario implements AutoCloseable {
        private final Harness harness;
        private final Set<String> surviving;
        private final Set<String> returning;
        private final Map<Integer, List<DynamicKafkaSourceSplit>> reports = new TreeMap<>();
        private Map<String, Long> rounds;
        private int assignmentStart;

        private Scenario(Harness harness, Set<String> surviving, Set<String> returning)
                throws Throwable {
            this.harness = harness;
            this.surviving = surviving;
            this.returning = returning;
            harness.setMetadata(surviving);
            harness.discover();
            DynamicKafkaSourceEnumState removed = harness.enumerator.snapshotState(-1);
            byReader(harness.assignmentsSince(0))
                    .forEach(
                            (reader, splits) -> {
                                List<DynamicKafkaSourceSplit> state = new ArrayList<>();
                                for (DynamicKafkaSourceSplit split : splits) {
                                    DynamicKafkaSourceSplit progressed =
                                            atOffset(
                                                    split,
                                                    Math.min(
                                                            100,
                                                            split.getKafkaPartitionSplit()
                                                                    .getStoppingOffset()
                                                                    .orElse(Long.MAX_VALUE)));
                                    DynamicKafkaSourceEnumState.RetainedClusterState retained =
                                            removed.getRetainedClusterEnumeratorStates()
                                                    .get(split.getKafkaClusterId());
                                    state.add(
                                            retained == null
                                                    ? progressed
                                                    : progressed.retainUntil(
                                                            retained.getRetainedUntilMs()));
                                }
                                reports.put(reader, state);
                            });
        }

        private static Scenario twoSplitsOnOneOwner() throws Throwable {
            return twoSplitsOnOneOwner(false);
        }

        private static Scenario twoSplitsOnOneOwner(boolean boundedReturning) throws Throwable {
            Harness harness = new Harness(4, boundedReturning ? Set.of("cluster-02") : Set.of());
            List<DynamicKafkaSourceSplit> sameOwner = byReader(harness.assignmentsSince(0)).get(0);
            assertThat(sameOwner).hasSize(2);
            return new Scenario(
                    harness,
                    Set.of(sameOwner.get(0).getKafkaClusterId()),
                    Set.of(sameOwner.get(1).getKafkaClusterId()));
        }

        private static Scenario fourSurvivingAndTwoReturning() throws Throwable {
            Harness harness = new Harness(8, Set.of());
            Map<Integer, List<DynamicKafkaSourceSplit>> initial =
                    byReader(harness.assignmentsSince(0));
            return new Scenario(
                    harness,
                    initial.get(0).stream()
                            .map(DynamicKafkaSourceSplit::getKafkaClusterId)
                            .collect(Collectors.toSet()),
                    initial.get(1).subList(0, 2).stream()
                            .map(DynamicKafkaSourceSplit::getKafkaClusterId)
                            .collect(Collectors.toSet()));
        }

        private void startReadd() throws Throwable {
            Set<String> active = new HashSet<>(surviving);
            active.addAll(returning);
            assignmentStart = harness.assignmentCount();
            harness.setMetadata(active);
            harness.discover();
            refreshRounds();
            assertNoReturningAssignments();
        }

        private void refreshRounds() throws Exception {
            rounds = new TreeMap<>();
            for (String cluster : returning) {
                rounds.put(cluster, harness.context.latestHandoff(cluster));
            }
        }

        private void report(int reader, String cluster, long round, Map<String, Long> offsets) {
            harness.enumerator.handleSourceEvent(
                    reader, new RetainedSplitOffsetsEvent(round, cluster, offsets));
        }

        private void reportReader(int reader) {
            reportReader(reader, rounds);
        }

        private void reportReader(int reader, Map<String, Long> reportRounds) {
            reportRounds.forEach(
                    (cluster, round) ->
                            report(
                                    reader,
                                    cluster,
                                    round,
                                    reports.getOrDefault(reader, List.of()).stream()
                                            .filter(
                                                    split ->
                                                            split.getKafkaClusterId()
                                                                    .equals(cluster))
                                            .collect(
                                                    Collectors.toMap(
                                                            DynamicKafkaSourceSplit::splitId,
                                                            split ->
                                                                    split.getKafkaPartitionSplit()
                                                                            .getStartingOffset()))));
        }

        private void reportAll() {
            reportAll(rounds);
        }

        private void reportAll(Map<String, Long> reportRounds) {
            for (int reader = 0; reader < 2; reader++) {
                reportReader(reader, reportRounds);
            }
        }

        private void completeCheckpoint(long checkpoint) throws Exception {
            harness.enumerator.snapshotState(checkpoint);
            harness.enumerator.notifyCheckpointComplete(checkpoint);
        }

        private List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments() {
            return harness.assignmentsSince(assignmentStart);
        }

        private void assertNoReturningAssignments() {
            assertThat(flatten(assignments()))
                    .noneMatch(
                            split ->
                                    !split.isRetained()
                                            && returning.contains(split.getKafkaClusterId()));
        }

        private void assertReturningAssigned() {
            List<DynamicKafkaSourceSplit> expected =
                    reports.values().stream()
                            .flatMap(List::stream)
                            .filter(split -> returning.contains(split.getKafkaClusterId()))
                            .map(DynamicKafkaSourceSplit::clearRetention)
                            .collect(Collectors.toList());
            assertThat(flatten(assignments()).stream().filter(split -> !split.isRetained()))
                    .containsExactlyInAnyOrderElementsOf(expected);
            assertThat(owners(assignments())).hasSize(flatten(assignments()).size());
        }

        @Override
        public void close() throws Exception {
            harness.close();
        }
    }

    private static class Harness implements AutoCloseable {
        private final RecordingContext context = new RecordingContext(2);
        private final Map<String, List<DynamicKafkaSourceSplit>> initial = new TreeMap<>();
        private final MockKafkaMetadataService metadata;
        private final DynamicKafkaSourceEnumerator enumerator;

        private Harness(int clusterCount, Set<String> boundedClusters) throws Throwable {
            this(clusterCount, boundedClusters, true);
        }

        private Harness(int clusterCount, Set<String> boundedClusters, boolean global)
                throws Throwable {
            this(clusterCount, boundedClusters, global, 60_000);
        }

        private Harness(
                int clusterCount, Set<String> boundedClusters, boolean global, long retentionMs)
                throws Throwable {
            for (int index = 0; index < clusterCount; index++) {
                String cluster = String.format("cluster-%02d", index);
                initial.put(
                        cluster,
                        List.of(
                                new DynamicKafkaSourceSplit(
                                        cluster,
                                        new KafkaPartitionSplit(
                                                new TopicPartition("topic", 0),
                                                10,
                                                boundedClusters.contains(cluster)
                                                        ? 20
                                                        : KafkaPartitionSplit
                                                                .NO_STOPPING_OFFSET))));
            }
            Set<KafkaStream> streams = streams(initial.keySet());
            metadata = new MockKafkaMetadataService(streams);
            Map<String, KafkaSourceEnumState> states = new HashMap<>();
            initial.forEach(
                    (cluster, splits) ->
                            states.put(
                                    cluster,
                                    new KafkaSourceEnumState(
                                            splits.stream()
                                                    .map(
                                                            DynamicKafkaSourceSplit
                                                                    ::getKafkaPartitionSplit)
                                                    .collect(Collectors.toList()),
                                            Collections.emptyList(),
                                            true)));
            enumerator =
                    createEnumerator(
                            context,
                            metadata,
                            new DynamicKafkaSourceEnumState(streams, states),
                            global,
                            false,
                            retentionMs);
            enumerator.start();
            register(
                    0,
                    initial.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            register(1, Collections.emptyList());
            discover();
        }

        private void setMetadata(Set<String> clusters) {
            metadata.setKafkaStreams(streams(clusters));
        }

        private void register(int reader, List<DynamicKafkaSourceSplit> splits) {
            context.registerReader(ReaderInfo.createReaderInfo(reader, "reader-" + reader, splits));
            enumerator.addReader(reader);
        }

        private void discover() throws Throwable {
            context.runPeriodicCallable(0);
        }

        private int assignmentCount() {
            return context.getSplitsAssignmentSequence().size();
        }

        private List<SplitsAssignment<DynamicKafkaSourceSplit>> assignmentsSince(int index) {
            return new ArrayList<>(
                    context.getSplitsAssignmentSequence().subList(index, assignmentCount()));
        }

        @Override
        public void close() throws Exception {
            try {
                enumerator.close();
            } finally {
                context.close();
            }
        }
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            RecordingContext context,
            MockKafkaMetadataService metadata,
            DynamicKafkaSourceEnumState state) {
        return createEnumerator(context, metadata, state, true, false);
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            RecordingContext context,
            MockKafkaMetadataService metadata,
            DynamicKafkaSourceEnumState state,
            boolean global,
            boolean discoverPartitions) {
        return createEnumerator(context, metadata, state, global, discoverPartitions, 60_000);
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            RecordingContext context,
            MockKafkaMetadataService metadata,
            DynamicKafkaSourceEnumState state,
            boolean global,
            boolean discoverPartitions,
            long retentionMs) {
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1000");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                Long.toString(retentionMs));
        if (global) {
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), "global");
        }
        return new DynamicKafkaSourceEnumerator(
                new KafkaStreamSetSubscriber(Set.of("stream")),
                metadata,
                context,
                OffsetsInitializer.earliest(),
                new NoStoppingOffsetsInitializer(),
                properties,
                Boundedness.CONTINUOUS_UNBOUNDED,
                state,
                (enumContext, cluster, service, callback) ->
                        discoverPartitions
                                ? new PartitionDiscoveryContextProxy(
                                        cluster, service, enumContext, callback)
                                : new NoOpKafkaEnumContextProxy(
                                        cluster, service, enumContext, callback));
    }

    private static void register(
            RecordingContext context,
            DynamicKafkaSourceEnumerator enumerator,
            int reader,
            List<DynamicKafkaSourceSplit> report) {
        context.registerReader(ReaderInfo.createReaderInfo(reader, "reader-" + reader, report));
        enumerator.addReader(reader);
    }

    private static DynamicKafkaSourceEnumState roundTrip(DynamicKafkaSourceEnumState state)
            throws Exception {
        DynamicKafkaSourceEnumStateSerializer serializer =
                new DynamicKafkaSourceEnumStateSerializer();
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(state));
    }

    private static DynamicKafkaSourceEnumState version4(DynamicKafkaSourceEnumState state)
            throws Exception {
        DynamicKafkaSourceEnumStateSerializer serializer =
                new DynamicKafkaSourceEnumStateSerializer();
        byte[] bytes = serializer.serialize(state);
        return serializer.deserialize(4, bytes);
    }

    private static class CountingSplitSet extends AbstractSet<SplitAndAssignmentStatus> {
        private final Set<SplitAndAssignmentStatus> splits;
        private long visits;

        private CountingSplitSet(Set<SplitAndAssignmentStatus> splits) {
            this.splits = splits;
        }

        @Override
        public Iterator<SplitAndAssignmentStatus> iterator() {
            Iterator<SplitAndAssignmentStatus> iterator = splits.iterator();
            return new Iterator<SplitAndAssignmentStatus>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public SplitAndAssignmentStatus next() {
                    visits++;
                    return iterator.next();
                }
            };
        }

        @Override
        public int size() {
            return splits.size();
        }
    }

    private static class RecordingContext
            extends MockSplitEnumeratorContext<DynamicKafkaSourceSplit> {
        private RecordingContext(int parallelism) {
            super(parallelism);
        }

        private long latestHandoff(String cluster) throws Exception {
            List<RequestRetainedSplitOffsetsEvent> requests =
                    getSentSourceEvent().values().stream()
                            .flatMap(List::stream)
                            .filter(RequestRetainedSplitOffsetsEvent.class::isInstance)
                            .map(RequestRetainedSplitOffsetsEvent.class::cast)
                            .filter(event -> event.getKafkaClusterId().equals(cluster))
                            .collect(Collectors.toList());
            assertThat(requests).as("returning cluster must request retained offsets").isNotEmpty();
            return requests.stream()
                    .mapToLong(RequestRetainedSplitOffsetsEvent::getHandoffId)
                    .max()
                    .orElseThrow();
        }

        private MetadataUpdateEvent latestMetadata(int reader) throws Exception {
            List<SourceEvent> events = getSentSourceEvent().get(reader);
            for (int index = events.size() - 1; index >= 0; index--) {
                if (events.get(index) instanceof MetadataUpdateEvent) {
                    return (MetadataUpdateEvent) events.get(index);
                }
            }
            throw new AssertionError("Missing metadata update for reader " + reader);
        }
    }

    private static class NoOpKafkaEnumContextProxy extends StoppableKafkaEnumContextProxy {
        private NoOpKafkaEnumContextProxy(
                String cluster,
                KafkaMetadataService metadata,
                SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
                Runnable callback) {
            super(cluster, metadata, context, callback);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {}
    }

    private static class PartitionDiscoveryContextProxy extends NoOpKafkaEnumContextProxy {
        private boolean initializingSplits;

        private PartitionDiscoveryContextProxy(
                String cluster,
                KafkaMetadataService metadata,
                SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
                Runnable callback) {
            super(cluster, metadata, context, callback);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            if (initializingSplits) {
                try {
                    handler.accept(callable.call(), null);
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            } else {
                initializingSplits = true;
                try {
                    handler.accept((T) Set.of(new TopicPartition("topic", 0)), null);
                } finally {
                    initializingSplits = false;
                }
            }
        }
    }

    private static Set<KafkaStream> streams(Set<String> clusters) {
        if (clusters.isEmpty()) {
            return Collections.emptySet();
        }
        Map<String, ClusterMetadata> metadata = new HashMap<>();
        for (String cluster : clusters) {
            Properties properties = new Properties();
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            metadata.put(cluster, new ClusterMetadata(Set.of("topic"), properties));
        }
        return Set.of(new KafkaStream("stream", metadata));
    }

    private static DynamicKafkaSourceSplit atOffset(DynamicKafkaSourceSplit split, long offset) {
        return new DynamicKafkaSourceSplit(
                split.getKafkaClusterId(),
                new KafkaPartitionSplit(
                        split.getKafkaPartitionSplit().getTopicPartition(),
                        offset,
                        split.getKafkaPartitionSplit()
                                .getStoppingOffset()
                                .orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET)),
                split.getRetainedUntilMs());
    }

    private static List<DynamicKafkaSourceSplit> flatten(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments) {
        return assignments.stream()
                .flatMap(assignment -> assignment.assignment().values().stream())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private static Map<Integer, List<DynamicKafkaSourceSplit>> byReader(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments) {
        Map<Integer, List<DynamicKafkaSourceSplit>> result = new TreeMap<>();
        assignments.forEach(
                assignment ->
                        assignment
                                .assignment()
                                .forEach(
                                        (reader, splits) ->
                                                result.computeIfAbsent(
                                                                reader,
                                                                ignored -> new ArrayList<>())
                                                        .addAll(splits)));
        return result;
    }

    private static Map<String, Integer> owners(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments) {
        Map<String, Integer> result = new HashMap<>();
        assignments.forEach(
                assignment ->
                        assignment
                                .assignment()
                                .forEach(
                                        (reader, splits) ->
                                                splits.forEach(
                                                        split ->
                                                                assertThat(
                                                                                result.put(
                                                                                        split
                                                                                                .splitId(),
                                                                                        reader))
                                                                        .as(
                                                                                "each split has only one assignment")
                                                                        .isNull())));
        return result;
    }
}
