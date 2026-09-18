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
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsSplitReassignmentOnRecovery;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.mock.Whitebox;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Recovery tests for {@link DynamicKafkaSourceEnumerator}. */
public class DynamicKafkaSourceEnumeratorRecoveryTest {

    @Test
    public void testReassignsReportedActiveSplitsAfterMetadataShrink() throws Throwable {
        int parallelism = 4;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String activeTopic = "active-topic";
        String removedTopic = "removed-topic";

        List<DynamicKafkaSourceSplit> activeSplits = createSplits(clusterId, activeTopic, 10);
        List<DynamicKafkaSourceSplit> reportedSplits = new ArrayList<>(activeSplits);
        reportedSplits.addAll(createSplits(clusterId, removedTopic, 2));

        KafkaStream restoredKafkaStream =
                createKafkaStream(streamId, clusterId, Set.of(activeTopic, removedTopic));
        KafkaStream currentKafkaStream = createKafkaStream(streamId, clusterId, activeTopic);
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(restoredKafkaStream, clusterId, reportedSplits);
        Properties properties = createGlobalModeProperties();

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(
                                        Collections.singleton(currentKafkaStream)),
                                context,
                                properties,
                                restoredState)) {
            enumerator.start();
            for (int reader = 0; reader < parallelism; reader++) {
                List<DynamicKafkaSourceSplit> readerReportedSplits =
                        reader == 0 ? reportedSplits : Collections.emptyList();
                context.registerReader(
                        ReaderInfo.createReaderInfo(
                                reader, "location-" + reader, readerReportedSplits));
                enumerator.addReader(reader);
            }

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            context.runNextOneTimeCallable();

            Map<Integer, Integer> assignmentCounts = new HashMap<>();
            Set<String> assignedSplitIds = new HashSet<>();
            int totalAssignments = 0;
            for (int reader = 0; reader < parallelism; reader++) {
                assignmentCounts.put(reader, 0);
            }
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                for (Map.Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assignmentCounts.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
                    totalAssignments += entry.getValue().size();
                    for (DynamicKafkaSourceSplit split : entry.getValue()) {
                        assertThat(split.getKafkaPartitionSplit().getTopic())
                                .isEqualTo(activeTopic);
                        assignedSplitIds.add(split.splitId());
                    }
                }
            }

            assertThat(totalAssignments).isEqualTo(activeSplits.size());
            assertThat(assignedSplitIds).hasSize(activeSplits.size());
            assertThat(assignmentCounts.values()).containsExactlyInAnyOrder(3, 3, 2, 2);
        }
    }

    @Test
    public void testReturnsRetainedSplitsBeforeSendingDeferredMetadata() throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String activeClusterId = "active-cluster";
        String activeTopic = "active-topic";
        DynamicKafkaSourceSplit activeSplit = createSplits(activeClusterId, activeTopic, 1).get(0);
        DynamicKafkaSourceSplit removedSplit =
                createSplits("removed-cluster", "removed-topic", 1).get(0);

        KafkaStream kafkaStream = createKafkaStream(streamId, activeClusterId, activeTopic);
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(
                        kafkaStream, activeClusterId, Collections.singletonList(activeSplit));
        Properties properties = createGlobalModeProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                restoredState)) {
            enumerator.start();
            context.registerReader(
                    ReaderInfo.createReaderInfo(
                            0, "location-0", List.of(activeSplit, removedSplit)));
            enumerator.addReader(0);
            enumerator.handleSourceEvent(0, new GetMetadataUpdateEvent());

            assertThat(context.getSentSourceEvent().getOrDefault(0, Collections.emptyList()))
                    .isEmpty();

            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            context.runNextOneTimeCallable();

            List<DynamicKafkaSourceSplit> assignedSplits =
                    context.getSplitsAssignmentSequence().stream()
                            .flatMap(
                                    assignment ->
                                            assignment.assignment().values().stream()
                                                    .flatMap(List::stream))
                            .collect(java.util.stream.Collectors.toList());
            DynamicKafkaSourceSplit retainedSplit =
                    assignedSplits.stream()
                            .filter(split -> split.splitId().equals(removedSplit.splitId()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertThat(retainedSplit.isRetained()).isTrue();
            assertThat(context.getSentSourceEvent().get(0))
                    .hasSize(1)
                    .allMatch(MetadataUpdateEvent.class::isInstance);
        }
    }

    @Test
    public void testReassignsReportedSplitsWithPerClusterOwnerSelection() throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> splits = createSplits(clusterId, topic, 4);
        KafkaStream kafkaStream = createKafkaStream(streamId, clusterId, topic);

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                createBaseProperties(),
                                createRestoredState(kafkaStream, clusterId, splits))) {
            enumerator.start();
            context.registerReader(ReaderInfo.createReaderInfo(0, "location-0", splits));
            enumerator.addReader(0);
            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);
            context.runNextOneTimeCallable();

            Map<Integer, Integer> assignmentCounts = new HashMap<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                for (Map.Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assignmentCounts.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
                }
            }
            assertThat(assignmentCounts).containsEntry(0, 2).containsEntry(1, 2);
        }
    }

    @Test
    public void testSourceOptsIntoSplitReassignmentOnRecovery() {
        assertThat(
                        SupportsSplitReassignmentOnRecovery.class.isAssignableFrom(
                                DynamicKafkaSource.class))
                .isTrue();
    }

    @Test
    public void testInitialRestoreKeepsOneOwnerBeforeAnyLocalFailure() throws Throwable {
        verifyNumericCheckpointRecovery(false, false);
    }

    @Test
    public void testInitialRestoreDiscoveryBeforeRegistrationKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(true, false);
    }

    @Test
    public void testLocalFailureBeforeNewCheckpointKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(false, true);
    }

    @Test
    public void testLocalFailureWithReverseRegistrationKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 3, true, false, false, false);
    }

    @Test
    public void testLocalFailureWithInterleavedReturnsKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 3, true, true, false, false);
    }

    @Test
    public void testOnlyReaderZeroFailsWithoutMovingSurvivingSplits() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 1, false, false, false, false);
    }

    @Test
    public void testOnlyReaderOneFailsWithoutMovingSurvivingSplits() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 2, false, false, false, false);
    }

    @Test
    public void testPerClusterLocalFailureDoesNotReclaimSurvivingReaderSplits() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 1, false, false, false, false, "per_cluster");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testLocalFailureOfReactivatedRetainedSplitsKeepsCurrentOwners(int failedReaderMask)
            throws Throwable {
        verifyNumericCheckpointRecovery(
                false, true, failedReaderMask, false, false, false, false, "global", true);
    }

    @Test
    public void testLocalFailurePreservesStillInactiveRetainedSplit() throws Throwable {
        String cluster = "cluster-0";
        KafkaStream stream = createKafkaStream("stream", cluster, "active-topic");
        DynamicKafkaSourceSplit retained =
                new DynamicKafkaSourceSplit(
                                cluster,
                                new KafkaPartitionSplit(
                                        new TopicPartition("inactive-topic", 0), 123L))
                        .retainUntil(Long.MAX_VALUE);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createGlobalModeProperties(),
                                createRestoredState(stream, cluster, Collections.emptyList()))) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(retained));
            registerReaderAndReportRestoredSplits(context, enumerator, 1, Collections.emptyList());
            context.runNextOneTimeCallable();
            assertExactCheckpointOffsets(context, 0, List.of(retained));

            int priorAssignments = context.getSplitsAssignmentSequence().size();
            context.unregisterReader(0);
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(retained));

            assertExactCheckpointOffsets(context, priorAssignments, List.of(retained));
            context.getSplitsAssignmentSequence().stream()
                    .skip(priorAssignments)
                    .forEach(assignment -> assertThat(assignment.assignment()).containsOnlyKeys(0));
        }
    }

    @Test
    public void testLocalFailureAfterNewCheckpointUsesReportedOffsets() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 1, false, false, true, false);
    }

    @Test
    public void testCheckpointReportsReplacePreinitializedPendingOwnersAndOffsets()
            throws Throwable {
        verifyNumericCheckpointRecovery(true, true, 3, true, true, false, true);
    }

    @Test
    public void testNewPartitionDiscoveryWaitsForRecoveryAndSurvivesRepeatedLocalFailure()
            throws Throwable {
        String cluster = "cluster-0";
        KafkaStream stream = createKafkaStream("stream", cluster, "topic");
        List<DynamicKafkaSourceSplit> checkpointSplits = createSplits(cluster, "topic", 2);
        DynamicKafkaSourceSplit discovered =
                withOffset(createSplits(cluster, "topic", 3).get(2), 2500L);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createGlobalModeProperties(),
                                createRestoredState(stream, cluster, checkpointSplits))) {
            enumerator.start();
            context.runNextOneTimeCallable();
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 0, Collections.singletonList(checkpointSplits.get(0)));
            // Invoke the real coordinator callback with a completed broker-discovery result.
            // No broker or mock owner-selection implementation participates in the regression.
            Object subEnumerator =
                    ((Map<?, ?>) Whitebox.getInternalState(enumerator, "clusterEnumeratorMap"))
                            .get(cluster);
            Class<?> changeClass =
                    Class.forName(KafkaSourceEnumerator.class.getName() + "$PartitionSplitChange");
            Constructor<?> constructor = changeClass.getDeclaredConstructor(Set.class, Set.class);
            constructor.setAccessible(true);
            Method handler =
                    KafkaSourceEnumerator.class.getDeclaredMethod(
                            "handlePartitionSplitChanges", changeClass, Throwable.class);
            handler.setAccessible(true);
            handler.invoke(
                    subEnumerator,
                    constructor.newInstance(
                            Collections.singleton(discovered.getKafkaPartitionSplit()),
                            Collections.emptySet()),
                    null);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 1, Collections.singletonList(checkpointSplits.get(1)));
            List<DynamicKafkaSourceSplit> expected = new ArrayList<>(checkpointSplits);
            expected.add(discovered);
            assertExactCheckpointOffsets(context, 0, expected);
            Map<Integer, List<DynamicKafkaSourceSplit>> actualOwners = new HashMap<>();
            context.getSplitsAssignmentSequence()
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            actualOwners
                                                                    .computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            int failedReader =
                    actualOwners.entrySet().stream()
                            .filter(entry -> entry.getValue().contains(discovered))
                            .findFirst()
                            .get()
                            .getKey();
            for (int attempt = 0; attempt < 2; attempt++) {
                int priorAssignments = context.getSplitsAssignmentSequence().size();
                context.unregisterReader(failedReader);
                enumerator.addSplitsBack(actualOwners.get(failedReader), failedReader);
                registerReaderAndReportRestoredSplits(
                        context,
                        enumerator,
                        failedReader,
                        Collections.singletonList(checkpointSplits.get(failedReader)));
                assertExactCheckpointOffsets(
                        context, priorAssignments, actualOwners.get(failedReader));
                context.getSplitsAssignmentSequence().stream()
                        .skip(priorAssignments)
                        .forEach(
                                assignment ->
                                        assertThat(assignment.assignment())
                                                .containsOnlyKeys(failedReader));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("boundedRecoveryModes")
    public void testBoundedRecoveryAssignsAllClustersBeforeNoMoreSplits(
            String mode, boolean discoverFirst) throws Throwable {
        verifyBoundedRecoveryAssignmentOrder(mode, discoverFirst, true);
    }

    @ParameterizedTest
    @MethodSource("boundedRecoveryModes")
    public void testBoundedLocalRecoveryAssignsAllClustersBeforeNoMoreSplits(
            String mode, boolean discoverFirst) throws Throwable {
        verifyBoundedRecoveryAssignmentOrder(mode, discoverFirst, false);
    }

    private void verifyBoundedRecoveryAssignmentOrder(
            String mode, boolean discoverFirst, boolean checkInitialOrder) throws Throwable {
        Map<String, ClusterMetadata> metadata = new HashMap<>();
        Map<String, KafkaSourceEnumState> states = new HashMap<>();
        Map<Integer, List<DynamicKafkaSourceSplit>> reports = new HashMap<>();
        for (String cluster : List.of("cluster-0", "cluster-1")) {
            metadata.putAll(createKafkaStream("stream", cluster, "topic").getClusterMetadataMap());
            List<DynamicKafkaSourceSplit> splits = new ArrayList<>();
            for (int reader = 0; reader < 2; reader++) {
                DynamicKafkaSourceSplit split =
                        new DynamicKafkaSourceSplit(
                                cluster,
                                new KafkaPartitionSplit(
                                        new TopicPartition("topic", reader), 100L + reader, 1000L));
                splits.add(split);
                reports.computeIfAbsent(reader, ignored -> new ArrayList<>()).add(split);
            }
            states.put(
                    cluster,
                    new KafkaSourceEnumState(unwrapSplits(splits), Collections.emptyList(), true));
        }
        KafkaStream stream = new KafkaStream("stream", metadata);
        Properties properties = createBaseProperties();
        properties.setProperty(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), mode);
        List<String> events = new ArrayList<>();
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<DynamicKafkaSourceSplit>(2) {
                            @Override
                            public void assignSplits(
                                    SplitsAssignment<DynamicKafkaSourceSplit> assignment) {
                                assignment
                                        .assignment()
                                        .forEach(
                                                (reader, splits) ->
                                                        splits.forEach(
                                                                split ->
                                                                        events.add(
                                                                                "assign:"
                                                                                        + reader
                                                                                        + ":"
                                                                                        + split
                                                                                                .splitId())));
                                super.assignSplits(assignment);
                            }

                            @Override
                            public void signalNoMoreSplits(int reader) {
                                events.add("done:" + reader);
                                super.signalNoMoreSplits(reader);
                            }
                        };
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton("stream")),
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                OffsetsInitializer.earliest(),
                                OffsetsInitializer.latest(),
                                properties,
                                Boundedness.BOUNDED,
                                new DynamicKafkaSourceEnumState(
                                        Collections.singleton(stream), states),
                                new NoOpKafkaEnumContextProxyFactory())) {
            enumerator.start();
            // Complete actual sub-enumerator discovery callbacks without broker I/O. Each
            // sub-enumerator must subsequently signal completion during its addReader flush.
            Map<?, ?> subEnumerators =
                    (Map<?, ?>) Whitebox.getInternalState(enumerator, "clusterEnumeratorMap");
            Class<?> changeClass =
                    Class.forName(KafkaSourceEnumerator.class.getName() + "$PartitionSplitChange");
            Constructor<?> constructor = changeClass.getDeclaredConstructor(Set.class, Set.class);
            constructor.setAccessible(true);
            Method handler =
                    KafkaSourceEnumerator.class.getDeclaredMethod(
                            "handlePartitionSplitChanges", changeClass, Throwable.class);
            handler.setAccessible(true);
            for (Object subEnumerator : subEnumerators.values()) {
                handler.invoke(
                        subEnumerator,
                        constructor.newInstance(Collections.emptySet(), Collections.emptySet()),
                        null);
            }
            if (discoverFirst) {
                context.runNextOneTimeCallable();
            }
            for (int reader = 0; reader < 2; reader++) {
                registerReaderAndReportRestoredSplits(
                        context, enumerator, reader, reports.get(reader));
            }
            if (!discoverFirst) {
                context.runNextOneTimeCallable();
            }
            if (checkInitialOrder) {
                assertBoundedAssignmentsBeforeCompletion(events, 0, 2);
                assertBoundedAssignmentsBeforeCompletion(events, 1, 2);
            }
            Map<Integer, List<DynamicKafkaSourceSplit>> assigned = new HashMap<>();
            context.getSplitsAssignmentSequence()
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            assigned.computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            events.clear();
            int priorAssignments = context.getSplitsAssignmentSequence().size();
            context.unregisterReader(0);
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, assigned.get(0));

            assertBoundedAssignmentsBeforeCompletion(events, 0, 2);
            assertThat(events).noneMatch(event -> event.startsWith("assign:1:"));
            assertExactCheckpointOffsets(context, priorAssignments, assigned.get(0));
            context.getSplitsAssignmentSequence().stream()
                    .skip(priorAssignments)
                    .flatMap(assignment -> assignment.assignment().values().stream())
                    .flatMap(List::stream)
                    .forEach(
                            split ->
                                    assertThat(split.getKafkaPartitionSplit().getStoppingOffset())
                                            .contains(1000L));
        }
    }

    private static Stream<Arguments> boundedRecoveryModes() {
        return Stream.of(
                Arguments.of("per_cluster", true),
                Arguments.of("per_cluster", false),
                Arguments.of("global", true),
                Arguments.of("global", false));
    }

    private static void assertBoundedAssignmentsBeforeCompletion(
            List<String> events, int reader, int expectedSplits) {
        String assignmentPrefix = "assign:" + reader + ":";
        int completion = events.indexOf("done:" + reader);
        assertThat(completion).as("completion must be signaled: %s", events).isNotNegative();
        assertThat(events.stream().filter(event -> event.startsWith(assignmentPrefix)))
                .hasSize(expectedSplits);
        assertThat(
                        events.subList(0, completion).stream()
                                .filter(event -> event.startsWith(assignmentPrefix)))
                .as("all restored assignments must precede completion: %s", events)
                .hasSize(expectedSplits);
    }

    private void verifyNumericCheckpointRecovery(boolean discoverFirst, boolean localFailure)
            throws Throwable {
        verifyNumericCheckpointRecovery(discoverFirst, localFailure, 3, false, false, false, false);
    }

    private void verifyNumericCheckpointRecovery(
            boolean discoverFirst,
            boolean localFailure,
            int failedReaderMask,
            boolean reverseRegistration,
            boolean interleavedReturns,
            boolean checkpointAfterRedistribution,
            boolean restoredPendingCopies)
            throws Throwable {
        verifyNumericCheckpointRecovery(
                discoverFirst,
                localFailure,
                failedReaderMask,
                reverseRegistration,
                interleavedReturns,
                checkpointAfterRedistribution,
                restoredPendingCopies,
                "global");
    }

    private void verifyNumericCheckpointRecovery(
            boolean discoverFirst,
            boolean localFailure,
            int failedReaderMask,
            boolean reverseRegistration,
            boolean interleavedReturns,
            boolean checkpointAfterRedistribution,
            boolean restoredPendingCopies,
            String mode)
            throws Throwable {
        verifyNumericCheckpointRecovery(
                discoverFirst,
                localFailure,
                failedReaderMask,
                reverseRegistration,
                interleavedReturns,
                checkpointAfterRedistribution,
                restoredPendingCopies,
                mode,
                false);
    }

    private void verifyNumericCheckpointRecovery(
            boolean discoverFirst,
            boolean localFailure,
            int failedReaderMask,
            boolean reverseRegistration,
            boolean interleavedReturns,
            boolean checkpointAfterRedistribution,
            boolean restoredPendingCopies,
            String mode,
            boolean retainedCheckpointReports)
            throws Throwable {
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        int parallelism = 2;
        List<DynamicKafkaSourceSplit> checkpointSplits =
                createSplits(clusterId, topic, 11).stream()
                        .map(
                                split ->
                                        new DynamicKafkaSourceSplit(
                                                clusterId,
                                                new KafkaPartitionSplit(
                                                        split.getKafkaPartitionSplit()
                                                                .getTopicPartition(),
                                                        1000L
                                                                + split.getKafkaPartitionSplit()
                                                                        .getTopicPartition()
                                                                        .partition())))
                        .collect(Collectors.toList());
        KafkaStream stream = createKafkaStream(streamId, clusterId, topic);
        Properties properties = createBaseProperties();
        properties.setProperty(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), mode);
        Map<Integer, List<DynamicKafkaSourceSplit>> checkpointOwners = new HashMap<>();
        for (DynamicKafkaSourceSplit split : checkpointSplits) {
            int owner =
                    split.getKafkaPartitionSplit().getTopicPartition().partition() % parallelism;
            checkpointOwners
                    .computeIfAbsent(owner, ignored -> new ArrayList<>())
                    .add(retainedCheckpointReports ? split.retainUntil(Long.MAX_VALUE) : split);
        }
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(stream, clusterId, checkpointSplits);
        if (restoredPendingCopies) {
            restoredState =
                    new DynamicKafkaSourceEnumState(
                            Collections.singleton(stream),
                            Collections.singletonMap(
                                    clusterId,
                                    new KafkaSourceEnumState(
                                            Collections.emptyList(),
                                            checkpointSplits.stream()
                                                    .map(
                                                            split ->
                                                                    withOffset(split, 9000L)
                                                                            .getKafkaPartitionSplit())
                                                    .collect(Collectors.toList()),
                                            true)));
        }
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                properties,
                                restoredState)) {
            enumerator.start();
            if (discoverFirst) {
                context.runNextOneTimeCallable();
            }
            for (int reader = 0; reader < parallelism; reader++) {
                registerReaderAndReportRestoredSplits(
                        context, enumerator, reader, checkpointOwners.get(reader));
            }
            if (!discoverFirst) {
                context.runNextOneTimeCallable();
            }
            assertSinglePhysicalOwners(context, 0, 11);
            assertExactCheckpointOffsets(context, 0, checkpointSplits);
            if (!localFailure) {
                return;
            }
            // Model SourceCoordinator local recovery: return all
            // assignments since the restored checkpoint, then register original checkpoint reports.
            Map<Integer, List<DynamicKafkaSourceSplit>> uncheckpointedAssignments = new HashMap<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                assignment
                        .assignment()
                        .forEach(
                                (reader, splits) ->
                                        uncheckpointedAssignments
                                                .computeIfAbsent(
                                                        reader, ignored -> new ArrayList<>())
                                                .addAll(splits));
            }
            int assignmentsBeforeFailure = context.getSplitsAssignmentSequence().size();
            Map<Integer, List<DynamicKafkaSourceSplit>> currentAssignments = new HashMap<>();
            uncheckpointedAssignments.forEach(
                    (reader, splits) -> currentAssignments.put(reader, new ArrayList<>(splits)));
            if (checkpointAfterRedistribution) {
                // Runtime no longer returns assignments covered by a completed checkpoint.
                // Reader reports now use the current owner and the checkpointed next offset.
                checkpointOwners.clear();
                currentAssignments.forEach(
                        (reader, splits) ->
                                checkpointOwners.put(
                                        reader,
                                        splits.stream()
                                                .map(
                                                        split ->
                                                                withOffset(
                                                                        split,
                                                                        split.getKafkaPartitionSplit()
                                                                                        .getStartingOffset()
                                                                                + 10000L))
                                                .collect(Collectors.toList())));
                uncheckpointedAssignments.replaceAll((reader, splits) -> Collections.emptyList());
            }
            for (int reader = 0; reader < parallelism; reader++) {
                if ((failedReaderMask & (1 << reader)) != 0) {
                    context.unregisterReader(reader);
                }
            }
            if (!interleavedReturns) {
                for (int reader = 0; reader < parallelism; reader++) {
                    if ((failedReaderMask & (1 << reader)) != 0) {
                        enumerator.addSplitsBack(uncheckpointedAssignments.get(reader), reader);
                    }
                }
            }
            for (int index = 0; index < parallelism; index++) {
                int reader = reverseRegistration ? parallelism - 1 - index : index;
                if ((failedReaderMask & (1 << reader)) != 0) {
                    if (interleavedReturns) {
                        enumerator.addSplitsBack(uncheckpointedAssignments.get(reader), reader);
                    }
                    registerReaderAndReportRestoredSplits(
                            context, enumerator, reader, checkpointOwners.get(reader));
                }
            }
            Map<Integer, List<DynamicKafkaSourceSplit>> assignmentsAfterFailure = new HashMap<>();
            context.getSplitsAssignmentSequence().stream()
                    .skip(assignmentsBeforeFailure)
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            assignmentsAfterFailure
                                                                    .computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            for (int reader = 0; reader < parallelism; reader++) {
                if ((failedReaderMask & (1 << reader)) == 0) {
                    assertThat(assignmentsAfterFailure).doesNotContainKey(reader);
                } else {
                    List<DynamicKafkaSourceSplit> expected =
                            checkpointAfterRedistribution
                                    ? checkpointOwners.get(reader)
                                    : currentAssignments.get(reader);
                    assertThat(assignmentsAfterFailure.get(reader))
                            .as(
                                    "failed reader %s keeps its actual owner and checkpoint offsets",
                                    reader)
                            .containsExactlyInAnyOrderElementsOf(expected);
                }
            }
        }
    }

    private static DynamicKafkaSourceSplit withOffset(DynamicKafkaSourceSplit split, long offset) {
        return new DynamicKafkaSourceSplit(
                split.getKafkaClusterId(),
                new KafkaPartitionSplit(
                        split.getKafkaPartitionSplit().getTopicPartition(), offset));
    }

    private static void assertExactCheckpointOffsets(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            int skipAssignments,
            List<DynamicKafkaSourceSplit> expected) {
        assertThat(
                        context.getSplitsAssignmentSequence().stream()
                                .skip(skipAssignments)
                                .flatMap(assignment -> assignment.assignment().values().stream())
                                .flatMap(List::stream)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void assertSinglePhysicalOwners(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            int skipAssignments,
            int expectedPartitions) {
        Map<String, Set<Integer>> owners = new HashMap<>();
        context.getSplitsAssignmentSequence().stream()
                .skip(skipAssignments)
                .forEach(
                        assignment ->
                                assignment
                                        .assignment()
                                        .forEach(
                                                (reader, splits) ->
                                                        splits.forEach(
                                                                split ->
                                                                        owners.computeIfAbsent(
                                                                                        split
                                                                                                .splitId(),
                                                                                        ignored ->
                                                                                                new HashSet<>())
                                                                                .add(reader))));
        assertThat(owners).hasSize(expectedPartitions);
        assertThat(owners.entrySet())
                .as("every physical split must have one active reader: %s", owners)
                .allSatisfy(entry -> assertThat(entry.getValue()).hasSize(1));
    }

    private static void registerReaderAndReportRestoredSplits(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            DynamicKafkaSourceEnumerator enumerator,
            int readerId,
            List<DynamicKafkaSourceSplit> restoredSplits) {
        context.registerReader(
                ReaderInfo.createReaderInfo(readerId, "location-" + readerId, restoredSplits));
        enumerator.addReader(readerId);
    }

    private static DynamicKafkaSourceEnumState createRestoredState(
            KafkaStream kafkaStream, String clusterId, List<DynamicKafkaSourceSplit> activeSplits) {
        return new DynamicKafkaSourceEnumState(
                Collections.singleton(kafkaStream),
                Collections.singletonMap(
                        clusterId,
                        new KafkaSourceEnumState(
                                unwrapSplits(activeSplits), Collections.emptyList(), true)));
    }

    private static Properties createGlobalModeProperties() {
        Properties properties = createBaseProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());
        return properties;
    }

    private static Properties createBaseProperties() {
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
        return properties;
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            String streamId,
            KafkaMetadataService metadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            Properties properties,
            DynamicKafkaSourceEnumState restoredState) {
        return new DynamicKafkaSourceEnumerator(
                new KafkaStreamSetSubscriber(Collections.singleton(streamId)),
                metadataService,
                context,
                OffsetsInitializer.earliest(),
                new NoStoppingOffsetsInitializer(),
                properties,
                Boundedness.CONTINUOUS_UNBOUNDED,
                restoredState,
                new NoOpKafkaEnumContextProxyFactory());
    }

    private static KafkaStream createKafkaStream(
            String streamId, String clusterId, String activeTopic) {
        return createKafkaStream(streamId, clusterId, Collections.singleton(activeTopic));
    }

    private static KafkaStream createKafkaStream(
            String streamId, String clusterId, Set<String> activeTopics) {
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return new KafkaStream(
                streamId,
                Collections.singletonMap(
                        clusterId, new ClusterMetadata(activeTopics, clusterProperties)));
    }

    private static List<DynamicKafkaSourceSplit> createSplits(
            String clusterId, String topic, int count) {
        List<DynamicKafkaSourceSplit> splits = new ArrayList<>();
        for (int partition = 0; partition < count; partition++) {
            splits.add(
                    new DynamicKafkaSourceSplit(
                            clusterId,
                            new KafkaPartitionSplit(
                                    new TopicPartition(topic, partition),
                                    KafkaPartitionSplit.EARLIEST_OFFSET)));
        }
        return splits;
    }

    private static List<KafkaPartitionSplit> unwrapSplits(
            List<DynamicKafkaSourceSplit> dynamicSplits) {
        List<KafkaPartitionSplit> splits = new ArrayList<>();
        for (DynamicKafkaSourceSplit split : dynamicSplits) {
            splits.add(split.getKafkaPartitionSplit());
        }
        return splits;
    }

    private static class NoOpKafkaEnumContextProxyFactory
            implements StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory {

        @Override
        public StoppableKafkaEnumContextProxy create(
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                Runnable signalNoMoreSplitsCallback) {
            return new NoOpKafkaEnumContextProxy(
                    kafkaClusterId, kafkaMetadataService, enumContext, signalNoMoreSplitsCallback);
        }
    }

    private static class NoOpKafkaEnumContextProxy extends StoppableKafkaEnumContextProxy {

        private NoOpKafkaEnumContextProxy(
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
                Runnable signalNoMoreSplitsCallback) {
            super(kafkaClusterId, kafkaMetadataService, enumContext, signalNoMoreSplitsCallback);
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
}
