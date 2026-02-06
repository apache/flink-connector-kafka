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
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.checkpoint.RoundRobinOperatorStateRepartitioner;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test for {@link DynamicKafkaSourceEnumerator}. */
public class DynamicKafkaSourceEnumeratorTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC = "DynamicKafkaSourceEnumeratorTest";
    private static final int NUM_SPLITS_PER_CLUSTER = 3;
    private static final int NUM_RECORDS_PER_SPLIT = 5;
    private static final String SOURCE_READER_SPLIT_STATE_NAME = "source-reader-splits";

    @BeforeAll
    public static void beforeAll() throws Throwable {
        DynamicKafkaSourceTestHelper.setup();
        DynamicKafkaSourceTestHelper.createTopic(TOPIC, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                TOPIC, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        DynamicKafkaSourceTestHelper.tearDown();
    }

    @Test
    public void testStartupWithoutContinuousDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
        }
    }

    @Test
    public void testStartupWithContinuousDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                (properties) ->
                                        properties.setProperty(
                                                DynamicKafkaSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
        }
    }

    @Test
    public void testStartupWithKafkaMetadataServiceFailure_noPeriodicDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context, new MockKafkaMetadataService(true), (properties) -> {})) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThatThrownBy(() -> runAllOneTimeCallables(context))
                    .as(
                            "Exception expected since periodic discovery is disabled and metadata is required for setting up the job")
                    .hasRootCause(new RuntimeException("Mock exception"));
        }
    }

    @Test
    public void testStartupWithKafkaMetadataServiceFailure_withContinuousDiscovery()
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                new MockKafkaMetadataService(true),
                                (properties) ->
                                        properties.setProperty(
                                                DynamicKafkaSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).hasSize(1);
            assertThatThrownBy(() -> context.runPeriodicCallable(0))
                    .as("Exception expected since there is no state")
                    .hasRootCause(new RuntimeException("Mock exception"));
        }
    }

    @Test
    public void
            testStartupWithKafkaMetadataServiceFailure_withContinuousDiscoveryAndCheckpointState()
                    throws Throwable {
        // init enumerator with checkpoint state
        final DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState = getCheckpointState();
        Properties properties = new Properties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                new MockKafkaMetadataService(true),
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                dynamicKafkaSourceEnumState,
                                new TestKafkaEnumContextProxyFactory())) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).hasSize(1);
            // no exception
            context.runPeriodicCallable(0);

            assertThatThrownBy(() -> context.runPeriodicCallable(0))
                    .hasRootCause(new RuntimeException("Mock exception"));
        }
    }

    @Test
    public void testHandleMetadataServiceError() throws Throwable {
        int failureThreshold = 5;

        Properties properties = new Properties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                Integer.toString(failureThreshold));
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");

        MockKafkaMetadataService mockKafkaMetadataService =
                new MockKafkaMetadataService(
                        Collections.singleton(DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC)));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                mockKafkaMetadataService,
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new DynamicKafkaSourceEnumState(),
                                new TestKafkaEnumContextProxyFactory())) {
            enumerator.start();

            assertThat(context.getPeriodicCallables()).hasSize(1);
            context.runPeriodicCallable(0);

            // init splits
            runAllOneTimeCallables(context);

            // swap to exceptional metadata service
            mockKafkaMetadataService.setThrowException(true);

            for (int i = 0; i < failureThreshold; i++) {
                context.runPeriodicCallable(0);
            }

            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> context.runPeriodicCallable(0))
                        .hasRootCause(new RuntimeException("Mock exception"));
                // Need to reset internal throwable reference after each invocation of
                // runPeriodicCallable,
                // since the context caches the previous exceptions indefinitely
                AtomicReference<Throwable> errorInWorkerThread =
                        (AtomicReference<Throwable>)
                                Whitebox.getInternalState(context, "errorInWorkerThread");
                errorInWorkerThread.set(null);
            }

            mockKafkaMetadataService.setThrowException(false);
            assertThatCode(() -> context.runPeriodicCallable(0))
                    .as("Exception counter should have been reset")
                    .doesNotThrowAnyException();
        }
    }

    @Test
    public void testKafkaMetadataServiceDiscovery() throws Throwable {
        KafkaStream kafkaStreamWithOneCluster = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);
        kafkaStreamWithOneCluster
                .getClusterMetadataMap()
                .remove(DynamicKafkaSourceTestHelper.getKafkaClusterId(1));

        KafkaStream kafkaStreamWithTwoClusters = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);

        MockKafkaMetadataService mockKafkaMetadataService =
                new MockKafkaMetadataService(Collections.singleton(kafkaStreamWithOneCluster));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                mockKafkaMetadataService,
                                (properties) ->
                                        properties.setProperty(
                                                DynamicKafkaSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            context.runPeriodicCallable(0);

            // 1 callable for main enumerator and 2 for the sub enumerators since we have 2 clusters
            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();

            assertThat(context.getSplitsAssignmentSequence())
                    .as("no splits should be assigned yet since there are no readers")
                    .isEmpty();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), kafkaStreamWithOneCluster);
            int currentNumSplits = context.getSplitsAssignmentSequence().size();

            // no changes to splits
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            assertThat(context.getSplitsAssignmentSequence()).hasSize(currentNumSplits);

            // mock metadata change
            mockKafkaMetadataService.setKafkaStreams(
                    Collections.singleton(kafkaStreamWithTwoClusters));

            // changes should have occurred here
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "1 additional split assignment since there was 1 metadata update that caused a change")
                    .hasSize(currentNumSplits + 1);
            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), kafkaStreamWithTwoClusters);
        }
    }

    @Test
    public void testReaderRegistrationAfterSplitDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();
            assertThat(context.getSplitsAssignmentSequence())
                    .as("no splits should be assigned yet since there are no readers")
                    .isEmpty();
            assertThat(context.getSplitsAssignmentSequence())
                    .as("no readers have registered yet")
                    .isEmpty();
            assertThat(context.getSentSourceEvent()).as("no readers have registered yet").isEmpty();

            // initialize readers 0 and 2
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            assertThat(context.getSentSourceEvent().keySet())
                    .as("reader 0 and 2 should have only received the source event")
                    .containsExactlyInAnyOrder(0, 2);
            Set<Integer> allReadersThatReceivedSplits =
                    context.getSplitsAssignmentSequence().stream()
                            .flatMap(
                                    splitAssignment ->
                                            splitAssignment.assignment().keySet().stream())
                            .collect(Collectors.toSet());
            assertThat(allReadersThatReceivedSplits)
                    .as("reader 0 and 2 should hve only received splits")
                    .containsExactlyInAnyOrder(0, 2);

            // initialize readers 1
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            assertThat(context.getSentSourceEvent().keySet())
                    .as("all readers should have received get metadata update event")
                    .containsExactlyInAnyOrder(0, 1, 2);

            for (List<SourceEvent> sourceEventsPerReader : context.getSentSourceEvent().values()) {
                assertThat(sourceEventsPerReader)
                        .as("there should have been only 1 source event per reader")
                        .hasSize(1);
            }

            // should have all splits assigned by now
            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC));
        }
    }

    @Test
    public void testReaderRegistrationBeforeSplitDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            assertThat(context.getSplitsAssignmentSequence())
                    .as("readers should not be assigned yet since there are no splits")
                    .isEmpty();

            // 1 callable for main enumerator and 3 for the sub enumerators since we have 3 clusters
            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC));
        }
    }

    @Test
    public void testSnapshotState() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            DynamicKafkaSourceEnumState stateBeforeSplitAssignment = enumerator.snapshotState(-1);
            assertThat(
                            stateBeforeSplitAssignment.getClusterEnumeratorStates().values()
                                    .stream()
                                    .map(subState -> subState.assignedSplits().stream())
                                    .count())
                    .as("no readers registered, so state should be empty")
                    .isZero();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            assertThat(context.getSplitsAssignmentSequence())
                    .as("readers should not be assigned yet since there are no splits")
                    .isEmpty();

            // 1 callable for main enumerator and 3 for the sub enumerators since we have 3 clusters
            runAllOneTimeCallables(context);

            assertThat(context.getOneTimeCallables())
                    .as("There should be no more callables after running the 4")
                    .isEmpty();

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC));

            DynamicKafkaSourceEnumState stateAfterSplitAssignment = enumerator.snapshotState(-1);

            assertThat(
                            stateAfterSplitAssignment.getClusterEnumeratorStates().values().stream()
                                    .flatMap(enumState -> enumState.assignedSplits().stream())
                                    .count())
                    .isEqualTo(
                            NUM_SPLITS_PER_CLUSTER
                                    * DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS);
        }
    }

    @Test
    public void testEnumeratorStateDoesNotContainStaleTopicPartitions() throws Throwable {
        final String topic2 = TOPIC + "_2";

        DynamicKafkaSourceTestHelper.createTopic(topic2, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                topic2, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);

        final Set<KafkaStream> initialStreams =
                Collections.singleton(
                        new KafkaStream(
                                TOPIC,
                                DynamicKafkaSourceTestHelper.getClusterMetadataMap(
                                        0, TOPIC, topic2)));

        final Set<KafkaStream> updatedStreams =
                Collections.singleton(
                        new KafkaStream(
                                TOPIC,
                                DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC)));

        try (MockKafkaMetadataService metadataService =
                        new MockKafkaMetadataService(initialStreams);
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                (properties) ->
                                        properties.setProperty(
                                                DynamicKafkaSourceOptions
                                                        .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                        .key(),
                                                "1"))) {
            enumerator.start();

            context.runPeriodicCallable(0);

            runAllOneTimeCallables(context);

            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);

            DynamicKafkaSourceEnumState initialState = enumerator.snapshotState(-1);

            assertThat(getFilteredTopicPartitions(initialState, TOPIC, AssignmentStatus.ASSIGNED))
                    .hasSize(2);
            assertThat(getFilteredTopicPartitions(initialState, TOPIC, AssignmentStatus.UNASSIGNED))
                    .hasSize(1);
            assertThat(getFilteredTopicPartitions(initialState, topic2, AssignmentStatus.ASSIGNED))
                    .hasSize(2);
            assertThat(
                            getFilteredTopicPartitions(
                                    initialState, topic2, AssignmentStatus.UNASSIGNED))
                    .hasSize(1);

            // mock metadata change
            metadataService.setKafkaStreams(updatedStreams);

            // changes should have occurred here
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            DynamicKafkaSourceEnumState migratedState = enumerator.snapshotState(-1);

            assertThat(getFilteredTopicPartitions(migratedState, TOPIC, AssignmentStatus.ASSIGNED))
                    .hasSize(3);
            assertThat(
                            getFilteredTopicPartitions(
                                    migratedState, TOPIC, AssignmentStatus.UNASSIGNED))
                    .isEmpty();
            assertThat(getFilteredTopicPartitions(migratedState, topic2, AssignmentStatus.ASSIGNED))
                    .isEmpty();
            assertThat(
                            getFilteredTopicPartitions(
                                    migratedState, topic2, AssignmentStatus.UNASSIGNED))
                    .isEmpty();
        }
    }

    @Test
    public void testStartupWithCheckpointState() throws Throwable {
        // init enumerator with checkpoint state
        final DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState = getCheckpointState();
        Properties properties = new Properties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                new MockKafkaMetadataService(
                                        Collections.singleton(
                                                DynamicKafkaSourceTestHelper.getKafkaStream(
                                                        TOPIC))),
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                dynamicKafkaSourceEnumState,
                                new TestKafkaEnumContextProxyFactory())) {
            // start and check callables
            enumerator.start();
            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as(
                            "3 one time callables should have been scheduled. 1 for main enumerator and then 2 for each underlying enumerator")
                    .hasSize(1 + DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS);

            // initialize all readers and do split assignment
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            runAllOneTimeCallables(context);

            assertThat(context.getSentSourceEvent()).as("3 readers registered").hasSize(3);
            for (List<SourceEvent> sourceEventsReceived : context.getSentSourceEvent().values()) {
                assertThat(sourceEventsReceived)
                        .as("each reader should have sent 1 source event")
                        .hasSize(1);
            }

            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "there should not be new splits and we don't assign previously assigned splits at startup and there is no metadata/split changes")
                    .isEmpty();
        }

        // test with periodic discovery enabled
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                new MockKafkaMetadataService(
                                        Collections.singleton(
                                                DynamicKafkaSourceTestHelper.getKafkaStream(
                                                        TOPIC))),
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                dynamicKafkaSourceEnumState,
                                new TestKafkaEnumContextProxyFactory())) {
            enumerator.start();
            assertThat(context.getPeriodicCallables())
                    .as("A periodic time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThat(context.getOneTimeCallables())
                    .as(
                            "0 one time callables for main enumerator and 2 one time callables for each underlying enumerator should have been scheduled")
                    .hasSize(DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS);

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            // checkpoint state should have triggered split assignment
            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "There is no split assignment since there are no new splits that are not contained in state")
                    .isEmpty();
        }
    }

    @Test
    public void testAddSplitsBack() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator = createEnumerator(context)) {
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            enumerator.start();

            runAllOneTimeCallables(context);

            Map<Integer, Set<DynamicKafkaSourceSplit>> readerAssignmentsBeforeFailure =
                    getReaderAssignments(context);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("we only expect splits have been assigned 2 times")
                    .hasSize(DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS);

            // simulate failures
            context.unregisterReader(0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(0), 0);
            context.unregisterReader(2);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(2), 2);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("Splits assignment should be unchanged")
                    .hasSize(DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS);

            // mock reader recovery
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC));
            assertThat(getReaderAssignments(context))
                    .containsAllEntriesOf(readerAssignmentsBeforeFailure);
            assertThat(context.getSplitsAssignmentSequence())
                    .as(
                            "the readers came back up, so there should be 2 additional split assignments in the sequence")
                    .hasSize(DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS + 2);
        }
    }

    @Test
    public void testGlobalEnumeratorModeBalancesSplitsAcrossClusters() throws Throwable {
        final int numSubtasks = 2;
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(numSubtasks);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                properties ->
                                        properties.setProperty(
                                                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE
                                                        .key(),
                                                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL
                                                        .name()
                                                        .toLowerCase()))) {
            enumerator.start();

            for (int i = 0; i < numSubtasks; i++) {
                mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, i);
            }

            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC));

            Map<Integer, Integer> assignedSplitCountByReader = new HashMap<>();
            for (int readerId = 0; readerId < numSubtasks; readerId++) {
                assignedSplitCountByReader.put(readerId, 0);
            }
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                for (Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assignedSplitCountByReader.merge(
                            entry.getKey(), entry.getValue().size(), Integer::sum);
                }
            }
            int minAssignedSplits = Collections.min(assignedSplitCountByReader.values());
            int maxAssignedSplits = Collections.max(assignedSplitCountByReader.values());
            assertThat(maxAssignedSplits - minAssignedSplits)
                    .as(
                            "global mode should keep per-reader split counts balanced, current assigned split count is "
                                    + assignedSplitCountByReader.values())
                    .isLessThanOrEqualTo(1);
        }
    }

    @Test
    public void testGlobalModeBalancesAssignmentsWithClusterAndPartitionExpansion()
            throws Throwable {
        final int numSubtasks = 4;
        final String expandedTopic = TOPIC + "_expanded";
        final int expandedTopicPartitions = NUM_SPLITS_PER_CLUSTER * 2;
        DynamicKafkaSourceTestHelper.createTopic(expandedTopic, expandedTopicPartitions, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                expandedTopic, expandedTopicPartitions, NUM_RECORDS_PER_SPLIT);

        KafkaStream initialStream =
                new KafkaStream(
                        TOPIC, DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC));

        Map<String, ClusterMetadata> expandedClusterMetadata = new HashMap<>();
        expandedClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC, expandedTopic));
        expandedClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(1, TOPIC, expandedTopic));
        KafkaStream expandedStream = new KafkaStream(TOPIC, expandedClusterMetadata);

        MockKafkaMetadataService metadataService =
                new MockKafkaMetadataService(Collections.singleton(initialStream));
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(numSubtasks);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                properties -> {
                                    properties.setProperty(
                                            DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                                            DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL
                                                    .name()
                                                    .toLowerCase());
                                    properties.setProperty(
                                            DynamicKafkaSourceOptions
                                                    .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                    .key(),
                                            "1");
                                })) {
            enumerator.start();
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            for (int i = 0; i < numSubtasks; i++) {
                mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, i);
            }

            verifyAllSplitsHaveBeenAssigned(context.getSplitsAssignmentSequence(), initialStream);

            int assignmentsBeforeUpdate = context.getSplitsAssignmentSequence().size();

            metadataService.setKafkaStreams(Collections.singleton(expandedStream));
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            Map<String, Map<String, Integer>> expectedPartitionsByClusterAndTopic = new HashMap<>();
            Map<String, Integer> expectedTopicPartitions = new HashMap<>();
            expectedTopicPartitions.put(TOPIC, NUM_SPLITS_PER_CLUSTER);
            expectedTopicPartitions.put(expandedTopic, expandedTopicPartitions);
            expectedPartitionsByClusterAndTopic.put(
                    DynamicKafkaSourceTestHelper.getKafkaClusterId(0),
                    new HashMap<>(expectedTopicPartitions));
            expectedPartitionsByClusterAndTopic.put(
                    DynamicKafkaSourceTestHelper.getKafkaClusterId(1),
                    new HashMap<>(expectedTopicPartitions));
            verifyExpectedTopicPartitions(
                    context.getSplitsAssignmentSequence(), expectedPartitionsByClusterAndTopic);

            List<SplitsAssignment<DynamicKafkaSourceSplit>> incrementalAssignments =
                    context.getSplitsAssignmentSequence()
                            .subList(
                                    assignmentsBeforeUpdate,
                                    context.getSplitsAssignmentSequence().size());
            assertThat(incrementalAssignments)
                    .as("metadata expansion should produce new split assignments")
                    .isNotEmpty();
            assertAssignmentsBalanced(incrementalAssignments, numSubtasks);
        }
    }

    @Test
    public void testGlobalModeClusterAndTopicShrinkThenRescaleRecovery() throws Throwable {
        final int initialParallelism = 8;
        final int restoredParallelism = 8;
        final String expandedTopic = TOPIC + "_shrink_expand";
        final int expandedTopicPartitions = NUM_SPLITS_PER_CLUSTER + 1;

        DynamicKafkaSourceTestHelper.createTopic(expandedTopic, expandedTopicPartitions, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                expandedTopic, expandedTopicPartitions, NUM_RECORDS_PER_SPLIT);

        KafkaStream shrunkStream =
                new KafkaStream(
                        TOPIC, DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC));

        Map<String, ClusterMetadata> expandedClusterMetadata = new HashMap<>();
        expandedClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC, expandedTopic));
        expandedClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(1, TOPIC, expandedTopic));
        KafkaStream expandedStream = new KafkaStream(TOPIC, expandedClusterMetadata);

        DynamicKafkaSourceEnumState shrunkCheckpoint;
        MockKafkaMetadataService metadataService =
                new MockKafkaMetadataService(Collections.singleton(expandedStream));
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(initialParallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                properties -> {
                                    properties.setProperty(
                                            DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                                            DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL
                                                    .name()
                                                    .toLowerCase());
                                    properties.setProperty(
                                            DynamicKafkaSourceOptions
                                                    .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                    .key(),
                                            "1");
                                })) {
            enumerator.start();
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            for (int i = 0; i < initialParallelism; i++) {
                mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, i);
            }

            Map<String, Map<String, Integer>> expandedExpectedPartitions = new HashMap<>();
            Map<String, Integer> expandedExpectedTopicPartitions = new HashMap<>();
            expandedExpectedTopicPartitions.put(TOPIC, NUM_SPLITS_PER_CLUSTER);
            expandedExpectedTopicPartitions.put(expandedTopic, expandedTopicPartitions);
            expandedExpectedPartitions.put(
                    DynamicKafkaSourceTestHelper.getKafkaClusterId(0),
                    new HashMap<>(expandedExpectedTopicPartitions));
            expandedExpectedPartitions.put(
                    DynamicKafkaSourceTestHelper.getKafkaClusterId(1),
                    new HashMap<>(expandedExpectedTopicPartitions));
            verifyExpectedTopicPartitions(
                    context.getSplitsAssignmentSequence(), expandedExpectedPartitions);

            Map<Integer, Set<String>> fullAssignmentsBeforeShrink =
                    getReaderAssignmentsBySplitId(
                            context.getSplitsAssignmentSequence(), initialParallelism);

            int assignmentsBeforeShrink = context.getSplitsAssignmentSequence().size();
            metadataService.setKafkaStreams(Collections.singleton(shrunkStream));
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            assertThat(context.getSplitsAssignmentSequence().size())
                    .as("metadata shrink should not create new split assignments")
                    .isEqualTo(assignmentsBeforeShrink);

            shrunkCheckpoint = enumerator.snapshotState(-1);
            Map<String, KafkaSourceEnumState> shrunkClusterStates =
                    shrunkCheckpoint.getClusterEnumeratorStates();
            String cluster0 = DynamicKafkaSourceTestHelper.getKafkaClusterId(0);
            String cluster1 = DynamicKafkaSourceTestHelper.getKafkaClusterId(1);
            assertThat(shrunkClusterStates)
                    .as("checkpoint should only retain the still-active cluster")
                    .containsOnlyKeys(cluster0);
            assertThat(shrunkClusterStates.get(cluster0).assignedSplits())
                    .as("only active topic partitions should remain after shrink")
                    .hasSize(NUM_SPLITS_PER_CLUSTER)
                    .allSatisfy(
                            split ->
                                    assertThat(split.getTopic())
                                            .as("removed topics should not remain in checkpoint")
                                            .isEqualTo(TOPIC));
            assertThat(shrunkClusterStates).doesNotContainKey(cluster1);

            Set<String> activeSplitIdsAfterShrink =
                    shrunkClusterStates.get(cluster0).assignedSplits().stream()
                            .map(split -> dynamicSplitId(cluster0, split))
                            .collect(Collectors.toSet());
            Map<Integer, Set<String>> activeAssignmentsAfterShrink =
                    retainOnlyActiveSplits(fullAssignmentsBeforeShrink, activeSplitIdsAfterShrink);
            long idleReadersAfterShrink =
                    activeAssignmentsAfterShrink.values().stream().filter(Set::isEmpty).count();
            assertThat(idleReadersAfterShrink)
                    .as("shrink should leave reader holes without rebalancing existing assignments")
                    .isGreaterThanOrEqualTo(initialParallelism - NUM_SPLITS_PER_CLUSTER);
        }

        MockKafkaMetadataService restoredMetadataService =
                new MockKafkaMetadataService(Collections.singleton(shrunkStream));
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(restoredParallelism);
                DynamicKafkaSourceEnumerator restoredEnumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                restoredMetadataService,
                                restoredContext,
                                OffsetsInitializer.earliest(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                shrunkCheckpoint,
                                new TestKafkaEnumContextProxyFactory())) {
            restoredEnumerator.start();
            for (int i = 0; i < restoredParallelism; i++) {
                mockRegisterReaderAndSendReaderStartupEvent(restoredContext, restoredEnumerator, i);
            }

            restoredContext.runPeriodicCallable(0);
            runAllOneTimeCallables(restoredContext);
            int assignmentsBeforeRegrowth = restoredContext.getSplitsAssignmentSequence().size();

            restoredMetadataService.setKafkaStreams(Collections.singleton(expandedStream));
            restoredContext.runPeriodicCallable(0);
            runAllOneTimeCallables(restoredContext);

            List<SplitsAssignment<DynamicKafkaSourceSplit>> incrementalAssignments =
                    restoredContext
                            .getSplitsAssignmentSequence()
                            .subList(
                                    assignmentsBeforeRegrowth,
                                    restoredContext.getSplitsAssignmentSequence().size());
            assertThat(incrementalAssignments)
                    .as("regrowth should assign only newly reintroduced cluster/topic splits")
                    .isNotEmpty();
            assertAssignmentsBalanced(incrementalAssignments, restoredParallelism);

            DynamicKafkaSourceEnumState restoredSnapshot = restoredEnumerator.snapshotState(-1);
            String cluster0 = DynamicKafkaSourceTestHelper.getKafkaClusterId(0);
            String cluster1 = DynamicKafkaSourceTestHelper.getKafkaClusterId(1);
            assertThat(restoredSnapshot.getClusterEnumeratorStates())
                    .as("regrowth should restore both clusters in enumerator state")
                    .containsKeys(cluster0, cluster1);

            for (String clusterId : ImmutableSet.of(cluster0, cluster1)) {
                KafkaSourceEnumState clusterState =
                        restoredSnapshot.getClusterEnumeratorStates().get(clusterId);
                assertThat(clusterState.assignedSplits())
                        .as("each cluster should track expected assignments after regrowth")
                        .hasSize(NUM_SPLITS_PER_CLUSTER + expandedTopicPartitions);

                Set<TopicPartition> assignedTopicPartitions =
                        clusterState.assignedSplits().stream()
                                .map(KafkaPartitionSplit::getTopicPartition)
                                .collect(Collectors.toSet());
                for (int partition = 0; partition < NUM_SPLITS_PER_CLUSTER; partition++) {
                    assertThat(assignedTopicPartitions)
                            .contains(new TopicPartition(TOPIC, partition));
                }
                for (int partition = 0; partition < expandedTopicPartitions; partition++) {
                    assertThat(assignedTopicPartitions)
                            .contains(new TopicPartition(expandedTopic, partition));
                }
            }
        }
    }

    @Test
    public void testGlobalModeShrinkRestoreAndRuntimeRepartitionAfterDownscale() throws Throwable {
        // Expect runtime repartition to rebalance 10 active splits over p=4 as 3,3,2,2.
        runGlobalModeShrinkRestoreAndRuntimeRepartitionScenario(
                "downscale", 4, List.of(3, 3, 2, 2));
    }

    @Test
    public void testGlobalModeShrinkRestoreAndRuntimeRepartitionAfterUpscale() throws Throwable {
        // Expect runtime repartition to rebalance 10 active splits over p=8 as 2,2,1,1,1,1,1,1.
        runGlobalModeShrinkRestoreAndRuntimeRepartitionScenario(
                "upscale", 8, List.of(2, 2, 1, 1, 1, 1, 1, 1));
    }

    private void runGlobalModeShrinkRestoreAndRuntimeRepartitionScenario(
            String scenarioName, int restoredParallelism, List<Integer> expectedRepartitionCounts)
            throws Throwable {
        // Scenario summary:
        // 1) Start in global mode with 13 splits over p=5 and verify near-balance.
        // 2) Shrink metadata so active splits become 10 and verify no immediate rebalance.
        // 3) Simulate runtime restore repartition for new parallelism and verify balanced counts.
        // 4) Restore enumerator from checkpoint and regrow metadata to verify incremental
        // assignment.
        final int initialParallelism = 5;
        final String removedTopic = TOPIC + "_removed3_" + scenarioName;
        final String retainedTopic = TOPIC + "_retained4_" + scenarioName;

        DynamicKafkaSourceTestHelper.createTopic(removedTopic, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                removedTopic, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
        DynamicKafkaSourceTestHelper.createTopic(retainedTopic, NUM_SPLITS_PER_CLUSTER + 1, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                retainedTopic, NUM_SPLITS_PER_CLUSTER + 1, NUM_RECORDS_PER_SPLIT);

        Map<String, ClusterMetadata> initialClusterMetadata = new HashMap<>();
        initialClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC, removedTopic));
        initialClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(1, TOPIC, retainedTopic));
        KafkaStream initialStream = new KafkaStream(TOPIC, initialClusterMetadata);

        Map<String, ClusterMetadata> shrunkClusterMetadata = new HashMap<>();
        shrunkClusterMetadata.putAll(DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC));
        shrunkClusterMetadata.putAll(
                DynamicKafkaSourceTestHelper.getClusterMetadataMap(1, TOPIC, retainedTopic));
        KafkaStream shrunkStream = new KafkaStream(TOPIC, shrunkClusterMetadata);

        DynamicKafkaSourceEnumState shrunkCheckpoint;
        MockKafkaMetadataService metadataService =
                new MockKafkaMetadataService(Collections.singleton(initialStream));
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(initialParallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                properties -> {
                                    properties.setProperty(
                                            DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                                            DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL
                                                    .name()
                                                    .toLowerCase());
                                    properties.setProperty(
                                            DynamicKafkaSourceOptions
                                                    .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                    .key(),
                                            "1");
                                })) {
            enumerator.start();
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            for (int i = 0; i < initialParallelism; i++) {
                mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, i);
            }

            Map<String, Map<String, Integer>> expectedInitialPartitions = new HashMap<>();
            Map<String, Integer> cluster0TopicPartitions = new HashMap<>();
            cluster0TopicPartitions.put(TOPIC, NUM_SPLITS_PER_CLUSTER);
            cluster0TopicPartitions.put(removedTopic, NUM_SPLITS_PER_CLUSTER);
            expectedInitialPartitions.put(
                    DynamicKafkaSourceTestHelper.getKafkaClusterId(0), cluster0TopicPartitions);
            Map<String, Integer> cluster1TopicPartitions = new HashMap<>();
            cluster1TopicPartitions.put(TOPIC, NUM_SPLITS_PER_CLUSTER);
            cluster1TopicPartitions.put(retainedTopic, NUM_SPLITS_PER_CLUSTER + 1);
            expectedInitialPartitions.put(
                    DynamicKafkaSourceTestHelper.getKafkaClusterId(1), cluster1TopicPartitions);
            verifyExpectedTopicPartitions(
                    context.getSplitsAssignmentSequence(), expectedInitialPartitions);

            Map<Integer, Set<String>> fullAssignmentsBeforeShrink =
                    getReaderAssignmentsBySplitId(
                            context.getSplitsAssignmentSequence(), initialParallelism);
            Map<Integer, Integer> countsBeforeShrink =
                    getReaderSplitCounts(fullAssignmentsBeforeShrink, initialParallelism);
            assertThat(
                            Collections.max(countsBeforeShrink.values())
                                    - Collections.min(countsBeforeShrink.values()))
                    .as("initial global assignment should be near-balanced")
                    .isLessThanOrEqualTo(1);

            int assignmentsBeforeShrink = context.getSplitsAssignmentSequence().size();
            metadataService.setKafkaStreams(Collections.singleton(shrunkStream));
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            assertThat(context.getSplitsAssignmentSequence().size())
                    .as("metadata shrink should not create new split assignments")
                    .isEqualTo(assignmentsBeforeShrink);

            shrunkCheckpoint = enumerator.snapshotState(-1);
            Set<String> activeSplitIdsAfterShrink =
                    shrunkCheckpoint.getClusterEnumeratorStates().entrySet().stream()
                            .flatMap(
                                    entry ->
                                            entry.getValue().assignedSplits().stream()
                                                    .map(
                                                            split ->
                                                                    dynamicSplitId(
                                                                            entry.getKey(), split)))
                            .collect(Collectors.toSet());
            assertThat(activeSplitIdsAfterShrink)
                    .as("shrink scenario keeps exactly 10 active splits")
                    .hasSize(10);

            Map<Integer, Set<String>> activeAssignmentsAfterShrink =
                    retainOnlyActiveSplits(fullAssignmentsBeforeShrink, activeSplitIdsAfterShrink);
            Map<Integer, Integer> countsAfterShrink =
                    getReaderSplitCounts(activeAssignmentsAfterShrink, initialParallelism);

            List<Integer> countsAfterRuntimeRepartition =
                    repartitionReaderStateCountsForRestore(countsAfterShrink, restoredParallelism);
            assertThat(countsAfterRuntimeRepartition)
                    .as("runtime repartition on restore should match expected balanced counts")
                    .containsExactlyElementsOf(expectedRepartitionCounts);
            assertThat(
                            Collections.max(countsAfterRuntimeRepartition)
                                    - Collections.min(countsAfterRuntimeRepartition))
                    .as("runtime repartition keeps split-count skew within 1")
                    .isLessThanOrEqualTo(1);
        }

        MockKafkaMetadataService restoredMetadataService =
                new MockKafkaMetadataService(Collections.singleton(shrunkStream));
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(restoredParallelism);
                DynamicKafkaSourceEnumerator restoredEnumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                restoredMetadataService,
                                restoredContext,
                                OffsetsInitializer.earliest(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                shrunkCheckpoint,
                                new TestKafkaEnumContextProxyFactory())) {
            restoredEnumerator.start();
            for (int i = 0; i < restoredParallelism; i++) {
                mockRegisterReaderAndSendReaderStartupEvent(restoredContext, restoredEnumerator, i);
            }

            restoredContext.runPeriodicCallable(0);
            runAllOneTimeCallables(restoredContext);
            int assignmentsBeforeRegrowth = restoredContext.getSplitsAssignmentSequence().size();

            restoredMetadataService.setKafkaStreams(Collections.singleton(initialStream));
            restoredContext.runPeriodicCallable(0);
            runAllOneTimeCallables(restoredContext);

            List<SplitsAssignment<DynamicKafkaSourceSplit>> incrementalAssignments =
                    restoredContext
                            .getSplitsAssignmentSequence()
                            .subList(
                                    assignmentsBeforeRegrowth,
                                    restoredContext.getSplitsAssignmentSequence().size());
            assertThat(incrementalAssignments)
                    .as("regrowth should assign only newly reintroduced splits")
                    .isNotEmpty();
            assertAssignmentsBalanced(incrementalAssignments, restoredParallelism);
        }
    }

    @Test
    public void testEnumeratorDoesNotAssignDuplicateSplitsInMetadataUpdate() throws Throwable {
        KafkaStream kafkaStreamWithOneCluster = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);
        kafkaStreamWithOneCluster
                .getClusterMetadataMap()
                .remove(DynamicKafkaSourceTestHelper.getKafkaClusterId(1));

        KafkaStream kafkaStreamWithTwoClusters = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);

        MockKafkaMetadataService mockKafkaMetadataService =
                new MockKafkaMetadataService(Collections.singleton(kafkaStreamWithOneCluster));

        Properties properties = new Properties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                                mockKafkaMetadataService,
                                context,
                                OffsetsInitializer.committedOffsets(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new DynamicKafkaSourceEnumState(),
                                new TestKafkaEnumContextProxyFactory())) {

            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            enumerator.start();

            // run all discovery
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), kafkaStreamWithOneCluster);

            // trigger metadata change
            mockKafkaMetadataService.setKafkaStreams(
                    Collections.singleton(kafkaStreamWithTwoClusters));
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(), kafkaStreamWithTwoClusters);

            Map<String, Integer> splitAssignmentFrequencyMap = new HashMap<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> splitsAssignmentStep :
                    context.getSplitsAssignmentSequence()) {
                for (List<DynamicKafkaSourceSplit> assignments :
                        splitsAssignmentStep.assignment().values()) {
                    for (DynamicKafkaSourceSplit assignment : assignments) {
                        splitAssignmentFrequencyMap.put(
                                assignment.splitId(),
                                splitAssignmentFrequencyMap.getOrDefault(assignment.splitId(), 0)
                                        + 1);
                    }
                }
            }

            assertThat(splitAssignmentFrequencyMap.values())
                    .as("all splits should have been assigned once")
                    .allMatch(count -> count == 1);
        }
    }

    @Test
    public void testInitExceptionNonexistingKafkaCluster() {
        Properties fakeProperties = new Properties();
        fakeProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "fake-cluster:8080");
        MockKafkaMetadataService mockKafkaMetadataServiceWithUnavailableCluster =
                new MockKafkaMetadataService(
                        ImmutableSet.of(
                                DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC),
                                new KafkaStream(
                                        "fake-stream",
                                        Collections.singletonMap(
                                                "fake-cluster",
                                                new ClusterMetadata(
                                                        Collections.singleton("fake-topic"),
                                                        fakeProperties)))));
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, mockKafkaMetadataServiceWithUnavailableCluster)) {
            enumerator.start();

            runAllOneTimeCallables(context);
        } catch (Throwable throwable) {
            assertThat(throwable).hasRootCauseInstanceOf(KafkaException.class);
        }
    }

    @Test
    public void testEnumeratorErrorPropagation() {
        Properties fakeProperties = new Properties();
        fakeProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "fake-cluster:8080");
        KafkaStream fakeStream =
                new KafkaStream(
                        "fake-stream",
                        Collections.singletonMap(
                                "fake-cluster",
                                new ClusterMetadata(
                                        Collections.singleton("fake-topic"), fakeProperties)));

        MockKafkaMetadataService mockKafkaMetadataServiceWithUnavailableCluster =
                new MockKafkaMetadataService(
                        ImmutableSet.of(
                                DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC), fakeStream));
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(context, mockKafkaMetadataServiceWithUnavailableCluster)) {
            enumerator.start();

            runAllOneTimeCallables(context);
        } catch (Throwable throwable) {
            assertThat(throwable).hasRootCauseInstanceOf(KafkaException.class);
        }
    }

    private DynamicKafkaSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context) {
        return createEnumerator(
                context,
                new MockKafkaMetadataService(
                        Collections.singleton(DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC))),
                (properties) -> {});
    }

    private DynamicKafkaSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            KafkaMetadataService kafkaMetadataService) {
        return createEnumerator(context, kafkaMetadataService, (properties) -> {});
    }

    private DynamicKafkaSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            Consumer<Properties> applyPropertiesConsumer) {
        return createEnumerator(
                context,
                new MockKafkaMetadataService(
                        Collections.singleton(DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC))),
                applyPropertiesConsumer);
    }

    private DynamicKafkaSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            KafkaMetadataService kafkaMetadataService,
            Consumer<Properties> applyPropertiesConsumer) {
        Properties properties = new Properties();
        applyPropertiesConsumer.accept(properties);
        properties.putIfAbsent(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.putIfAbsent(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
        return new DynamicKafkaSourceEnumerator(
                new KafkaStreamSetSubscriber(Collections.singleton(TOPIC)),
                kafkaMetadataService,
                context,
                OffsetsInitializer.earliest(),
                new NoStoppingOffsetsInitializer(),
                properties,
                Boundedness.CONTINUOUS_UNBOUNDED,
                new DynamicKafkaSourceEnumState(),
                new TestKafkaEnumContextProxyFactory());
    }

    private void mockRegisterReaderAndSendReaderStartupEvent(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            DynamicKafkaSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location " + reader));
        enumerator.addReader(reader);
        // readers send source event at startup
        enumerator.handleSourceEvent(reader, new GetMetadataUpdateEvent());
    }

    private void verifyAllSplitsHaveBeenAssigned(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> splitsAssignmentSequence,
            KafkaStream kafkaStream) {
        Map<String, Set<String>> clusterTopicMap = new HashMap<>();
        for (Entry<String, ClusterMetadata> entry :
                kafkaStream.getClusterMetadataMap().entrySet()) {
            clusterTopicMap
                    .computeIfAbsent(entry.getKey(), unused -> new HashSet<>())
                    .addAll(entry.getValue().getTopics());
        }

        Set<DynamicKafkaSourceSplit> splitsAssigned =
                splitsAssignmentSequence.stream()
                        .flatMap(
                                splitsAssignment ->
                                        splitsAssignment.assignment().values().stream()
                                                .flatMap(Collection::stream))
                        .collect(Collectors.toSet());

        assertThat(splitsAssignmentSequence).isNotEmpty();

        Map<String, Set<TopicPartition>> clusterToTopicPartition = new HashMap<>();
        for (SplitsAssignment<DynamicKafkaSourceSplit> split : splitsAssignmentSequence) {
            for (Entry<Integer, List<DynamicKafkaSourceSplit>> assignments :
                    split.assignment().entrySet()) {
                for (DynamicKafkaSourceSplit assignment : assignments.getValue()) {
                    clusterToTopicPartition
                            .computeIfAbsent(assignment.getKafkaClusterId(), key -> new HashSet<>())
                            .add(assignment.getKafkaPartitionSplit().getTopicPartition());
                }
            }
        }

        assertThat(splitsAssigned)
                .hasSize(NUM_SPLITS_PER_CLUSTER * clusterTopicMap.keySet().size());

        // verify correct clusters
        for (String kafkaClusterId : clusterTopicMap.keySet()) {
            assertThat(clusterToTopicPartition)
                    .as("All Kafka clusters must be assigned in the splits.")
                    .containsKey(kafkaClusterId);
        }

        // verify topic partitions
        Set<TopicPartition> assignedTopicPartitionSet =
                clusterToTopicPartition.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        for (Set<String> topics : clusterTopicMap.values()) {
            for (String topic : topics) {
                Set<TopicPartition> expectedTopicPartitions = new HashSet<>();
                for (int i = 0; i < NUM_SPLITS_PER_CLUSTER; i++) {
                    expectedTopicPartitions.add(new TopicPartition(topic, i));
                }
                assertThat(assignedTopicPartitionSet)
                        .as("splits must contain all topics and 2 partitions per topic")
                        .containsExactlyInAnyOrderElementsOf(expectedTopicPartitions);
            }
        }
    }

    private Map<Integer, Set<DynamicKafkaSourceSplit>> getReaderAssignments(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context) {
        Map<Integer, Set<DynamicKafkaSourceSplit>> readerToSplits = new HashMap<>();
        for (SplitsAssignment<DynamicKafkaSourceSplit> split :
                context.getSplitsAssignmentSequence()) {
            for (Entry<Integer, List<DynamicKafkaSourceSplit>> assignments :
                    split.assignment().entrySet()) {
                readerToSplits
                        .computeIfAbsent(assignments.getKey(), key -> new HashSet<>())
                        .addAll(assignments.getValue());
            }
        }
        return readerToSplits;
    }

    private Map<Integer, Set<String>> getReaderAssignmentsBySplitId(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> splitsAssignmentSequence,
            int numReaders) {
        Map<Integer, Set<String>> readerToSplitIds = new HashMap<>();
        for (int readerId = 0; readerId < numReaders; readerId++) {
            readerToSplitIds.put(readerId, new HashSet<>());
        }
        for (SplitsAssignment<DynamicKafkaSourceSplit> split : splitsAssignmentSequence) {
            for (Entry<Integer, List<DynamicKafkaSourceSplit>> assignments :
                    split.assignment().entrySet()) {
                readerToSplitIds
                        .computeIfAbsent(assignments.getKey(), ignored -> new HashSet<>())
                        .addAll(
                                assignments.getValue().stream()
                                        .map(DynamicKafkaSourceSplit::splitId)
                                        .collect(Collectors.toSet()));
            }
        }
        return readerToSplitIds;
    }

    private Map<Integer, Set<String>> retainOnlyActiveSplits(
            Map<Integer, Set<String>> readerToSplitIds, Set<String> activeSplitIds) {
        Map<Integer, Set<String>> filtered = new HashMap<>();
        for (Entry<Integer, Set<String>> entry : readerToSplitIds.entrySet()) {
            Set<String> activeForReader =
                    entry.getValue().stream()
                            .filter(activeSplitIds::contains)
                            .collect(Collectors.toCollection(HashSet::new));
            filtered.put(entry.getKey(), activeForReader);
        }
        return filtered;
    }

    private static String dynamicSplitId(String clusterId, KafkaPartitionSplit split) {
        return clusterId + "-" + split.splitId();
    }

    private void verifyExpectedTopicPartitions(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> splitsAssignmentSequence,
            Map<String, Map<String, Integer>> expectedPartitionsByClusterAndTopic) {
        Map<String, Map<String, Set<Integer>>> observedPartitionsByClusterAndTopic =
                new HashMap<>();
        for (SplitsAssignment<DynamicKafkaSourceSplit> split : splitsAssignmentSequence) {
            for (Entry<Integer, List<DynamicKafkaSourceSplit>> assignments :
                    split.assignment().entrySet()) {
                for (DynamicKafkaSourceSplit assignment : assignments.getValue()) {
                    observedPartitionsByClusterAndTopic
                            .computeIfAbsent(
                                    assignment.getKafkaClusterId(), ignored -> new HashMap<>())
                            .computeIfAbsent(
                                    assignment.getKafkaPartitionSplit().getTopic(),
                                    ignored -> new HashSet<>())
                            .add(assignment.getKafkaPartitionSplit().getPartition());
                }
            }
        }

        for (Entry<String, Map<String, Integer>> clusterExpected :
                expectedPartitionsByClusterAndTopic.entrySet()) {
            assertThat(observedPartitionsByClusterAndTopic)
                    .as("expected cluster must have assigned splits")
                    .containsKey(clusterExpected.getKey());
            for (Entry<String, Integer> topicExpected : clusterExpected.getValue().entrySet()) {
                Set<Integer> observedTopicPartitions =
                        observedPartitionsByClusterAndTopic
                                .get(clusterExpected.getKey())
                                .get(topicExpected.getKey());
                assertThat(observedTopicPartitions)
                        .as("expected topic must have assigned splits")
                        .isNotNull();
                assertThat(observedTopicPartitions).hasSize(topicExpected.getValue());
                for (int partition = 0; partition < topicExpected.getValue(); partition++) {
                    assertThat(observedTopicPartitions).contains(partition);
                }
            }
        }
    }

    private void assertAssignmentsBalanced(
            List<SplitsAssignment<DynamicKafkaSourceSplit>> assignmentSequence, int numReaders) {
        Map<Integer, Integer> assignedSplitCountByReader = new HashMap<>();
        for (int readerId = 0; readerId < numReaders; readerId++) {
            assignedSplitCountByReader.put(readerId, 0);
        }
        for (SplitsAssignment<DynamicKafkaSourceSplit> assignment : assignmentSequence) {
            for (Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                    assignment.assignment().entrySet()) {
                assignedSplitCountByReader.merge(
                        entry.getKey(), entry.getValue().size(), Integer::sum);
            }
        }

        int minAssignedSplits = Collections.min(assignedSplitCountByReader.values());
        int maxAssignedSplits = Collections.max(assignedSplitCountByReader.values());
        assertThat(maxAssignedSplits - minAssignedSplits)
                .as("global mode should keep per-reader split counts balanced")
                .isLessThanOrEqualTo(1);
    }

    private Map<Integer, Integer> getReaderSplitCounts(
            Map<Integer, Set<String>> assignmentsByReader, int numReaders) {
        Map<Integer, Integer> splitCounts = new HashMap<>();
        for (int readerId = 0; readerId < numReaders; readerId++) {
            splitCounts.put(readerId, 0);
        }
        assignmentsByReader.forEach(
                (readerId, splitIds) -> splitCounts.put(readerId, splitIds.size()));
        return splitCounts;
    }

    private List<Integer> repartitionReaderStateCountsForRestore(
            Map<Integer, Integer> splitCountsByOldSubtask, int restoredParallelism) {
        final int oldParallelism = splitCountsByOldSubtask.size();
        List<List<OperatorStateHandle>> previousState = new ArrayList<>();

        long offsetSeed = 0L;
        for (int subtask = 0; subtask < oldParallelism; subtask++) {
            int splitCount = splitCountsByOldSubtask.getOrDefault(subtask, 0);
            long[] offsets = new long[splitCount];
            for (int i = 0; i < splitCount; i++) {
                offsets[i] = offsetSeed++;
            }

            Map<String, OperatorStateHandle.StateMetaInfo> stateNameToMeta = new HashMap<>();
            stateNameToMeta.put(
                    SOURCE_READER_SPLIT_STATE_NAME,
                    new OperatorStateHandle.StateMetaInfo(
                            offsets, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));

            previousState.add(
                    Collections.singletonList(
                            new OperatorStreamStateHandle(
                                    stateNameToMeta,
                                    new ByteStreamStateHandle(
                                            "reader-state-" + subtask, new byte[0]))));
        }

        List<List<OperatorStateHandle>> repartitionedState =
                RoundRobinOperatorStateRepartitioner.INSTANCE.repartitionState(
                        previousState, oldParallelism, restoredParallelism);

        List<Integer> splitCountsAfterRepartition = new ArrayList<>(repartitionedState.size());
        for (List<OperatorStateHandle> subtaskStateHandles : repartitionedState) {
            int splitCount = 0;
            for (OperatorStateHandle stateHandle : subtaskStateHandles) {
                splitCount +=
                        stateHandle.getStateNameToPartitionOffsets().entrySet().stream()
                                .filter(
                                        entry ->
                                                SOURCE_READER_SPLIT_STATE_NAME.equals(
                                                        entry.getKey()))
                                .mapToInt(entry -> entry.getValue().getOffsets().length)
                                .sum();
            }
            splitCountsAfterRepartition.add(splitCount);
        }
        return splitCountsAfterRepartition;
    }

    private List<TopicPartition> getFilteredTopicPartitions(
            DynamicKafkaSourceEnumState state, String topic, AssignmentStatus assignmentStatus) {
        return state.getClusterEnumeratorStates().values().stream()
                .flatMap(s -> s.splits().stream())
                .filter(
                        partition ->
                                partition.split().getTopic().equals(topic)
                                        && partition.assignmentStatus() == assignmentStatus)
                .map(
                        splitAndAssignmentStatus ->
                                splitAndAssignmentStatus.split().getTopicPartition())
                .collect(Collectors.toList());
    }

    private static void runAllOneTimeCallables(MockSplitEnumeratorContext context)
            throws Throwable {
        while (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private DynamicKafkaSourceEnumState getCheckpointState(KafkaStream kafkaStream)
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                (properties) -> {})) {
            enumerator.start();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(context.getSplitsAssignmentSequence(), kafkaStream);

            return enumerator.snapshotState(-1);
        }
    }

    private DynamicKafkaSourceEnumState getCheckpointState() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator = createEnumerator(context)) {
            enumerator.start();

            // initialize all readers
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            runAllOneTimeCallables(context);

            verifyAllSplitsHaveBeenAssigned(
                    context.getSplitsAssignmentSequence(),
                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC));

            return enumerator.snapshotState(-1);
        }
    }

    private static class TestKafkaEnumContextProxyFactory
            implements StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory {

        @Override
        public StoppableKafkaEnumContextProxy create(
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                Runnable signalNoMoreSplitsCallback) {
            return new TestKafkaEnumContextProxy(
                    kafkaClusterId,
                    kafkaMetadataService,
                    (MockSplitEnumeratorContext<DynamicKafkaSourceSplit>) enumContext);
        }
    }

    private static class TestKafkaEnumContextProxy extends StoppableKafkaEnumContextProxy {

        private final SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext;

        public TestKafkaEnumContextProxy(
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext) {
            super(kafkaClusterId, kafkaMetadataService, enumContext, null);
            this.enumContext = enumContext;
        }

        /**
         * Schedule periodic callables under the coordinator executor, so we can use {@link
         * MockSplitEnumeratorContext} to invoke the callable (split assignment) on demand to test
         * the integration of KafkaSourceEnumerator.
         */
        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {
            enumContext.callAsync(
                    wrapCallAsyncCallable(callable),
                    wrapCallAsyncCallableHandler(handler),
                    initialDelay,
                    period);
        }
    }
}
