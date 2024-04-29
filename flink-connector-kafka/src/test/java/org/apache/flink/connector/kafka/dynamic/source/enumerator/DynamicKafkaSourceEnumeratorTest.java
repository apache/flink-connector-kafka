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
import org.apache.flink.connector.kafka.source.enumerator.TopicPartitionAndAssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
                                    .map(subState -> subState.assignedPartitions().stream())
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
                                    .flatMap(enumState -> enumState.assignedPartitions().stream())
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
            assertThat(
                            getFilteredTopicPartitions(
                                    initialState, TOPIC, AssignmentStatus.UNASSIGNED_INITIAL))
                    .hasSize(1);
            assertThat(getFilteredTopicPartitions(initialState, topic2, AssignmentStatus.ASSIGNED))
                    .hasSize(2);
            assertThat(
                            getFilteredTopicPartitions(
                                    initialState, topic2, AssignmentStatus.UNASSIGNED_INITIAL))
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
                                    migratedState, TOPIC, AssignmentStatus.UNASSIGNED_INITIAL))
                    .isEmpty();
            assertThat(getFilteredTopicPartitions(migratedState, topic2, AssignmentStatus.ASSIGNED))
                    .isEmpty();
            assertThat(
                            getFilteredTopicPartitions(
                                    migratedState, topic2, AssignmentStatus.UNASSIGNED_INITIAL))
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

    private List<TopicPartition> getFilteredTopicPartitions(
            DynamicKafkaSourceEnumState state, String topic, AssignmentStatus assignmentStatus) {
        return state.getClusterEnumeratorStates().values().stream()
                .flatMap(s -> s.partitions().stream())
                .filter(
                        partition ->
                                partition.topicPartition().topic().equals(topic)
                                        && partition.assignmentStatus() == assignmentStatus)
                .map(TopicPartitionAndAssignmentStatus::topicPartition)
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
