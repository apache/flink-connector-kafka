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
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.SplitAndAssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.metadata.TopicIntegrityException;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the topic integrity check of the {@link DynamicKafkaSourceEnumerator}. */
public class DynamicKafkaSourceEnumeratorTopicIntegrityTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC = "DynamicKafkaSourceEnumeratorTopicIntegrityTest";
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
    public void testTrackedTopicIdsSurviveMetadataChangeAndDetectRecreation() throws Throwable {
        final String topic2 = TOPIC + "_integrity";

        DynamicKafkaSourceTestHelper.createTopic(topic2, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                topic2, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
        final String clusterId = DynamicKafkaSourceTestHelper.getKafkaClusterId(0);

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

        final DynamicKafkaSourceEnumState stateAfterMetadataChange;
        final Map<String, String> initialTrackedIds;
        try (MockKafkaMetadataService metadataService =
                        new MockKafkaMetadataService(initialStreams);
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                DynamicKafkaSourceEnumeratorTopicIntegrityTest
                                        ::enableTopicIntegrityCheck)) {
            enumerator.start();
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);

            initialTrackedIds = getTrackedTopicIds(enumerator.snapshotState(-1), clusterId);
            assertThat(initialTrackedIds).containsOnlyKeys(TOPIC, topic2);

            metadataService.setKafkaStreams(updatedStreams);
            context.runPeriodicCallable(0);

            // the restarted enumerators have not rediscovered the topics yet, so the ids can only
            // come from the state carried over the metadata change
            assertThat(getTrackedTopicIds(enumerator.snapshotState(-1), clusterId))
                    .containsOnly(Map.entry(TOPIC, initialTrackedIds.get(TOPIC)));

            // the rediscovery agrees with the carried over id, so the check passes
            runAllOneTimeCallables(context);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);

            stateAfterMetadataChange = enumerator.snapshotState(-1);
            assertThat(getTrackedTopicIds(stateAfterMetadataChange, clusterId))
                    .containsOnly(Map.entry(TOPIC, initialTrackedIds.get(TOPIC)));
        }

        // a stale id in the checkpoint is what a topic recreated after the checkpoint looks like
        final KafkaSourceEnumState clusterState =
                stateAfterMetadataChange.getClusterEnumeratorStates().get(clusterId);
        final String staleTopicId = Uuid.randomUuid().toString();
        final DynamicKafkaSourceEnumState staleState =
                withClusterState(
                        stateAfterMetadataChange,
                        clusterId,
                        new KafkaSourceEnumState(
                                clusterState.splits(),
                                clusterState.initialDiscoveryFinished(),
                                Collections.singletonMap(TOPIC, staleTopicId)));

        // topic2 is back in the stream, so the restored enumerators are restarted on the first
        // metadata refresh before they run their first discovery
        try (MockKafkaMetadataService metadataService =
                        new MockKafkaMetadataService(initialStreams);
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                DynamicKafkaSourceEnumeratorTopicIntegrityTest
                                        ::enableTopicIntegrityCheck,
                                staleState)) {
            enumerator.start();
            context.runPeriodicCallable(0);

            assertThat(getTrackedTopicIds(enumerator.snapshotState(-1), clusterId))
                    .containsOnly(Map.entry(TOPIC, staleTopicId));
            assertThatThrownBy(() -> runAllOneTimeCallables(context))
                    .hasRootCauseInstanceOf(TopicIntegrityException.class)
                    .hasRootCauseMessage("Topic " + TOPIC + " was recreated");
        }
    }

    @Test
    public void testRestoreKeepsTrackedTopicIdsOfTopicsWithUnassignedSplits() throws Throwable {
        final String topic2 = TOPIC + "_unassigned";
        // a checkpoint taken before the splits of topic2 were handed to a reader
        assertRestoreKeepsTrackedTopicIds(
                topic2,
                splits ->
                        splits.stream()
                                .map(
                                        splitStatus ->
                                                topic2.equals(splitStatus.split().getTopic())
                                                        ? new SplitAndAssignmentStatus(
                                                                splitStatus.split(),
                                                                AssignmentStatus.UNASSIGNED)
                                                        : splitStatus)
                                .collect(Collectors.toSet()));
    }

    @Test
    public void testRestoreKeepsTrackedTopicIdsOfTopicsWithoutSplits() throws Throwable {
        final String topic2 = TOPIC + "_no_splits";
        // a checkpoint taken after topic2 was described, which records its id, but before its
        // splits were initialized
        assertRestoreKeepsTrackedTopicIds(
                topic2,
                splits ->
                        splits.stream()
                                .filter(
                                        splitStatus ->
                                                !topic2.equals(splitStatus.split().getTopic()))
                                .collect(Collectors.toSet()));
    }

    /**
     * Checkpoints a cluster with {@link #TOPIC} and {@code topic2}, rewrites the splits of the
     * checkpoint with {@code splitsTransform}, restores from it and asserts that the first
     * discovery keeps the tracked ids of both topics and ends up with the splits of {@code topic2}.
     */
    private static void assertRestoreKeepsTrackedTopicIds(
            String topic2, UnaryOperator<Set<SplitAndAssignmentStatus>> splitsTransform)
            throws Throwable {
        DynamicKafkaSourceTestHelper.createTopic(topic2, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                topic2, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
        final String clusterId = DynamicKafkaSourceTestHelper.getKafkaClusterId(0);
        final KafkaStream stream =
                new KafkaStream(
                        TOPIC,
                        DynamicKafkaSourceTestHelper.getClusterMetadataMap(0, TOPIC, topic2));

        final DynamicKafkaSourceEnumState checkpointState =
                getCheckpointStateWithTopicIntegrityCheck(stream);
        final KafkaSourceEnumState clusterState =
                checkpointState.getClusterEnumeratorStates().get(clusterId);
        final Map<String, String> trackedIds = clusterState.trackedTopicIdsByName();
        assertThat(trackedIds).containsOnlyKeys(TOPIC, topic2);

        final DynamicKafkaSourceEnumState restoredState =
                withClusterState(
                        checkpointState,
                        clusterId,
                        new KafkaSourceEnumState(
                                splitsTransform.apply(clusterState.splits()),
                                clusterState.initialDiscoveryFinished(),
                                trackedIds));

        try (MockKafkaMetadataService metadataService =
                        new MockKafkaMetadataService(Collections.singleton(stream));
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                metadataService,
                                DynamicKafkaSourceEnumeratorTopicIntegrityTest
                                        ::enableTopicIntegrityCheck,
                                restoredState)) {
            enumerator.start();
            // the restored sub-enumerator discovers before the first metadata refresh
            runAllOneTimeCallables(context);

            final KafkaSourceEnumState stateAfterDiscovery =
                    enumerator.snapshotState(-1).getClusterEnumeratorStates().get(clusterId);
            assertThat(stateAfterDiscovery.trackedTopicIdsByName())
                    .containsExactlyInAnyOrderEntriesOf(trackedIds);
            assertThat(stateAfterDiscovery.splits())
                    .filteredOn(splitStatus -> topic2.equals(splitStatus.split().getTopic()))
                    .hasSize(NUM_SPLITS_PER_CLUSTER);
        }
    }

    private static void enableTopicIntegrityCheck(Properties properties) {
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(KafkaSourceOptions.TOPIC_INTEGRITY_CHECK_ENABLED.key(), "true");
    }

    private static DynamicKafkaSourceEnumState getCheckpointStateWithTopicIntegrityCheck(
            KafkaStream kafkaStream) throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                DynamicKafkaSourceEnumeratorTopicIntegrityTest
                                        ::enableTopicIntegrityCheck)) {
            enumerator.start();
            context.runPeriodicCallable(0);
            runAllOneTimeCallables(context);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 0);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 1);
            mockRegisterReaderAndSendReaderStartupEvent(context, enumerator, 2);
            return enumerator.snapshotState(-1);
        }
    }

    private static DynamicKafkaSourceEnumState withClusterState(
            DynamicKafkaSourceEnumState state,
            String clusterId,
            KafkaSourceEnumState clusterState) {
        final Map<String, KafkaSourceEnumState> clusterStates =
                new HashMap<>(state.getClusterEnumeratorStates());
        clusterStates.put(clusterId, clusterState);
        return new DynamicKafkaSourceEnumState(state.getKafkaStreams(), clusterStates);
    }

    private static Map<String, String> getTrackedTopicIds(
            DynamicKafkaSourceEnumState state, String clusterId) {
        return state.getClusterEnumeratorStates().get(clusterId).trackedTopicIdsByName();
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            KafkaMetadataService kafkaMetadataService,
            Consumer<Properties> applyPropertiesConsumer) {
        return createEnumerator(
                context,
                kafkaMetadataService,
                applyPropertiesConsumer,
                new DynamicKafkaSourceEnumState());
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            KafkaMetadataService kafkaMetadataService,
            Consumer<Properties> applyPropertiesConsumer,
            DynamicKafkaSourceEnumState restoredState) {
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
                restoredState,
                new DynamicKafkaSourceEnumeratorTest.TestKafkaEnumContextProxyFactory());
    }

    private static void mockRegisterReaderAndSendReaderStartupEvent(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            DynamicKafkaSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location " + reader));
        enumerator.addReader(reader);
        // readers send source event at startup
        enumerator.handleSourceEvent(reader, new GetMetadataUpdateEvent());
    }

    private static void runAllOneTimeCallables(MockSplitEnumeratorContext<?> context)
            throws Throwable {
        while (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }
}
