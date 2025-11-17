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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit.MIGRATED;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaSourceEnumerator}. */
@ResourceLock("KafkaTestBase")
public class KafkaSourceEnumeratorTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
    private static final int NUM_PARTITIONS_DYNAMIC_TOPIC = 4;

    private static final String TOPIC1 = "topic";
    private static final String TOPIC2 = "pattern-topic";

    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final int READER2 = 2;
    private static final Set<String> PRE_EXISTING_TOPICS =
            new HashSet<>(Arrays.asList(TOPIC1, TOPIC2));
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;
    private static final boolean INCLUDE_DYNAMIC_TOPIC = true;
    private static final boolean EXCLUDE_DYNAMIC_TOPIC = false;
    private static KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl retriever;
    private static final Map<TopicPartition, Long> specificOffsets = new HashMap<>();

    @BeforeAll
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        retriever =
                new KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl(
                        KafkaSourceTestEnv.getAdminClient(), KafkaSourceTestEnv.GROUP_ID);
        KafkaSourceTestEnv.setupTopic(TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        KafkaSourceTestEnv.setupTopic(TOPIC2, true, true, KafkaSourceTestEnv::getRecordsForTopic);

        for (Map.Entry<TopicPartition, Long> partitionEnd :
                retriever
                        .endOffsets(KafkaSourceTestEnv.getPartitionsForTopics(PRE_EXISTING_TOPICS))
                        .entrySet()) {
            specificOffsets.put(
                    partitionEnd.getKey(),
                    partitionEnd.getValue() / (partitionEnd.getKey().partition() + 1));
        }
        assertThat(specificOffsets).hasSize(2 * KafkaSourceTestEnv.NUM_PARTITIONS);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testStartWithDiscoverPartitionsOnce() throws Exception {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);

            // enumerator just start noMoreNewPartitionSplits will be false
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isFalse();
        }
    }

    @Test
    public void testStartWithPeriodicPartitionDiscovery() throws Exception {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables())
                    .as("A periodic partition discovery callable should have been scheduled")
                    .hasSize(1);

            // enumerator just start noMoreNewPartitionSplits will be false
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isFalse();
        }
    }

    @Test
    public void testDiscoverPartitionsTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();

            // register reader 0.
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // Verify assignments for reader 0.
            verifyLastReadersAssignments(
                    context, Arrays.asList(READER0, READER1), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testReaderRegistrationTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

            registerReader(context, enumerator, READER1);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 2);
        }
    }

    @Test
    public void testRunWithDiscoverPartitionsOnceToCheckNoMoreSplit() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThat(context.getPeriodicCallables()).isEmpty();
            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // enumerator noMoreNewPartitionSplits first will be false, when execute
            // handlePartitionSplitChanges will be set true
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isTrue();
        }
    }

    @Test
    public void testRunWithPeriodicPartitionDiscoveryOnceToCheckNoMoreSplit() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables())
                    .as("A periodic partition discovery callable should have been scheduled")
                    .hasSize(1);
            // Run the partition discover callable and check the partition assignment.
            runPeriodicPartitionDiscovery(context);

            // enumerator noMoreNewPartitionSplits first will be false, even when execute
            // handlePartitionSplitChanges it still be false
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isFalse();
        }
    }

    @Test
    public void testRunWithDiscoverPartitionsOnceWithZeroMsToCheckNoMoreSplit() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                // set partitionDiscoveryIntervalMs = 0
                KafkaSourceEnumerator enumerator = createEnumerator(context, 0L)) {

            // Start the enumerator, and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThat(context.getPeriodicCallables()).isEmpty();
            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // enumerator noMoreNewPartitionSplits first will be false, when execute
            // handlePartitionSplitChanges will be set true
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isTrue();
        }
    }

    @Test
    public void testDiscoverPartitionsPeriodically() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                INCLUDE_DYNAMIC_TOPIC,
                                OffsetsInitializer.latest());
                AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {

            startEnumeratorAndRegisterReaders(context, enumerator, OffsetsInitializer.latest());

            // invoke partition discovery callable again and there should be no new assignments.
            runPeriodicPartitionDiscovery(context);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("No assignments should be made because there is no partition change")
                    .hasSize(2);

            // create the dynamic topic.
            adminClient
                    .createTopics(
                            Collections.singleton(
                                    new NewTopic(
                                            DYNAMIC_TOPIC_NAME,
                                            NUM_PARTITIONS_DYNAMIC_TOPIC,
                                            (short) 1)))
                    .all()
                    .get();

            // invoke partition discovery callable again.
            while (true) {
                runPeriodicPartitionDiscovery(context);
                if (context.getSplitsAssignmentSequence().size() < 3) {
                    Thread.sleep(10);
                } else {
                    break;
                }
            }
            // later elements are initialized with EARLIEST
            verifyLastReadersAssignments(
                    context,
                    Arrays.asList(READER0, READER1),
                    Collections.singleton(DYNAMIC_TOPIC_NAME),
                    3,
                    OffsetsInitializer.earliest());

            // new partitions use EARLIEST_OFFSET, while initial partitions use LATEST_OFFSET
            List<KafkaPartitionSplit> initialPartitionAssign =
                    getAllAssignSplits(context, PRE_EXISTING_TOPICS);
            assertThat(initialPartitionAssign)
                    .extracting(KafkaPartitionSplit::getStartingOffset)
                    .containsOnly((long) KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION);
            List<KafkaPartitionSplit> newPartitionAssign =
                    getAllAssignSplits(context, Collections.singleton(DYNAMIC_TOPIC_NAME));
            assertThat(newPartitionAssign)
                    .extracting(KafkaPartitionSplit::getStartingOffset)
                    .containsOnly(KafkaPartitionSplit.EARLIEST_OFFSET);

        } finally {
            try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
                adminClient.deleteTopics(Collections.singleton(DYNAMIC_TOPIC_NAME)).all().get();
            } catch (Exception e) {
                // Let it go.
            }
        }
    }

    @Test
    public void shouldLazilyInitializeSplitOffsetsOnMigration() throws Throwable {
        final TopicPartition assigned1 = new TopicPartition(TOPIC1, 0);
        final TopicPartition assigned2 = new TopicPartition(TOPIC1, 1);
        final TopicPartition unassigned1 = new TopicPartition(TOPIC2, 0);
        final TopicPartition unassigned2 = new TopicPartition(TOPIC2, 1);

        final long migratedOffset1 = 11L;
        final long migratedOffset2 = 22L;
        final OffsetsInitializer offsetsInitializer =
                new OffsetsInitializer() {
                    @Override
                    public Map<TopicPartition, Long> getPartitionOffsets(
                            Collection<TopicPartition> partitions,
                            PartitionOffsetsRetriever partitionOffsetsRetriever) {
                        return Map.of(unassigned2, migratedOffset2);
                    }

                    @Override
                    public OffsetResetStrategy getAutoOffsetResetStrategy() {
                        return null;
                    }
                };
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                0,
                                offsetsInitializer,
                                PRE_EXISTING_TOPICS,
                                List.of(
                                        new KafkaPartitionSplit(assigned1, MIGRATED),
                                        new KafkaPartitionSplit(assigned2, 2)),
                                List.of(
                                        new KafkaPartitionSplit(unassigned1, 1),
                                        new KafkaPartitionSplit(unassigned2, MIGRATED)),
                                false,
                                new Properties())) {
            KafkaSourceEnumState state = snapshotWhenReady(enumerator);
            assertThat(state.assignedSplits())
                    .containsExactlyInAnyOrder(
                            new KafkaPartitionSplit(assigned1, MIGRATED),
                            new KafkaPartitionSplit(assigned2, 2));
            assertThat(state.unassignedSplits())
                    .containsExactlyInAnyOrder(
                            new KafkaPartitionSplit(unassigned1, 1),
                            new KafkaPartitionSplit(unassigned2, MIGRATED));

            enumerator.start();

            runOneTimePartitionDiscovery(context);
            KafkaSourceEnumState state2 = snapshotWhenReady(enumerator);
            // verify that only unassigned splits are migrated; assigned splits are tracked by
            // the reader, so any initialization on enumerator would be discarded
            assertThat(state2.assignedSplits())
                    .containsExactlyInAnyOrder(
                            new KafkaPartitionSplit(assigned1, MIGRATED),
                            new KafkaPartitionSplit(assigned2, 2));
            assertThat(state2.unassignedSplits())
                    .containsExactlyInAnyOrder(
                            new KafkaPartitionSplit(unassigned1, 1),
                            new KafkaPartitionSplit(unassigned2, migratedOffset2));
        }
    }

    private static KafkaSourceEnumState snapshotWhenReady(KafkaSourceEnumerator enumerator)
            throws Exception {
        while (true) {
            try {
                return enumerator.snapshotState(1L);
            } catch (CheckpointException e) {
                if (e.getCheckpointFailureReason()
                        != CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY) {
                    throw e;
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(StandardOffsetsInitializer.class)
    public void testAddSplitsBack(StandardOffsetsInitializer offsetsInitializer) throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                true,
                                offsetsInitializer.getOffsetsInitializer())) {

            startEnumeratorAndRegisterReaders(
                    context, enumerator, offsetsInitializer.getOffsetsInitializer());

            // READER2 not yet assigned
            final Set<KafkaPartitionSplit> unassignedSplits =
                    enumerator.getPendingPartitionSplitAssignment().get(READER2);
            assertThat(enumerator.snapshotState(1L).unassignedSplits())
                    .containsExactlyInAnyOrderElementsOf(unassignedSplits);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            final List<KafkaPartitionSplit> assignedSplits =
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0);
            final List<KafkaPartitionSplit> advancedSplits =
                    assignedSplits.stream()
                            .map(
                                    split ->
                                            new KafkaPartitionSplit(
                                                    split.getTopicPartition(),
                                                    split.getStartingOffset() + 1))
                            .collect(Collectors.toList());
            enumerator.addSplitsBack(advancedSplits, READER0);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("The added back splits should have not been assigned")
                    .hasSize(2);

            final KafkaSourceEnumState state = enumerator.snapshotState(2L);
            assertThat(state.unassignedSplits())
                    .containsExactlyInAnyOrderElementsOf(
                            Iterables.concat(
                                    advancedSplits, unassignedSplits)); // READER0 + READER2
            assertThat(state.assignedSplits()).doesNotContainAnyElementsOf(advancedSplits);

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyAssignments(
                    Map.of(READER0, advancedSplits),
                    context.getSplitsAssignmentSequence().get(2).assignment());
            assertThat(enumerator.snapshotState(3L).unassignedSplits())
                    .containsExactlyInAnyOrderElementsOf(unassignedSplits);
        }
    }

    @Test
    public void testWorkWithPreexistingAssignments() throws Throwable {
        Collection<KafkaPartitionSplit> preexistingAssignments;
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context1 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context1, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
            startEnumeratorAndRegisterReaders(context1, enumerator, OffsetsInitializer.earliest());
            preexistingAssignments =
                    asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context2 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context2,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY ? 1 : -1,
                                OffsetsInitializer.earliest(),
                                PRE_EXISTING_TOPICS,
                                preexistingAssignments,
                                Collections.emptySet(),
                                true,
                                new Properties())) {
            enumerator.start();
            runPeriodicPartitionDiscovery(context2);

            registerReader(context2, enumerator, READER0);
            assertThat(context2.getSplitsAssignmentSequence()).isEmpty();

            registerReader(context2, enumerator, READER1);
            verifyLastReadersAssignments(
                    context2, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testKafkaClientProperties() throws Exception {
        Properties properties = new Properties();
        String clientIdPrefix = "test-prefix";
        Integer defaultTimeoutMs = 99999;
        properties.setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), clientIdPrefix);
        properties.setProperty(
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(defaultTimeoutMs));
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY ? 1 : -1,
                                OffsetsInitializer.earliest(),
                                PRE_EXISTING_TOPICS,
                                Collections.emptySet(),
                                Collections.emptySet(),
                                false,
                                properties)) {
            enumerator.start();

            AdminClient adminClient =
                    (AdminClient) Whitebox.getInternalState(enumerator, "adminClient");
            assertThat(adminClient).isNotNull();
            String clientId = (String) Whitebox.getInternalState(adminClient, "clientId");
            assertThat(clientId).isNotNull().startsWith(clientIdPrefix);
            assertThat((int) Whitebox.getInternalState(adminClient, "defaultApiTimeoutMs"))
                    .isEqualTo(defaultTimeoutMs);

            assertThat(clientId).isNotNull().startsWith(clientIdPrefix);
        }
    }

    @ParameterizedTest
    @EnumSource(StandardOffsetsInitializer.class)
    public void testSnapshotState(StandardOffsetsInitializer offsetsInitializer) throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context, false, true, offsetsInitializer.getOffsetsInitializer())) {
            enumerator.start();

            // Step1: Before first discovery, so the state should be empty
            final KafkaSourceEnumState state1 = enumerator.snapshotState(1L);
            assertThat(state1.assignedSplits()).isEmpty();
            assertThat(state1.unassignedSplits()).isEmpty();
            assertThat(state1.initialDiscoveryFinished()).isFalse();

            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);

            // Step2: First partition discovery after start, but no assignments to readers
            context.runNextOneTimeCallable();
            final KafkaSourceEnumState state2 = enumerator.snapshotState(2L);
            assertThat(state2.assignedSplits()).isEmpty();
            assertThat(state2.unassignedSplits()).isEmpty();
            assertThat(state2.initialDiscoveryFinished()).isFalse();

            // Step3: Assign partials partitions to reader0 and reader1
            context.runNextOneTimeCallable();

            // The state should contain splits assigned to READER0 and READER1, but no READER2
            // register.
            // Thus, both assignedPartitions and unassignedInitialPartitions are not empty.
            final KafkaSourceEnumState state3 = enumerator.snapshotState(2L);
            verifySplitAssignmentWithPartitions(
                    getExpectedAssignments(
                            new HashSet<>(Arrays.asList(READER0, READER1)),
                            PRE_EXISTING_TOPICS,
                            offsetsInitializer.getOffsetsInitializer()),
                    state3.assignedSplits());
            assertThat(state3.assignedSplits()).isNotEmpty();
            assertThat(state3.unassignedSplits()).isNotEmpty();
            assertThat(state3.initialDiscoveryFinished()).isTrue();

            // Step3: register READER2, then all partitions are assigned
            registerReader(context, enumerator, READER2);
            final KafkaSourceEnumState state4 = enumerator.snapshotState(3L);
            assertThat(state4.assignedSplits())
                    .containsExactlyInAnyOrderElementsOf(
                            Iterables.concat(state3.assignedSplits(), state3.unassignedSplits()));
            assertThat(state4.unassignedSplits()).isEmpty();
            assertThat(state4.initialDiscoveryFinished()).isTrue();
        }
    }

    @Test
    public void testPartitionChangeChecking() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

            // All partitions of TOPIC1 and TOPIC2 should have been discovered now

            // Check partition change using only DYNAMIC_TOPIC_NAME-0
            TopicPartition newPartition = new TopicPartition(DYNAMIC_TOPIC_NAME, 0);
            Set<TopicPartition> fetchedPartitions = new HashSet<>();
            fetchedPartitions.add(newPartition);
            final KafkaSourceEnumerator.PartitionChange partitionChange =
                    enumerator.getPartitionChange(fetchedPartitions, false);

            // Since enumerator never met DYNAMIC_TOPIC_NAME-0, it should be mark as a new partition
            Set<TopicPartition> expectedNewPartitions = Collections.singleton(newPartition);

            // All existing topics are not in the fetchedPartitions, so they should be marked as
            // removed
            Set<TopicPartition> expectedRemovedPartitions = new HashSet<>();
            for (int i = 0; i < KafkaSourceTestEnv.NUM_PARTITIONS; i++) {
                expectedRemovedPartitions.add(new TopicPartition(TOPIC1, i));
                expectedRemovedPartitions.add(new TopicPartition(TOPIC2, i));
            }

            // Since enumerator never met DYNAMIC_TOPIC_NAME-1, it should be marked as a new
            // partition
            assertThat(partitionChange.getNewPartitions()).isEqualTo(expectedNewPartitions);
            assertThat(partitionChange.getInitialPartitions()).isEqualTo(Set.of());
            assertThat(partitionChange.getRemovedPartitions()).isEqualTo(expectedRemovedPartitions);
        }
    }

    // -------------- some common startup sequence ---------------

    private void startEnumeratorAndRegisterReaders(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            KafkaSourceEnumerator enumerator,
            OffsetsInitializer offsetsInitializer)
            throws Throwable {
        // Start the enumerator and it should schedule a one time task to discover and assign
        // partitions.
        enumerator.start();

        // register reader 0 before the partition discovery.
        registerReader(context, enumerator, READER0);
        assertThat(context.getSplitsAssignmentSequence()).isEmpty();

        // Run the partition discover callable and check the partition assignment.
        runPeriodicPartitionDiscovery(context);
        verifyLastReadersAssignments(
                context,
                Collections.singleton(READER0),
                PRE_EXISTING_TOPICS,
                1,
                offsetsInitializer);

        // Register reader 1 after first partition discovery.
        registerReader(context, enumerator, READER1);
        verifyLastReadersAssignments(
                context,
                Collections.singleton(READER1),
                PRE_EXISTING_TOPICS,
                2,
                offsetsInitializer);
    }

    // ----------------------------------------

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery) {
        return createEnumerator(
                enumContext,
                enablePeriodicPartitionDiscovery,
                EXCLUDE_DYNAMIC_TOPIC,
                OffsetsInitializer.earliest());
    }

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs) {
        return createEnumerator(
                enumContext,
                partitionDiscoveryIntervalMs,
                EXCLUDE_DYNAMIC_TOPIC,
                OffsetsInitializer.earliest());
    }

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            boolean includeDynamicTopic,
            OffsetsInitializer startingOffsetsInitializer) {
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        return createEnumerator(
                enumContext,
                enablePeriodicPartitionDiscovery ? 1 : -1,
                startingOffsetsInitializer,
                topics,
                Collections.emptySet(),
                Collections.emptySet(),
                false,
                new Properties());
    }

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs,
            boolean includeDynamicTopic,
            OffsetsInitializer startingOffsetsInitializer) {
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        return createEnumerator(
                enumContext,
                partitionDiscoveryIntervalMs,
                startingOffsetsInitializer,
                topics,
                Collections.emptySet(),
                Collections.emptySet(),
                false,
                new Properties());
    }

    /**
     * Create the enumerator. For the purpose of the tests in this class we don't care about the
     * subscriber and offsets initializer, so just use arbitrary settings.
     */
    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs,
            OffsetsInitializer startingOffsetsInitializer,
            Collection<String> topicsToSubscribe,
            Collection<KafkaPartitionSplit> assignedSplits,
            Collection<KafkaPartitionSplit> unassignedInitialSplits,
            boolean initialDiscoveryFinished,
            Properties overrideProperties) {
        return createEnumerator(
                enumContext,
                partitionDiscoveryIntervalMs,
                topicsToSubscribe,
                assignedSplits,
                unassignedInitialSplits,
                overrideProperties,
                startingOffsetsInitializer);
    }

    /**
     * Create the enumerator. For the purpose of the tests in this class we don't care about the
     * subscriber and stopping initializer, so just use arbitrary settings.
     */
    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs,
            Collection<String> topicsToSubscribe,
            Collection<KafkaPartitionSplit> assignedSplits,
            Collection<KafkaPartitionSplit> unassignedInitialSplits,
            Properties overrideProperties,
            OffsetsInitializer startingOffsetsInitializer) {
        // Use a TopicPatternSubscriber so that no exception if a subscribed topic hasn't been
        // created yet.
        StringJoiner topicNameJoiner = new StringJoiner("|");
        topicsToSubscribe.forEach(topicNameJoiner::add);
        Pattern topicPattern = Pattern.compile(topicNameJoiner.toString());
        KafkaSubscriber subscriber = KafkaSubscriber.getTopicPatternSubscriber(topicPattern);

        OffsetsInitializer stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();

        Properties props =
                new Properties(KafkaSourceTestEnv.getConsumerProperties(StringDeserializer.class));
        KafkaSourceEnumerator.deepCopyProperties(overrideProperties, props);
        props.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                String.valueOf(partitionDiscoveryIntervalMs));

        return new KafkaSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                Boundedness.CONTINUOUS_UNBOUNDED,
                new KafkaSourceEnumState(assignedSplits, unassignedInitialSplits, false));
    }

    // ---------------------

    /** The standard {@link OffsetsInitializer}s used for parameterized tests. */
    enum StandardOffsetsInitializer {
        EARLIEST_OFFSETS(OffsetsInitializer.earliest()),
        LATEST_OFFSETS(OffsetsInitializer.latest()),
        SPECIFIC_OFFSETS(OffsetsInitializer.offsets(specificOffsets, OffsetResetStrategy.NONE)),
        COMMITTED_OFFSETS(OffsetsInitializer.committedOffsets());

        private final OffsetsInitializer offsetsInitializer;

        StandardOffsetsInitializer(OffsetsInitializer offsetsInitializer) {
            this.offsetsInitializer = offsetsInitializer;
        }

        public OffsetsInitializer getOffsetsInitializer() {
            return offsetsInitializer;
        }
    }

    private void registerReader(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            KafkaSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location 0"));
        enumerator.addReader(reader);
    }

    private void verifyLastReadersAssignments(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topics,
            int expectedAssignmentSeqSize) {
        verifyLastReadersAssignments(
                context, readers, topics, expectedAssignmentSeqSize, OffsetsInitializer.earliest());
    }

    private void verifyLastReadersAssignments(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topics,
            int expectedAssignmentSeqSize,
            OffsetsInitializer offsetsInitializer) {
        verifyAssignments(
                getExpectedAssignments(new HashSet<>(readers), topics, offsetsInitializer),
                context.getSplitsAssignmentSequence()
                        .get(expectedAssignmentSeqSize - 1)
                        .assignment());
    }

    private void verifyAssignments(
            Map<Integer, Collection<KafkaPartitionSplit>> expectedAssignments,
            Map<Integer, List<KafkaPartitionSplit>> actualAssignments) {
        assertThat(actualAssignments).containsOnlyKeys(expectedAssignments.keySet());
        SoftAssertions.assertSoftly(
                softly -> {
                    for (Map.Entry<Integer, List<KafkaPartitionSplit>> actual :
                            actualAssignments.entrySet()) {
                        softly.assertThat(actual.getValue())
                                .as("Assignment for reader %s", actual.getKey())
                                .containsExactlyInAnyOrderElementsOf(
                                        expectedAssignments.get(actual.getKey()));
                    }
                });
    }

    private Map<Integer, Collection<KafkaPartitionSplit>> getExpectedAssignments(
            Set<Integer> readers,
            Set<String> topics,
            OffsetsInitializer startingOffsetsInitializer) {
        Map<Integer, Collection<KafkaPartitionSplit>> expectedAssignments = new HashMap<>();
        Set<KafkaPartitionSplit> allPartitions = new HashSet<>();

        if (topics.contains(DYNAMIC_TOPIC_NAME)) {
            for (int i = 0; i < NUM_PARTITIONS_DYNAMIC_TOPIC; i++) {
                TopicPartition tp = new TopicPartition(DYNAMIC_TOPIC_NAME, i);
                allPartitions.add(createSplit(tp, startingOffsetsInitializer));
            }
        }

        for (TopicPartition tp : KafkaSourceTestEnv.getPartitionsForTopics(PRE_EXISTING_TOPICS)) {
            if (topics.contains(tp.topic())) {
                allPartitions.add(createSplit(tp, startingOffsetsInitializer));
            }
        }

        for (KafkaPartitionSplit split : allPartitions) {
            int ownerReader =
                    KafkaSourceEnumerator.getSplitOwner(split.getTopicPartition(), NUM_SUBTASKS);
            if (readers.contains(ownerReader)) {
                expectedAssignments.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(split);
            }
        }
        return expectedAssignments;
    }

    private static KafkaPartitionSplit createSplit(
            TopicPartition tp, OffsetsInitializer startingOffsetsInitializer) {
        return new KafkaPartitionSplit(
                tp, startingOffsetsInitializer.getPartitionOffsets(List.of(tp), retriever).get(tp));
    }

    private void verifySplitAssignmentWithPartitions(
            Map<Integer, Collection<KafkaPartitionSplit>> expectedAssignment,
            Collection<KafkaPartitionSplit> actualTopicPartitions) {
        final Set<KafkaPartitionSplit> allTopicPartitionsFromAssignment =
                expectedAssignment.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        assertThat(actualTopicPartitions)
                .containsExactlyInAnyOrderElementsOf(allTopicPartitionsFromAssignment);
    }

    /** get all assigned partition splits of topics. */
    private List<KafkaPartitionSplit> getAllAssignSplits(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context, Set<String> topics) {

        List<KafkaPartitionSplit> allSplits = new ArrayList<>();
        List<SplitsAssignment<KafkaPartitionSplit>> splitsAssignmentSequence =
                context.getSplitsAssignmentSequence();
        for (SplitsAssignment<KafkaPartitionSplit> splitsAssignment : splitsAssignmentSequence) {
            List<KafkaPartitionSplit> splitsOfOnceAssignment =
                    splitsAssignment.assignment().values().stream()
                            .flatMap(splits -> splits.stream())
                            .filter(split -> topics.contains(split.getTopic()))
                            .collect(Collectors.toList());
            allSplits.addAll(splitsOfOnceAssignment);
        }

        return allSplits;
    }

    private Collection<KafkaPartitionSplit> asEnumState(
            Map<Integer, List<KafkaPartitionSplit>> assignments) {
        return assignments.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private void runOneTimePartitionDiscovery(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runNextOneTimeCallable();
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private void runPeriodicPartitionDiscovery(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }
}
