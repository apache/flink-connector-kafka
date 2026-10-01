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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.streaming.connectors.kafka.KafkaTestBase.kafkaServer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Unit tests for {@link KafkaPartitionSplitReader}. */
@ResourceLock("KafkaTestBase")
public class KafkaPartitionSplitReaderTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static final String TOPIC3 = "topic3";

    private static Map<Integer, Map<String, KafkaPartitionSplit>> splitsByOwners;
    private static Map<TopicPartition, Long> earliestOffsets;

    private final IntegerDeserializer deserializer = new IntegerDeserializer();

    @BeforeAll
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        KafkaSourceTestEnv.setupTopic(TOPIC2, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        KafkaSourceTestEnv.createTestTopic(TOPIC3);
        splitsByOwners =
                KafkaSourceTestEnv.getSplitsByOwners(Arrays.asList(TOPIC1, TOPIC2), NUM_SUBTASKS);
        earliestOffsets =
                KafkaSourceTestEnv.getEarliestOffsets(
                        KafkaSourceTestEnv.getPartitionsForTopics(Arrays.asList(TOPIC1, TOPIC2)));
    }

    @AfterAll
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testHandleSplitChangesAndFetch() throws Exception {
        KafkaPartitionSplitReader reader = createReader();
        assignSplitsAndFetchUntilFinish(reader, 0);
        assignSplitsAndFetchUntilFinish(reader, 1);
    }

    @Test
    public void testWakeUp() throws Exception {
        KafkaPartitionSplitReader reader = createReader();
        TopicPartition nonExistingTopicPartition = new TopicPartition("NotExist", 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        KafkaPartitionSplit.toSplitId(nonExistingTopicPartition),
                        new KafkaPartitionSplit(nonExistingTopicPartition, 0)));
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                reader.fetch();
                            } catch (Throwable e) {
                                error.set(e);
                            }
                        },
                        "testWakeUp-thread");
        t.start();
        long deadline = System.currentTimeMillis() + 5000L;
        while (t.isAlive() && System.currentTimeMillis() < deadline) {
            reader.wakeUp();
            Thread.sleep(10);
        }
        assertThat(error.get()).isNull();
    }

    @Test
    public void testWakeupThenAssign() throws IOException {
        KafkaPartitionSplitReader reader = createReader();
        // Assign splits with records
        assignSplits(reader, splitsByOwners.get(0));
        // Run a fetch operation, and it should not block
        reader.fetch();
        // Wake the reader up then assign a new split. This assignment should not throw
        // WakeupException.
        reader.wakeUp();
        TopicPartition tp = new TopicPartition(TOPIC1, 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        KafkaPartitionSplit.toSplitId(tp),
                        new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET)));
    }

    @Test
    public void testNumBytesInCounter() throws Exception {
        final OperatorMetricGroup operatorMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        final Counter numBytesInCounter =
                operatorMetricGroup.getIOMetricGroup().getNumBytesInCounter();
        KafkaPartitionSplitReader reader =
                createReader(
                        new Properties(),
                        InternalSourceReaderMetricGroup.wrap(operatorMetricGroup));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(TOPIC1, 0), 0L))));
        reader.fetch();
        final long latestNumBytesIn = numBytesInCounter.getCount();
        // Since it's hard to know the exact number of bytes consumed, we just check if it is
        // greater than 0
        assertThat(latestNumBytesIn).isGreaterThan(0L);
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(TOPIC2, 0), 0L))));
        reader.fetch();
        // We just check if numBytesIn is increasing
        assertThat(numBytesInCounter.getCount()).isGreaterThan(latestNumBytesIn);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"_underscore.period-minus"})
    public void testPendingRecordsGauge(String topicSuffix) throws Throwable {
        final String topic1Name = TOPIC1 + topicSuffix;
        final String topic2Name = TOPIC2 + topicSuffix;
        if (!topicSuffix.isEmpty()) {
            KafkaSourceTestEnv.setupTopic(
                    topic1Name, true, true, KafkaSourceTestEnv::getRecordsForTopic);
            KafkaSourceTestEnv.setupTopic(
                    topic2Name, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        }
        MetricListener metricListener = new MetricListener();
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        KafkaPartitionSplitReader reader =
                createReader(
                        props,
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(topic1Name, 0), 0L))));
        // pendingRecords should have not been registered because of lazily registration
        assertThat(metricListener.getGauge(MetricNames.PENDING_RECORDS)).isNotPresent();
        // Trigger first fetch
        reader.fetch();
        final Optional<Gauge<Long>> pendingRecords =
                metricListener.getGauge(MetricNames.PENDING_RECORDS);
        assertThat(pendingRecords).isPresent();
        // Validate pendingRecords
        assertThat(pendingRecords).isNotNull();
        assertThat((long) pendingRecords.get().getValue()).isEqualTo(NUM_RECORDS_PER_PARTITION - 1);
        for (int i = 1; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertThat((long) pendingRecords.get().getValue())
                    .isEqualTo(NUM_RECORDS_PER_PARTITION - i - 1);
        }
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(topic2Name, 0), 0L))));
        // Validate pendingRecords
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertThat((long) pendingRecords.get().getValue())
                    .isEqualTo(NUM_RECORDS_PER_PARTITION - i - 1);
        }
    }

    @Test
    public void testAssignEmptySplit() throws Exception {
        KafkaPartitionSplitReader reader = createReader();
        final KafkaPartitionSplit normalSplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC1, 0),
                        KafkaPartitionSplit.EARLIEST_OFFSET,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET);
        final KafkaPartitionSplit emptySplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC2, 0),
                        KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION,
                        KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION);
        final KafkaPartitionSplit emptySplitWithZeroStoppingOffset =
                new KafkaPartitionSplit(new TopicPartition(TOPIC3, 0), 0, 0);

        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Arrays.asList(normalSplit, emptySplit, emptySplitWithZeroStoppingOffset)));

        // Fetch and check empty splits is added to finished splits
        RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> recordsWithSplitIds = reader.fetch();
        assertThat(recordsWithSplitIds.finishedSplits()).contains(emptySplit.splitId());
        assertThat(recordsWithSplitIds.finishedSplits())
                .contains(emptySplitWithZeroStoppingOffset.splitId());

        // Assign another valid split to avoid consumer.poll() blocking
        final KafkaPartitionSplit anotherNormalSplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC1, 1),
                        KafkaPartitionSplit.EARLIEST_OFFSET,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(anotherNormalSplit)));

        // Fetch again and check empty split set is cleared
        recordsWithSplitIds = reader.fetch();
        assertThat(recordsWithSplitIds.finishedSplits()).isEmpty();
    }

    @Test
    public void testUsingCommittedOffsetsWithNoneOffsetResetStrategy() {
        final Properties props = new Properties();
        props.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG, "using-committed-offset-with-none-offset-reset");
        KafkaPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
        // We expect that there is a committed offset, but the group does not actually have a
        // committed offset, and the offset reset strategy is none (Throw exception to the consumer
        // if no previous offset is found for the consumer's group);
        // So it is expected to throw an exception that missing the committed offset.
        assertThatThrownBy(
                        () ->
                                reader.handleSplitsChanges(
                                        new SplitsAddition<>(
                                                Collections.singletonList(
                                                        new KafkaPartitionSplit(
                                                                new TopicPartition(TOPIC1, 0),
                                                                KafkaPartitionSplit
                                                                        .COMMITTED_OFFSET)))))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("Undefined offset with no reset policy for partition");
    }

    @ParameterizedTest
    @CsvSource({"earliest, 0", "latest, 10"})
    public void testUsingCommittedOffsetsWithEarliestOrLatestOffsetResetStrategy(
            String offsetResetStrategy, Long expectedOffset) {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "using-committed-offset");
        KafkaPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
        // Add committed offset split
        final TopicPartition partition = new TopicPartition(TOPIC1, 0);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(
                                        partition, KafkaPartitionSplit.COMMITTED_OFFSET))));

        // Verify that the current offset of the consumer is the expected offset
        assertThat(reader.consumer().position(partition)).isEqualTo(expectedOffset);
    }

    @Test
    public void testConsumerClientRackSupplier() {
        String rackId = "use1-az1";
        Properties properties = new Properties();
        KafkaPartitionSplitReader reader =
                createReader(
                        properties,
                        UnregisteredMetricsGroup.createSourceReaderMetricGroup(),
                        rackId);

        // Here we call the helper function directly, because the KafkaPartitionSplitReader
        // doesn't allow us to examine the final ConsumerConfig object
        reader.setConsumerClientRack(properties, rackId);
        assertThat(properties.get(ConsumerConfig.CLIENT_RACK_CONFIG)).isEqualTo(rackId);
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void testSetConsumerClientRackIgnoresNullAndEmpty(String rackId) {
        Properties properties = new Properties();
        KafkaPartitionSplitReader reader =
                createReader(
                        properties,
                        UnregisteredMetricsGroup.createSourceReaderMetricGroup(),
                        rackId);

        // Here we call the helper function directly, because the KafkaPartitionSplitReader
        // doesn't allow us to examine the final ConsumerConfig object
        reader.setConsumerClientRack(properties, rackId);
        assertThat(properties.containsKey(ConsumerConfig.CLIENT_RACK_CONFIG)).isFalse();
    }

    @Test
    void testPauseOrResumeSplitsWithUnassignedPartition() {
        KafkaPartitionSplitReader reader = createReader();
        // Create a split for a partition that is NOT assigned to the consumer.
        // Without the fix, this would throw IllegalStateException:
        // "No current assignment for partition".
        TopicPartition unassignedPartition = new TopicPartition(TOPIC1, 0);
        KafkaPartitionSplit unassignedSplit =
                new KafkaPartitionSplit(unassignedPartition, KafkaPartitionSplit.EARLIEST_OFFSET);

        // Verify the partition is indeed not assigned
        assertThat(reader.consumer().assignment()).doesNotContain(unassignedPartition);

        // This should be a no-op, not throw an exception
        reader.pauseOrResumeSplits(
                Collections.singletonList(unassignedSplit), Collections.emptyList());
        reader.pauseOrResumeSplits(
                Collections.emptyList(), Collections.singletonList(unassignedSplit));
        reader.pauseOrResumeSplits(
                Collections.singletonList(unassignedSplit),
                Collections.singletonList(unassignedSplit));
    }

    @Test
    public void testDefaultPollTimeout() {
        // When the property is not set, the reader falls back to the option's default instead of
        // hardcoding a timeout of its own.
        assertThat(createReader().getPollTimeout())
                .isEqualTo(Duration.ofMillis(KafkaSourceOptions.POLL_TIMEOUT_MS.defaultValue()));
    }

    @Test
    public void testConfiguredPollTimeout() {
        final Properties props = new Properties();
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "500");
        KafkaPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());

        assertThat(reader.getPollTimeout()).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    public void testZeroPollTimeout() {
        // KafkaConsumer#poll accepts a zero timeout, which returns immediately with whatever is
        // already buffered, so the reader must not reject it either.
        final Properties props = new Properties();
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "0");
        KafkaPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());

        assertThat(reader.getPollTimeout()).isEqualTo(Duration.ZERO);
    }

    @Test
    public void testNegativePollTimeoutIsRejected() {
        final Properties props = new Properties();
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "-1");
        assertThatThrownBy(
                        () ->
                                createReader(
                                        props,
                                        UnregisteredMetricsGroup.createSourceReaderMetricGroup()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        String.format(
                                "Property %s should not be negative, but is -1",
                                KafkaSourceOptions.POLL_TIMEOUT_MS.key()));
    }

    @Test
    void testTrackedOffsetsAreRemovedWhenPartitionsAreUnassigned() throws Exception {
        KafkaPartitionSplitReader reader = createReaderForCommits();
        final TopicPartition finishingPartition = new TopicPartition(TOPIC1, 0);
        // a second partition is kept assigned throughout, which prevents consumer.poll()
        // blocking, and proves that only offsets of the unassigned partition are removed
        final TopicPartition remainingPartition = new TopicPartition(TOPIC1, 1);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Arrays.asList(
                                new KafkaPartitionSplit(
                                        finishingPartition,
                                        earliestOffsets.get(finishingPartition),
                                        NUM_RECORDS_PER_PARTITION),
                                new KafkaPartitionSplit(
                                        remainingPartition,
                                        earliestOffsets.get(remainingPartition),
                                        KafkaPartitionSplit.NO_STOPPING_OFFSET))));

        // a commit while both partitions are assigned records their committed offsets
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        offsetsToCommit.put(
                finishingPartition, new OffsetAndMetadata(earliestOffsets.get(finishingPartition)));
        offsetsToCommit.put(
                remainingPartition, new OffsetAndMetadata(earliestOffsets.get(remainingPartition)));
        commitOffsets(reader, offsetsToCommit, Collections.emptySet());

        // fetch until the bounded split has been finished and unassigned
        fetchUntil(
                reader,
                () -> !reader.consumer().assignment().contains(finishingPartition),
                "The bounded split was not finished.");
        assertThat(reader.consumer().assignment()).contains(remainingPartition);

        assertThat(reader.lastFetchedOffsets)
                .as("Offsets tracked for committing")
                .doesNotContainKey(finishingPartition)
                .containsKey(remainingPartition);
        assertThat(reader.lastKnownPositions)
                .as("Consumer positions tracked for committing")
                .doesNotContainKey(finishingPartition)
                .containsKey(remainingPartition);
        assertThat(reader.lastCommittedOffsets)
                .as("Offsets last committed")
                .doesNotContainKey(finishingPartition)
                .containsKey(remainingPartition);
    }

    @Test
    void testPartitionWithoutOffsetIsCommittedAtConsumerPosition() throws Exception {
        final KafkaPartitionSplitReader reader = createReaderForCommits();
        // an empty partition, so the split never emits a record
        final TopicPartition tp = new TopicPartition(TOPIC3, 0);
        assignSplit(reader, new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET));
        reader.fetch();

        assertThat(commitPartitionsWithoutOffset(reader, tp))
                .containsExactly(entry(tp, new OffsetAndMetadata(0L)));
    }

    @Test
    void testPartitionWithoutOffsetIsNotCommittedOnceRecordsAreFetched() throws Exception {
        final KafkaPartitionSplitReader reader = createReaderForCommits();
        final TopicPartition tp = new TopicPartition(TOPIC1, 0);
        assignSplit(reader, new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET));
        fetchUntil(
                reader,
                () -> reader.lastFetchedOffsets.containsKey(tp),
                "No records were fetched.");

        // the checkpoint does not include the fetched records, so the position would skip them
        assertThat(commitPartitionsWithoutOffset(reader, tp)).isEmpty();
    }

    @Test
    void testPartitionWithoutOffsetIsNotCommittedForBoundedSplit() throws Exception {
        final KafkaPartitionSplitReader reader = createReaderForCommits();
        final TopicPartition tp = new TopicPartition(TOPIC3, 1);
        assignSplit(reader, new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET, 5L));
        reader.fetch();

        assertThat(commitPartitionsWithoutOffset(reader, tp)).isEmpty();
    }

    /**
     * Reproduces a checkpoint whose offset is followed by a commit marker being committed twice:
     * first while the partition is idle (so the offset progresses over the marker), and then second
     * after a new record has been fetched (which must not move the committed offset back).
     */
    @Test
    void testCommittedOffsetDoesNotGoBackwardsWhenRecordIsFetchedBeforeCommit() throws Throwable {
        final String topic = "CommittedOffsetDoesNotGoBackwards";
        final String groupId = topic + "Group";
        final TopicPartition tp = new TopicPartition(topic, 0);
        KafkaSourceTestEnv.createTestTopic(topic, 1, 1);

        // offset 0 = record
        // offset 1 = commit marker
        produceRecordInTransaction(tp);

        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "100");
        final KafkaPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
        assignSplit(reader, new KafkaPartitionSplit(tp, 0L));

        // both checkpoints are taken after the record at offset 0 is emitted
        final Map<TopicPartition, OffsetAndMetadata> snapshot =
                Collections.singletonMap(tp, new OffsetAndMetadata(1L));

        // checkpoint 1 is committed while the partition is idle after the commit marker
        fetchUntil(
                reader,
                () -> Long.valueOf(2L).equals(reader.lastKnownPositions.get(tp)),
                "The consumer did not move past the commit marker.");
        commitOffsets(reader, snapshot, Collections.emptySet());
        assertThat(getCommittedOffset(tp, groupId))
                .as("The committed offset after checkpoint 1")
                .isEqualTo(2L);

        // offset 2 = record
        // offset 3 = commit marker
        produceRecordInTransaction(tp);

        // checkpoint 2 is committed after the record at offset 2 is fetched, but not emitted
        fetchUntil(
                reader,
                () -> Long.valueOf(2L).equals(reader.lastFetchedOffsets.get(tp)),
                "The record at offset 2 was not fetched.");
        commitOffsets(reader, snapshot, Collections.emptySet());
        assertThat(getCommittedOffset(tp, groupId))
                .as("The committed offset after checkpoint 2")
                .isEqualTo(2L);
    }

    /**
     * The consumer only completes a commit on a later poll, so a commit for a later checkpoint can
     * be sent while an earlier one is still in flight.
     */
    @Test
    void testCommitInFlightIsNotOvertakenByLowerOffset() throws Exception {
        final String groupId = "CommitInFlightGroup";
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "100");
        final KafkaPartitionSplitReader reader =
                createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
        final TopicPartition tp = new TopicPartition(TOPIC1, 0);
        // not fetched, so there is no consumer position to reconcile the offsets with
        assignSplit(reader, new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET));

        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> first =
                startCommit(
                        reader,
                        Collections.singletonMap(tp, new OffsetAndMetadata(5L)),
                        Collections.emptySet());
        assertThat(first).as("The first commit is still in flight").isNotDone();
        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> second =
                startCommit(
                        reader,
                        Collections.singletonMap(tp, new OffsetAndMetadata(3L)),
                        Collections.emptySet());
        fetchUntil(
                reader,
                () -> first.isDone() && second.isDone(),
                "The offset commits did not complete.");

        assertThat(first.get()).containsExactly(entry(tp, new OffsetAndMetadata(5L)));
        assertThat(second.get()).isEmpty();
        assertThat(getCommittedOffset(tp, groupId)).isEqualTo(5L);
    }

    /**
     * The consumer would otherwise only run the callback on a later poll, which on an idle
     * partition is a poll timeout away.
     */
    @Test
    void testCommitCompletesImmediatelyWhenThereIsNothingToCommit() {
        final KafkaPartitionSplitReader reader = createReaderForCommits();
        final TopicPartition tp = new TopicPartition(TOPIC3, 1);
        // a bounded split is not committed at the consumer position, so nothing is committed
        assignSplit(reader, new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET, 5L));

        assertThat(startCommit(reader, Collections.emptyMap(), Collections.singleton(tp)))
                .isCompletedWithValue(Collections.emptyMap());
    }

    /**
     * Partitions are only unassigned when their split finishes, which also removes their tracked
     * consumer position. A stale position is injected here, so that the assignment is checked on
     * its own.
     */
    @Test
    void testOffsetsOfUnassignedPartitionsAreNotReconciled() throws Exception {
        final KafkaPartitionSplitReader reader = createReaderForCommits();
        // keeps the consumer assigned, so that it can poll for the commit to complete
        assignSplit(
                reader,
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC3, 0), KafkaPartitionSplit.EARLIEST_OFFSET));
        final TopicPartition unassignedWithOffset = new TopicPartition(TOPIC1, 0);
        final TopicPartition unassignedWithoutOffset = new TopicPartition(TOPIC1, 1);
        reader.lastKnownPositions.put(unassignedWithOffset, 5L);
        reader.lastKnownPositions.put(unassignedWithoutOffset, 5L);

        assertThat(
                        commitOffsets(
                                reader,
                                Collections.singletonMap(
                                        unassignedWithOffset, new OffsetAndMetadata(2L)),
                                Collections.singleton(unassignedWithoutOffset)))
                .containsExactly(entry(unassignedWithOffset, new OffsetAndMetadata(2L)));
        assertThat(reader.lastCommittedOffsets).doesNotContainKey(unassignedWithOffset);
    }

    // ------------------

    private void assignSplitsAndFetchUntilFinish(KafkaPartitionSplitReader reader, int readerId)
            throws IOException {
        Map<String, KafkaPartitionSplit> splits =
                assignSplits(reader, splitsByOwners.get(readerId));

        Map<String, Integer> numConsumedRecords = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();
        while (finishedSplits.size() < splits.size()) {
            RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> recordsBySplitIds = reader.fetch();
            String splitId = recordsBySplitIds.nextSplit();
            while (splitId != null) {
                // Collect the records in this split.
                List<ConsumerRecord<byte[], byte[]>> splitFetch = new ArrayList<>();
                ConsumerRecord<byte[], byte[]> record;
                while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                    splitFetch.add(record);
                }

                // Compute the expected next offset for the split.
                TopicPartition tp = splits.get(splitId).getTopicPartition();
                long earliestOffset = earliestOffsets.get(tp);
                int numConsumedRecordsForSplit = numConsumedRecords.getOrDefault(splitId, 0);
                long expectedStartingOffset = earliestOffset + numConsumedRecordsForSplit;

                // verify the consumed records.
                if (verifyConsumed(splits.get(splitId), expectedStartingOffset, splitFetch)) {
                    finishedSplits.add(splitId);
                }
                numConsumedRecords.compute(
                        splitId,
                        (ignored, recordCount) ->
                                recordCount == null
                                        ? splitFetch.size()
                                        : recordCount + splitFetch.size());
                splitId = recordsBySplitIds.nextSplit();
            }
        }

        // Verify the number of records consumed from each split.
        numConsumedRecords.forEach(
                (splitId, recordCount) -> {
                    TopicPartition tp = splits.get(splitId).getTopicPartition();
                    long earliestOffset = earliestOffsets.get(tp);
                    long expectedRecordCount = NUM_RECORDS_PER_PARTITION - earliestOffset;
                    assertThat((long) recordCount)
                            .as(
                                    String.format(
                                            "%s should have %d records.",
                                            splits.get(splitId), expectedRecordCount))
                            .isEqualTo(expectedRecordCount);
                });
    }

    // ------------------

    private KafkaPartitionSplitReader createReader() {
        return createReader(
                new Properties(), UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    }

    private KafkaPartitionSplitReader createReader(
            Properties additionalProperties, SourceReaderMetricGroup sourceReaderMetricGroup) {
        return createReader(additionalProperties, sourceReaderMetricGroup, null);
    }

    private KafkaPartitionSplitReader createReader(
            Properties additionalProperties,
            SourceReaderMetricGroup sourceReaderMetricGroup,
            String rackId) {
        Properties props = new Properties();
        props.putAll(KafkaSourceTestEnv.getConsumerProperties(ByteArrayDeserializer.class));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        if (!additionalProperties.isEmpty()) {
            props.putAll(additionalProperties);
        }
        KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(sourceReaderMetricGroup);
        return new KafkaPartitionSplitReader(
                props,
                new TestingReaderContext(new Configuration(), sourceReaderMetricGroup),
                kafkaSourceReaderMetrics,
                rackId);
    }

    private KafkaPartitionSplitReader createReaderForCommits() {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "PartitionWithoutOffsetCommitGroup");
        // the partitions under test have no records to deliver, so do not wait for them
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "100");
        return createReader(props, UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    }

    private static void assignSplit(KafkaPartitionSplitReader reader, KafkaPartitionSplit split) {
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
    }

    private static Map<TopicPartition, OffsetAndMetadata> commitPartitionsWithoutOffset(
            KafkaPartitionSplitReader reader, TopicPartition tp) throws Exception {
        return commitOffsets(reader, Collections.emptyMap(), Collections.singleton(tp));
    }

    /** Returns the offsets that the reader committed to Kafka for a completed checkpoint. */
    private static Map<TopicPartition, OffsetAndMetadata> commitOffsets(
            KafkaPartitionSplitReader reader,
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
            Set<TopicPartition> partitionsWithoutOffset)
            throws Exception {
        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committed =
                startCommit(reader, offsetsToCommit, partitionsWithoutOffset);
        // the consumer only invokes the callback of a commit sent to Kafka on a later poll
        fetchUntil(reader, committed::isDone, "The offset commit did not complete.");
        return committed.get();
    }

    /** Returns the offsets that the reader commits to Kafka, without polling for them. */
    private static CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> startCommit(
            KafkaPartitionSplitReader reader,
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
            Set<TopicPartition> partitionsWithoutOffset) {
        final CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committed =
                new CompletableFuture<>();
        reader.notifyCheckpointComplete(
                offsetsToCommit,
                partitionsWithoutOffset,
                (offsets, e) -> {
                    if (e != null) {
                        committed.completeExceptionally(e);
                    } else {
                        committed.complete(offsets);
                    }
                });
        return committed;
    }

    private static void fetchUntil(
            KafkaPartitionSplitReader reader, Supplier<Boolean> condition, String errorMsg)
            throws Exception {
        waitUtil(
                () -> {
                    if (!condition.get()) {
                        try {
                            reader.fetch();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    return condition.get();
                },
                Duration.ofSeconds(30),
                errorMsg);
    }

    private static long getCommittedOffset(TopicPartition tp, String groupId) throws Exception {
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            final OffsetAndMetadata committed =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get()
                            .get(tp);
            assertThat(committed).as("No offset was committed for %s", tp).isNotNull();
            return committed.offset();
        }
    }

    /** Writes a transaction with a single record, which takes two offsets with its marker. */
    private static void produceRecordInTransaction(TopicPartition tp) throws Throwable {
        KafkaSourceTestEnv.produceToKafka(
                Collections.singletonList(
                        new ProducerRecord<>(tp.topic(), tp.partition(), tp.toString(), 0)),
                kafkaServer.getTransactionalProducerConfig());
    }

    private Map<String, KafkaPartitionSplit> assignSplits(
            KafkaPartitionSplitReader reader, Map<String, KafkaPartitionSplit> splits) {
        SplitsChange<KafkaPartitionSplit> splitsChange =
                new SplitsAddition<>(new ArrayList<>(splits.values()));
        reader.handleSplitsChanges(splitsChange);
        return splits;
    }

    private boolean verifyConsumed(
            final KafkaPartitionSplit split,
            final long expectedStartingOffset,
            final Collection<ConsumerRecord<byte[], byte[]>> consumed) {
        long expectedOffset = expectedStartingOffset;

        for (ConsumerRecord<byte[], byte[]> record : consumed) {
            int expectedValue = (int) expectedOffset;
            long expectedTimestamp = expectedOffset * 1000L;

            assertThat(deserializer.deserialize(record.topic(), record.value()))
                    .isEqualTo(expectedValue);
            assertThat(record.offset()).isEqualTo(expectedOffset);
            assertThat(record.timestamp()).isEqualTo(expectedTimestamp);

            expectedOffset++;
        }
        if (split.getStoppingOffset().isPresent()) {
            return expectedOffset == split.getStoppingOffset().get();
        } else {
            return false;
        }
    }
}
