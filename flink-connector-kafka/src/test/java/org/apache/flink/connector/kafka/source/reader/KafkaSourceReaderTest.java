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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.KafkaSourceTestUtils;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.COMMITTED_OFFSET_METRIC_GAUGE;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.INITIAL_OFFSET;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.KAFKA_CONSUMER_METRIC_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.PARTITION_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.TOPIC_GROUP;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv.NUM_PARTITIONS;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaSourceReader}. */
public class KafkaSourceReaderTest extends SourceReaderTestBase<KafkaPartitionSplit> {
    private static final String TOPIC = "KafkaSourceReaderTest";

    @BeforeAll
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            adminClient
                    .createTopics(
                            Collections.singleton(new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1)))
                    .all()
                    .get();
            // Use the admin client to trigger the creation of internal __consumer_offsets topic.
            // This makes sure that we won't see unavailable coordinator in the tests.
            waitUtil(
                    () -> {
                        try {
                            adminClient
                                    .listConsumerGroupOffsets("AnyGroup")
                                    .partitionsToOffsetAndMetadata()
                                    .get();
                        } catch (Exception e) {
                            return false;
                        }
                        return true;
                    },
                    Duration.ofSeconds(60),
                    "Waiting for offsets topic creation failed.");
        }
        KafkaSourceTestEnv.produceToKafka(
                getRecords(), StringSerializer.class, IntegerSerializer.class, null);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    protected int getNumSplits() {
        return NUM_PARTITIONS;
    }

    // -----------------------------------------

    @Test
    void testCommitOffsetsWithoutAliveFetchers() throws Exception {
        final String groupId = "testCommitOffsetsWithoutAliveFetchers";
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            KafkaPartitionSplit split =
                    new KafkaPartitionSplit(new TopicPartition(TOPIC, 0), 0, NUM_RECORDS_PER_SPLIT);
            reader.addSplits(Collections.singletonList(split));
            reader.notifyNoMoreSplits();
            ReaderOutput<Integer> output = new TestingReaderOutput<>();
            InputStatus status;
            do {
                status = reader.pollNext(output);
            } while (status != InputStatus.NOTHING_AVAILABLE);
            pollUntil(
                    reader,
                    output,
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
            // Due to a bug in KafkaConsumer, when the consumer closes, the offset commit callback
            // won't be fired, so the offsetsToCommit map won't be cleaned. To make the test
            // stable, we add a split whose starting offset is the log end offset, so the
            // split fetcher won't become idle and exit after commitOffsetAsync is invoked from
            // notifyCheckpointComplete().
            reader.addSplits(
                    Collections.singletonList(
                            new KafkaPartitionSplit(
                                    new TopicPartition(TOPIC, 0), NUM_RECORDS_PER_SPLIT)));
            pollUntil(
                    reader,
                    output,
                    () -> reader.getOffsetsToCommit().isEmpty(),
                    "The offset commit did not finish before timeout.");
        }
        // Verify the committed offsets.
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertThat(committedOffsets).hasSize(1);
            assertThat(committedOffsets.values())
                    .extracting(OffsetAndMetadata::offset)
                    .allMatch(offset -> offset == NUM_RECORDS_PER_SPLIT);
        }
    }

    @Test
    void testCommitEmptyOffsets() throws Exception {
        final String groupId = "testCommitEmptyOffsets";
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            reader.snapshotState(100L);
            reader.snapshotState(101L);
            reader.snapshotState(102L);

            // After each snapshot, a new entry should have been added to the offsets-to-commit
            // cache for the checkpoint
            final Map<Long, Map<TopicPartition, OffsetAndMetadata>> expectedOffsetsToCommit =
                    new HashMap<>();
            expectedOffsetsToCommit.put(100L, new HashMap<>());
            expectedOffsetsToCommit.put(101L, new HashMap<>());
            expectedOffsetsToCommit.put(102L, new HashMap<>());
            assertThat(reader.getOffsetsToCommit()).isEqualTo(expectedOffsetsToCommit);

            // only notify up to checkpoint 101L; all offsets prior to 101L should be evicted from
            // cache, leaving only 102L
            reader.notifyCheckpointComplete(101L);

            final Map<Long, Map<TopicPartition, OffsetAndMetadata>>
                    expectedOffsetsToCommitAfterNotify = new HashMap<>();
            expectedOffsetsToCommitAfterNotify.put(102L, new HashMap<>());
            assertThat(reader.getOffsetsToCommit()).isEqualTo(expectedOffsetsToCommitAfterNotify);
        }
        // Verify the committed offsets.
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertThat(committedOffsets).isEmpty();
        }
    }

    @Test
    void testOffsetCommitOnCheckpointComplete() throws Exception {
        final String groupId = "testOffsetCommitOnCheckpointComplete";
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            reader.addSplits(
                    getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
            } while (output.count() < totalNumRecords);

            // The completion of the last checkpoint should subsume all the previous checkpoitns.
            assertThat(reader.getOffsetsToCommit()).hasSize((int) checkpointId);

            long lastCheckpointId = checkpointId;
            waitUtil(
                    () -> {
                        try {
                            reader.notifyCheckpointComplete(lastCheckpointId);
                        } catch (Exception exception) {
                            throw new RuntimeException(
                                    "Caught unexpected exception when polling from the reader",
                                    exception);
                        }
                        return reader.getOffsetsToCommit().isEmpty();
                    },
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1),
                    "The offset commit did not finish before timeout.");
        }

        // Verify the committed offsets.
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertThat(committedOffsets).hasSize(numSplits);
            assertThat(committedOffsets.values())
                    .extracting(OffsetAndMetadata::offset)
                    .allMatch(offset -> offset == NUM_RECORDS_PER_SPLIT);
        }
    }

    @Test
    void testNotCommitOffsetsForUninitializedSplits() throws Exception {
        final long checkpointId = 1234L;
        try (KafkaSourceReader<Integer> reader = (KafkaSourceReader<Integer>) createReader()) {
            KafkaPartitionSplit split =
                    new KafkaPartitionSplit(
                            new TopicPartition(TOPIC, 0), KafkaPartitionSplit.EARLIEST_OFFSET);
            reader.addSplits(Collections.singletonList(split));
            reader.snapshotState(checkpointId);
            assertThat(reader.getOffsetsToCommit()).hasSize(1);
            assertThat(reader.getOffsetsToCommit().get(checkpointId)).isEmpty();
        }
    }

    @Test
    void testDisableOffsetCommit() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false");
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new TestingReaderContext(),
                                (ignore) -> {},
                                properties,
                                null)) {
            reader.addSplits(
                    getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
                // Offsets to commit should be always empty because offset commit is disabled
                assertThat(reader.getOffsetsToCommit()).isEmpty();
            } while (output.count() < totalNumRecords);
        }
    }

    @Test
    void testKafkaSourceMetrics() throws Exception {
        final MetricListener metricListener = new MetricListener();
        final String groupId = "testKafkaSourceMetrics";
        final TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        final TopicPartition tp1 = new TopicPartition(TOPIC, 1);

        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                groupId,
                                metricListener.getMetricGroup())) {

            KafkaPartitionSplit split0 =
                    new KafkaPartitionSplit(tp0, KafkaPartitionSplit.EARLIEST_OFFSET);
            KafkaPartitionSplit split1 =
                    new KafkaPartitionSplit(tp1, KafkaPartitionSplit.EARLIEST_OFFSET);
            reader.addSplits(Arrays.asList(split0, split1));

            TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
            pollUntil(
                    reader,
                    output,
                    () -> output.getEmittedRecords().size() == NUM_RECORDS_PER_SPLIT * 2,
                    String.format(
                            "Failed to poll %d records until timeout", NUM_RECORDS_PER_SPLIT * 2));

            // Metric "records-consumed-total" of KafkaConsumer should be NUM_RECORDS_PER_SPLIT
            assertThat(getKafkaConsumerMetric("records-consumed-total", metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT * 2);

            // Current consuming offset should be NUM_RECORD_PER_SPLIT - 1
            assertThat(getCurrentOffsetMetric(tp0, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT - 1);
            assertThat(getCurrentOffsetMetric(tp1, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT - 1);

            // No offset is committed till now
            assertThat(getCommittedOffsetMetric(tp0, metricListener)).isEqualTo(INITIAL_OFFSET);
            assertThat(getCommittedOffsetMetric(tp1, metricListener)).isEqualTo(INITIAL_OFFSET);

            // Trigger offset commit
            final long checkpointId = 15213L;
            reader.snapshotState(checkpointId);
            waitUtil(
                    () -> {
                        try {
                            reader.notifyCheckpointComplete(checkpointId);
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to notify checkpoint complete to reader", e);
                        }
                        return reader.getOffsetsToCommit().isEmpty();
                    },
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1),
                    String.format(
                            "Offsets are not committed successfully. Dangling offsets: %s",
                            reader.getOffsetsToCommit()));

            // Metric "commit-total" of KafkaConsumer should be greater than 0
            // It's hard to know the exactly number of commit because of the retry
            assertThat(getKafkaConsumerMetric("commit-total", metricListener)).isGreaterThan(0L);

            // Committed offset should be NUM_RECORD_PER_SPLIT
            assertThat(getCommittedOffsetMetric(tp0, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT);
            assertThat(getCommittedOffsetMetric(tp1, metricListener))
                    .isEqualTo(NUM_RECORDS_PER_SPLIT);

            // Number of successful commits should be greater than 0
            final Optional<Counter> commitsSucceeded =
                    metricListener.getCounter(
                            KAFKA_SOURCE_READER_METRIC_GROUP, COMMITS_SUCCEEDED_METRIC_COUNTER);
            assertThat(commitsSucceeded).isPresent();
            assertThat(commitsSucceeded.get().getCount()).isGreaterThan(0L);
        }
    }

    @Test
    void testAssigningEmptySplits() throws Exception {
        // Normal split with NUM_RECORDS_PER_SPLIT records
        final KafkaPartitionSplit normalSplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, 0),
                        0,
                        KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION);
        // Empty split with no record
        final KafkaPartitionSplit emptySplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, 1), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        // Split finished hook for listening finished splits
        final Set<String> finishedSplits = new HashSet<>();
        final Consumer<Collection<String>> splitFinishedHook = finishedSplits::addAll;

        try (final KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.BOUNDED,
                                "KafkaSourceReaderTestGroup",
                                new TestingReaderContext(),
                                splitFinishedHook)) {
            reader.addSplits(Arrays.asList(normalSplit, emptySplit));
            pollUntil(
                    reader,
                    new TestingReaderOutput<>(),
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            assertThat(finishedSplits)
                    .containsExactlyInAnyOrder(
                            KafkaPartitionSplit.toSplitId(normalSplit.getTopicPartition()),
                            KafkaPartitionSplit.toSplitId(emptySplit.getTopicPartition()));
        }
    }

    @Test
    void testAssigningEmptySplitOnly() throws Exception {
        // Empty split with no record
        KafkaPartitionSplit emptySplit0 =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, 0), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        KafkaPartitionSplit emptySplit1 =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, 1), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        // Split finished hook for listening finished splits
        final Set<String> finishedSplits = new HashSet<>();
        final Consumer<Collection<String>> splitFinishedHook = finishedSplits::addAll;

        try (final KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.BOUNDED,
                                "KafkaSourceReaderTestGroup",
                                new TestingReaderContext(),
                                splitFinishedHook)) {
            reader.addSplits(Arrays.asList(emptySplit0, emptySplit1));
            pollUntil(
                    reader,
                    new TestingReaderOutput<>(),
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            assertThat(reader.getNumAliveFetchers()).isEqualTo(0);
            assertThat(finishedSplits)
                    .containsExactly(emptySplit0.splitId(), emptySplit1.splitId());
        }
    }

    @Test
    public void testSupportsPausingOrResumingSplits() throws Exception {
        final Set<String> finishedSplits = new HashSet<>();

        try (final KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.BOUNDED,
                                "groupId",
                                new TestingReaderContext(),
                                finishedSplits::addAll)) {
            KafkaPartitionSplit split1 =
                    new KafkaPartitionSplit(new TopicPartition(TOPIC, 0), 0, NUM_RECORDS_PER_SPLIT);
            KafkaPartitionSplit split2 =
                    new KafkaPartitionSplit(new TopicPartition(TOPIC, 1), 0, NUM_RECORDS_PER_SPLIT);
            reader.addSplits(Arrays.asList(split1, split2));

            TestingReaderOutput<Integer> output = new TestingReaderOutput<>();

            reader.pauseOrResumeSplits(
                    Collections.singleton(split1.splitId()), Collections.emptyList());

            pollUntil(
                    reader,
                    output,
                    () ->
                            finishedSplits.contains(split2.splitId())
                                    && output.getEmittedRecords().size() == NUM_RECORDS_PER_SPLIT,
                    "The split fetcher did not exit before timeout.");

            reader.pauseOrResumeSplits(
                    Collections.emptyList(), Collections.singleton(split1.splitId()));

            pollUntil(
                    reader,
                    output,
                    () ->
                            finishedSplits.contains(split1.splitId())
                                    && output.getEmittedRecords().size()
                                            == NUM_RECORDS_PER_SPLIT * 2,
                    "The split fetcher did not exit before timeout.");

            assertThat(finishedSplits).containsExactly(split1.splitId(), split2.splitId());
        }
    }

    @Test
    public void testThatReaderDoesNotCallRackIdSupplierOnInit() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        SerializableSupplier<String> rackIdSupplier =
                () -> {
                    called.set(true);
                    return "dummy";
                };

        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new TestingReaderContext(),
                                (ignore) -> {},
                                new Properties(),
                                rackIdSupplier)) {
            // Do nothing here
        }

        assertThat(called).isFalse();
    }

    @Test
    public void testThatReaderDoesCallRackIdSupplierOnSplitAssignment() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        SerializableSupplier<String> rackIdSupplier =
                () -> {
                    called.set(true);
                    return "use1-az1";
                };

        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new TestingReaderContext(),
                                (ignore) -> {},
                                new Properties(),
                                rackIdSupplier)) {
            reader.addSplits(
                    Collections.singletonList(
                            new KafkaPartitionSplit(new TopicPartition(TOPIC, 1), 1L)));
        }

        assertThat(called).isTrue();
    }

    // ------------------------------------------

    @Override
    protected SourceReader<Integer, KafkaPartitionSplit> createReader() throws Exception {
        return createReader(Boundedness.BOUNDED, "KafkaSourceReaderTestGroup");
    }

    @Override
    protected List<KafkaPartitionSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<KafkaPartitionSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return splits;
    }

    @Override
    protected KafkaPartitionSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
        long stoppingOffset =
                boundedness == Boundedness.BOUNDED
                        ? NUM_RECORDS_PER_SPLIT
                        : KafkaPartitionSplit.NO_STOPPING_OFFSET;
        return new KafkaPartitionSplit(new TopicPartition(TOPIC, splitId), 0L, stoppingOffset);
    }

    @Override
    protected long getNextRecordIndex(KafkaPartitionSplit split) {
        return split.getStartingOffset();
    }

    // ---------------------

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness, String groupId) throws Exception {
        return createReader(boundedness, groupId, new TestingReaderContext(), (ignore) -> {});
    }

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness, String groupId, MetricGroup metricGroup) throws Exception {
        return createReader(
                boundedness,
                groupId,
                new TestingReaderContext(
                        new Configuration(), InternalSourceReaderMetricGroup.mock(metricGroup)),
                (ignore) -> {});
    }

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness,
            String groupId,
            SourceReaderContext context,
            Consumer<Collection<String>> splitFinishedHook)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return createReader(boundedness, context, splitFinishedHook, properties, null);
    }

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness,
            SourceReaderContext context,
            Consumer<Collection<String>> splitFinishedHook,
            Properties props,
            SerializableSupplier<String> rackIdSupplier)
            throws Exception {
        KafkaSourceBuilder<Integer> builder =
                KafkaSource.<Integer>builder()
                        .setClientIdPrefix("KafkaSourceReaderTest")
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        IntegerDeserializer.class))
                        .setPartitions(Collections.singleton(new TopicPartition("AnyTopic", 0)))
                        .setProperty(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                KafkaSourceTestEnv.brokerConnectionStrings)
                        .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        .setProperties(props);
        if (boundedness == Boundedness.BOUNDED) {
            builder.setBounded(OffsetsInitializer.latest());
        }
        if (rackIdSupplier != null) {
            builder.setRackIdSupplier(rackIdSupplier);
        }

        return KafkaSourceTestUtils.createReaderWithFinishedSplitHook(
                builder.build(), context, splitFinishedHook);
    }

    private void pollUntil(
            KafkaSourceReader<Integer> reader,
            ReaderOutput<Integer> output,
            Supplier<Boolean> condition,
            String errorMessage)
            throws Exception {
        waitUtil(
                () -> {
                    try {
                        reader.pollNext(output);
                    } catch (Exception exception) {
                        throw new RuntimeException(
                                "Caught unexpected exception when polling from the reader",
                                exception);
                    }
                    return condition.get();
                },
                Duration.ofSeconds(60),
                errorMessage);
    }

    private long getKafkaConsumerMetric(String name, MetricListener listener) {
        final Optional<Gauge<Object>> kafkaConsumerGauge =
                listener.getGauge(
                        KAFKA_SOURCE_READER_METRIC_GROUP, KAFKA_CONSUMER_METRIC_GROUP, name);
        assertThat(kafkaConsumerGauge).isPresent();
        return ((Double) kafkaConsumerGauge.get().getValue()).longValue();
    }

    private long getCurrentOffsetMetric(TopicPartition tp, MetricListener listener) {
        final Optional<Gauge<Object>> currentOffsetGauge =
                listener.getGauge(
                        KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        CURRENT_OFFSET_METRIC_GAUGE);
        assertThat(currentOffsetGauge).isPresent();
        return (long) currentOffsetGauge.get().getValue();
    }

    private long getCommittedOffsetMetric(TopicPartition tp, MetricListener listener) {
        final Optional<Gauge<Object>> committedOffsetGauge =
                listener.getGauge(
                        KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        COMMITTED_OFFSET_METRIC_GAUGE);
        assertThat(committedOffsetGauge).isPresent();
        return (long) committedOffsetGauge.get().getValue();
    }

    // ---------------------

    private static List<ProducerRecord<String, Integer>> getRecords() {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int part = 0; part < NUM_PARTITIONS; part++) {
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                records.add(
                        new ProducerRecord<>(
                                TOPIC, part, TOPIC + "-" + part, part * NUM_RECORDS_PER_SPLIT + i));
            }
        }
        return records;
    }
}
