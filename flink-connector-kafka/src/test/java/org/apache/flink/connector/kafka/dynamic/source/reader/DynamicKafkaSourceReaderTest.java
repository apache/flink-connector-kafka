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

package org.apache.flink.connector.kafka.dynamic.source.reader;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link org.apache.flink.connector.kafka.dynamic.source.reader.DynamicKafkaSourceReader}.
 */
public class DynamicKafkaSourceReaderTest extends SourceReaderTestBase<DynamicKafkaSourceSplit> {
    private static final String TOPIC = "DynamicKafkaSourceReaderTest";

    // we are testing two clusters and SourceReaderTestBase expects there to be a total of 10 splits
    private static final int NUM_SPLITS_PER_CLUSTER = 5;

    private static String kafkaClusterId0;
    private static String kafkaClusterId1;

    @BeforeAll
    static void beforeAll() throws Throwable {
        DynamicKafkaSourceTestHelper.setup();

        DynamicKafkaSourceTestHelper.createTopic(TOPIC, NUM_SPLITS_PER_CLUSTER, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(
                TOPIC, NUM_SPLITS_PER_CLUSTER, NUM_RECORDS_PER_SPLIT);
        kafkaClusterId0 = DynamicKafkaSourceTestHelper.getKafkaClusterId(0);
        kafkaClusterId1 = DynamicKafkaSourceTestHelper.getKafkaClusterId(1);
    }

    @AfterAll
    static void afterAll() throws Exception {
        DynamicKafkaSourceTestHelper.tearDown();
    }

    @Test
    void testHandleSourceEventWithRemovedMetadataAtStartup() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicKafkaSourceReader<Integer> reader = createReaderWithoutStart(context)) {
            // mock restoring state from Flink runtime
            List<DynamicKafkaSourceSplit> splits =
                    getSplits(
                            getNumSplits(),
                            NUM_RECORDS_PER_SPLIT,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            reader.addSplits(splits);

            // start reader
            reader.start();
            KafkaStream kafkaStream = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);

            // remove cluster 0
            kafkaStream.getClusterMetadataMap().remove(kafkaClusterId0);
            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.singleton(kafkaStream)));

            List<DynamicKafkaSourceSplit> splitsWithoutCluster0 =
                    splits.stream()
                            .filter(split -> !split.getKafkaClusterId().equals(kafkaClusterId0))
                            .collect(Collectors.toList());
            assertThat(reader.snapshotState(-1))
                    .as("The splits should not contain any split related to cluster 0")
                    .containsExactlyInAnyOrderElementsOf(splitsWithoutCluster0);
        }
    }

    @Test
    void testNoSubReadersInputStatus() throws Exception {
        try (DynamicKafkaSourceReader<Integer> reader =
                (DynamicKafkaSourceReader<Integer>) createReader()) {
            TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
            InputStatus inputStatus = reader.pollNext(readerOutput);
            assertEquals(
                    InputStatus.NOTHING_AVAILABLE,
                    inputStatus,
                    "nothing available since there are no sub readers created, there could be sub readers created in the future");

            // notify that this reader will not be assigned anymore splits
            reader.notifyNoMoreSplits();

            inputStatus = reader.pollNext(readerOutput);
            assertEquals(
                    InputStatus.END_OF_INPUT,
                    inputStatus,
                    "there will not be any more input from this reader since there are no splits");
        }
    }

    @Test
    void testNotifyNoMoreSplits() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicKafkaSourceReader<Integer> reader = createReaderWithoutStart(context)) {
            TestingReaderOutput<Integer> readerOutput = new TestingReaderOutput<>();
            reader.start();

            // Splits assigned
            List<DynamicKafkaSourceSplit> splits =
                    getSplits(getNumSplits(), NUM_RECORDS_PER_SPLIT, Boundedness.BOUNDED);
            reader.addSplits(splits);

            // Send no more splits
            reader.notifyNoMoreSplits();

            // Send metadata
            MetadataUpdateEvent metadata =
                    DynamicKafkaSourceTestHelper.getMetadataUpdateEvent(TOPIC);
            reader.handleSourceEvents(metadata);

            // Check consistency
            InputStatus status;
            do {
                status = reader.pollNext(readerOutput);
            } while (status != InputStatus.END_OF_INPUT);

            assertThat(readerOutput.getEmittedRecords())
                    .hasSize(getNumSplits() * NUM_RECORDS_PER_SPLIT);
        }
    }

    @Test
    void testAvailabilityFutureUpdates() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicKafkaSourceReader<Integer> reader = createReaderWithoutStart(context)) {
            CompletableFuture<Void> futureAtInit = reader.isAvailable();
            assertThat(reader.isActivelyConsumingSplits()).isFalse();
            assertThat(futureAtInit)
                    .as("future is not complete at fresh startup since no readers are created")
                    .isNotDone();
            assertThat(reader.getAvailabilityHelperSize()).isZero();

            reader.start();
            MetadataUpdateEvent metadata =
                    DynamicKafkaSourceTestHelper.getMetadataUpdateEvent(TOPIC);
            reader.handleSourceEvents(metadata);
            List<DynamicKafkaSourceSplit> splits =
                    getSplits(
                            getNumSplits(),
                            NUM_RECORDS_PER_SPLIT,
                            Boundedness.CONTINUOUS_UNBOUNDED);
            reader.addSplits(splits);
            CompletableFuture<Void> futureAfterSplitAssignment = reader.isAvailable();

            assertThat(futureAtInit)
                    .as(
                            "New future should have been produced since metadata triggers reader creation")
                    .isNotSameAs(futureAfterSplitAssignment);
            assertThat(reader.getAvailabilityHelperSize()).isEqualTo(2);

            // remove cluster 0
            KafkaStream kafkaStream = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);
            kafkaStream.getClusterMetadataMap().remove(kafkaClusterId0);
            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.singleton(kafkaStream)));

            CompletableFuture<Void> futureAfterRemovingCluster0 = reader.isAvailable();
            assertThat(futureAfterRemovingCluster0)
                    .as("There should new future since the metadata has changed")
                    .isNotSameAs(futureAfterSplitAssignment);
            assertThat(reader.getAvailabilityHelperSize()).isEqualTo(1);
        }
    }

    @Test
    void testReaderMetadataChangeWhenOneTopicChanges() throws Exception {
        try (DynamicKafkaSourceReader<Integer> reader =
                (DynamicKafkaSourceReader<Integer>) createReader()) {

            // splits with offsets
            DynamicKafkaSourceSplit cluster0Split =
                    new DynamicKafkaSourceSplit(
                            DynamicKafkaSourceTestHelper.getKafkaClusterId(0),
                            new KafkaPartitionSplit(new TopicPartition(TOPIC, 0), 10));
            DynamicKafkaSourceSplit cluster1Split =
                    new DynamicKafkaSourceSplit(
                            DynamicKafkaSourceTestHelper.getKafkaClusterId(1),
                            new KafkaPartitionSplit(new TopicPartition(TOPIC, 0), 10));
            reader.addSplits(ImmutableList.of(cluster0Split, cluster1Split));

            // metadata change with a topic changing
            KafkaStream kafkaStream = DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC);
            Set<String> topicsForCluster1 =
                    kafkaStream.getClusterMetadataMap().get(kafkaClusterId1).getTopics();
            topicsForCluster1.clear();
            topicsForCluster1.add("new topic");
            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.singleton(kafkaStream)));
            // same split but earlier offset
            DynamicKafkaSourceSplit newCluster0Split =
                    new DynamicKafkaSourceSplit(
                            kafkaClusterId0,
                            new KafkaPartitionSplit(new TopicPartition(TOPIC, 0), 10));
            // new split
            DynamicKafkaSourceSplit newCluster1Split =
                    new DynamicKafkaSourceSplit(
                            kafkaClusterId1,
                            new KafkaPartitionSplit(new TopicPartition("new topic", 0), 10));
            reader.addSplits(ImmutableList.of(newCluster0Split, newCluster1Split));

            List<DynamicKafkaSourceSplit> assignedSplits = reader.snapshotState(-1);

            assertThat(assignedSplits)
                    .as(
                            "The new split for cluster 1 should be assigned and split for cluster 0 should retain offset 10")
                    .containsExactlyInAnyOrder(cluster0Split, newCluster1Split);
        }
    }

    @Override
    protected SourceReader<Integer, DynamicKafkaSourceSplit> createReader() {
        TestingReaderContext context = new TestingReaderContext();
        return startReader(createReaderWithoutStart(context), context);
    }

    private DynamicKafkaSourceReader<Integer> createReaderWithoutStart(
            TestingReaderContext context) {
        Properties properties = getRequiredProperties();
        return new DynamicKafkaSourceReader<>(
                context,
                KafkaRecordDeserializationSchema.valueOnly(IntegerDeserializer.class),
                properties);
    }

    private SourceReader<Integer, DynamicKafkaSourceSplit> startReader(
            DynamicKafkaSourceReader<Integer> reader, TestingReaderContext context) {
        reader.start();
        assertThat(context.getSentEvents())
                .as("Reader sends GetMetadataUpdateEvent at startup")
                .hasSize(1);
        reader.handleSourceEvents(DynamicKafkaSourceTestHelper.getMetadataUpdateEvent(TOPIC));
        return reader;
    }

    private static Properties getRequiredProperties() {
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        return properties;
    }

    @Override
    protected List<DynamicKafkaSourceSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<DynamicKafkaSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return splits;
    }

    @Override
    protected DynamicKafkaSourceSplit getSplit(
            int splitId, int numRecords, Boundedness boundedness) {
        long stoppingOffset =
                boundedness == Boundedness.BOUNDED
                        ? NUM_RECORDS_PER_SPLIT
                        : KafkaPartitionSplit.NO_STOPPING_OFFSET;

        String kafkaClusterId;
        int splitIdForCluster = splitId % NUM_SPLITS_PER_CLUSTER;
        if (splitId < NUM_SPLITS_PER_CLUSTER) {
            kafkaClusterId = "kafka-cluster-0";
        } else {
            kafkaClusterId = "kafka-cluster-1";
        }

        return new DynamicKafkaSourceSplit(
                kafkaClusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, splitIdForCluster), 0L, stoppingOffset));
    }

    @Override
    protected long getNextRecordIndex(DynamicKafkaSourceSplit split) {
        return split.getKafkaPartitionSplit().getStartingOffset();
    }

    private Map<String, Set<String>> splitsToClusterTopicMap(List<DynamicKafkaSourceSplit> splits) {
        Map<String, Set<String>> clusterTopicMap = new HashMap<>();

        for (DynamicKafkaSourceSplit split : splits) {
            Set<String> topics =
                    clusterTopicMap.computeIfAbsent(
                            split.getKafkaClusterId(), (ignore) -> new HashSet<>());
            topics.add(split.getKafkaPartitionSplit().getTopic());
        }

        return clusterTopicMap;
    }
}

class DynamicKafkaSourceReaderPauseResumeTest {
    private static final String TOPIC = "DynamicKafkaSourceReaderPauseResumeTest";

    @Test
    void testPauseOrResumeRoutesDynamicSplitIdsToOwningCluster() throws Exception {
        try (DynamicKafkaSourceReader<Integer> reader = createReader()) {
            RecordingKafkaSourceReader cluster0Reader = new RecordingKafkaSourceReader();
            RecordingKafkaSourceReader cluster1Reader = new RecordingKafkaSourceReader();
            registerSubReader(reader, "cluster-0", cluster0Reader);
            registerSubReader(reader, "cluster-1", cluster1Reader);
            setActivelyConsumingSplits(reader, true);

            DynamicKafkaSourceSplit cluster0Split = createSplit("cluster-0", TOPIC, 0);
            DynamicKafkaSourceSplit cluster1Split = createSplit("cluster-1", TOPIC, 1);
            reader.addSplits(List.of(cluster0Split, cluster1Split));

            reader.pauseOrResumeSplits(
                    Collections.singleton(cluster0Split.splitId()),
                    Collections.singleton(cluster1Split.splitId()));

            assertThat(cluster0Reader.getPausedSplits()).containsExactly(cluster0Split.splitId());
            assertThat(cluster0Reader.getResumedSplits()).isEmpty();
            assertThat(cluster1Reader.getPausedSplits()).isEmpty();
            assertThat(cluster1Reader.getResumedSplits()).containsExactly(cluster1Split.splitId());
        }
    }

    @Test
    void testPauseOrResumeRoutesOverlappingClusterIdsWithoutPrefixMatching() throws Exception {
        String shortClusterId = "a";
        String overlappingClusterId = shortClusterId + "-" + TOPIC;

        try (DynamicKafkaSourceReader<Integer> reader = createReader()) {
            RecordingKafkaSourceReader shortClusterReader = new RecordingKafkaSourceReader();
            RecordingKafkaSourceReader overlappingClusterReader = new RecordingKafkaSourceReader();
            registerSubReader(reader, shortClusterId, shortClusterReader);
            registerSubReader(reader, overlappingClusterId, overlappingClusterReader);
            setActivelyConsumingSplits(reader, true);

            DynamicKafkaSourceSplit shortClusterSplit = createSplit(shortClusterId, TOPIC, 0);
            DynamicKafkaSourceSplit overlappingClusterSplit =
                    createSplit(overlappingClusterId, TOPIC, 0);
            reader.addSplits(List.of(shortClusterSplit, overlappingClusterSplit));

            reader.pauseOrResumeSplits(
                    Collections.singleton(shortClusterSplit.splitId()),
                    Collections.singleton(overlappingClusterSplit.splitId()));

            assertThat(shortClusterReader.getPausedSplits())
                    .containsExactly(shortClusterSplit.splitId());
            assertThat(shortClusterReader.getResumedSplits()).isEmpty();
            assertThat(overlappingClusterReader.getPausedSplits()).isEmpty();
            assertThat(overlappingClusterReader.getResumedSplits())
                    .containsExactly(overlappingClusterSplit.splitId());
        }
    }

    private static DynamicKafkaSourceReader<Integer> createReader() {
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        return new DynamicKafkaSourceReader<>(
                new TestingReaderContext(),
                KafkaRecordDeserializationSchema.valueOnly(IntegerDeserializer.class),
                properties);
    }

    private static DynamicKafkaSourceSplit createSplit(
            String kafkaClusterId, String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                kafkaClusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, partition),
                        0L,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET));
    }

    @SuppressWarnings("unchecked")
    private static void registerSubReader(
            DynamicKafkaSourceReader<Integer> reader,
            String kafkaClusterId,
            KafkaSourceReader<Integer> subReader)
            throws Exception {
        Field clusterReaderMapField =
                DynamicKafkaSourceReader.class.getDeclaredField("clusterReaderMap");
        clusterReaderMapField.setAccessible(true);
        NavigableMap<String, KafkaSourceReader<Integer>> clusterReaderMap =
                (NavigableMap<String, KafkaSourceReader<Integer>>)
                        clusterReaderMapField.get(reader);
        clusterReaderMap.put(kafkaClusterId, subReader);
    }

    private static void setActivelyConsumingSplits(
            DynamicKafkaSourceReader<Integer> reader, boolean isActivelyConsumingSplits)
            throws Exception {
        Field isActivelyConsumingSplitsField =
                DynamicKafkaSourceReader.class.getDeclaredField("isActivelyConsumingSplits");
        isActivelyConsumingSplitsField.setAccessible(true);
        isActivelyConsumingSplitsField.set(reader, isActivelyConsumingSplits);
    }

    private static final class RecordingKafkaSourceReader extends KafkaSourceReader<Integer> {
        private final Set<String> assignedSplitIds = new HashSet<>();
        private List<String> pausedSplits = Collections.emptyList();
        private List<String> resumedSplits = Collections.emptyList();

        private RecordingKafkaSourceReader() {
            this(new FutureCompletingBlockingQueue<>());
        }

        private RecordingKafkaSourceReader(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                        elementsQueue) {
            super(
                    elementsQueue,
                    new KafkaSourceFetcherManager(
                            elementsQueue, NoOpSplitReader::new, ignored -> {}),
                    noOpRecordEmitter(),
                    new Configuration(),
                    new TestingReaderContext(),
                    new KafkaSourceReaderMetrics(
                            UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
        }

        @Override
        public void addSplits(List<KafkaPartitionSplit> splits) {
            for (KafkaPartitionSplit split : splits) {
                assignedSplitIds.add(split.splitId());
            }
            super.addSplits(splits);
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            pausedSplits = filterAssignedSplits(splitsToPause);
            resumedSplits = filterAssignedSplits(splitsToResume);
        }

        private List<String> filterAssignedSplits(Collection<String> splitIds) {
            List<String> filteredSplitIds = new ArrayList<>();
            for (String splitId : splitIds) {
                if (assignedSplitIds.contains(splitId)) {
                    filteredSplitIds.add(splitId);
                }
            }
            return filteredSplitIds;
        }

        private List<String> getPausedSplits() {
            return pausedSplits;
        }

        private List<String> getResumedSplits() {
            return resumedSplits;
        }

        private static RecordEmitter<
                        ConsumerRecord<byte[], byte[]>, Integer, KafkaPartitionSplitState>
                noOpRecordEmitter() {
            return (record, output, splitState) -> {};
        }
    }

    private static final class NoOpSplitReader
            implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
        @Override
        public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
            return null;
        }

        @Override
        public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {}

        @Override
        public void wakeUp() {}

        @Override
        public void close() {}
    }
}
