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

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.metadata.SingleClusterTopicMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumState;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumerator;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.dynamic.source.testutils.DynamicKafkaSourceEnumStateTestUtils;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.testutils.DynamicKafkaSourceExternalContextFactory;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.connector.kafka.testutils.TwoKafkaContainers;
import org.apache.flink.connector.kafka.testutils.YamlFileMetadataService;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.dynamic.source.metrics.KafkaClusterMetricGroup.DYNAMIC_KAFKA_SOURCE_METRIC_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource}.
 */
public class DynamicKafkaSourceITTest {

    private static final String TOPIC = "DynamicKafkaSourceITTest";
    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_RECORDS_PER_SPLIT = 5;

    private static KafkaTestBase.KafkaClusterTestEnvMetadata kafkaClusterTestEnvMetadata0;
    private static KafkaTestBase.KafkaClusterTestEnvMetadata kafkaClusterTestEnvMetadata1;
    MiniClusterWithClientResource miniClusterResource;
    static InMemoryReporter reporter;

    @TempDir File testDir;

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DynamicKafkaSourceSpecificTests {
        @BeforeAll
        void beforeAll() throws Throwable {
            DynamicKafkaSourceTestHelper.setup();
            DynamicKafkaSourceTestHelper.createTopic(TOPIC, NUM_PARTITIONS, 1);
            DynamicKafkaSourceTestHelper.produceToKafka(
                    TOPIC, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT);

            kafkaClusterTestEnvMetadata0 =
                    DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(0);
            kafkaClusterTestEnvMetadata1 =
                    DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(1);
        }

        @BeforeEach
        void beforeEach() throws Exception {
            reporter = InMemoryReporter.create();
            miniClusterResource =
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(2)
                                    .setConfiguration(
                                            reporter.addToConfiguration(new Configuration()))
                                    .build());
            miniClusterResource.before();
        }

        @AfterEach
        void afterEach() {
            reporter.close();
            miniClusterResource.after();
        }

        @AfterAll
        void afterAll() throws Exception {
            DynamicKafkaSourceTestHelper.tearDown();
        }

        @Test
        void testBasicMultiClusterRead() throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
            MockKafkaMetadataService mockKafkaMetadataService =
                    new MockKafkaMetadataService(
                            Collections.singleton(
                                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC)));

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(
                                    mockKafkaMetadataService.getAllStreams().stream()
                                            .map(KafkaStream::getStreamId)
                                            .collect(Collectors.toSet()))
                            .setKafkaMetadataService(mockKafkaMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            CloseableIterator<Integer> iterator = stream.executeAndCollect();
            List<Integer> results = new ArrayList<>();
            while (results.size()
                            < DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS
                                    * NUM_PARTITIONS
                                    * NUM_RECORDS_PER_SPLIT
                    && iterator.hasNext()) {
                results.add(iterator.next());
            }

            iterator.close();

            // check that all test records have been consumed
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(
                                            0,
                                            DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS
                                                    * NUM_PARTITIONS
                                                    * NUM_RECORDS_PER_SPLIT)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testSingleClusterTopicMetadataService() throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");

            KafkaMetadataService kafkaMetadataService =
                    new SingleClusterTopicMetadataService(
                            kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                            kafkaClusterTestEnvMetadata0.getStandardProperties());

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(
                                    // use topics as stream ids
                                    Collections.singleton(TOPIC))
                            .setKafkaMetadataService(kafkaMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            CloseableIterator<Integer> iterator = stream.executeAndCollect();
            List<Integer> results = new ArrayList<>();
            while (results.size() < NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT && iterator.hasNext()) {
                results.add(iterator.next());
            }

            iterator.close();

            // check that all test records have been consumed
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testPerClusterOffsetsInitializersInUnboundedMode() throws Throwable {
            String topic = "test-per-cluster-unbounded-offsets";
            DynamicKafkaSourceTestHelper.createTopic(0, topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(1, topic, NUM_PARTITIONS);

            int cluster0Start = 0;
            int cluster0End =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            0, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, cluster0Start);
            int cluster1Start = cluster0End + 1000;
            int cluster1InitialEnd =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            1, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, cluster1Start);
            int cluster1ExtraStart = cluster1InitialEnd + 1000;
            AtomicInteger cluster1ExtraEnd = new AtomicInteger(-1);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");

            KafkaStream kafkaStream =
                    new KafkaStream(
                            "test-per-cluster-unbounded-stream",
                            ImmutableMap.of(
                                    kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                                    new ClusterMetadata(
                                            Collections.singleton(topic),
                                            kafkaClusterTestEnvMetadata0.getStandardProperties(),
                                            OffsetsInitializer.earliest(),
                                            null),
                                    kafkaClusterTestEnvMetadata1.getKafkaClusterId(),
                                    new ClusterMetadata(
                                            Collections.singleton(topic),
                                            kafkaClusterTestEnvMetadata1.getStandardProperties(),
                                            OffsetsInitializer.latest(),
                                            null)));

            MockKafkaMetadataService mockKafkaMetadataService =
                    new MockKafkaMetadataService(Collections.singleton(kafkaStream));

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(kafkaStream.getStreamId()))
                            .setKafkaMetadataService(mockKafkaMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");

            List<Integer> results = new ArrayList<>();
            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                results.add(iterator.next());
                                if (cluster1ExtraEnd.get() < 0) {
                                    cluster1ExtraEnd.set(
                                            DynamicKafkaSourceTestHelper.produceToKafka(
                                                    1,
                                                    topic,
                                                    NUM_PARTITIONS,
                                                    NUM_RECORDS_PER_SPLIT,
                                                    cluster1ExtraStart));
                                }
                            } catch (NoSuchElementException e) {
                                // swallow and wait
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }

                            if (cluster1ExtraEnd.get() < 0) {
                                return false;
                            }

                            int expectedCount =
                                    (cluster0End - cluster0Start)
                                            + (cluster1ExtraEnd.get() - cluster1ExtraStart);
                            return results.size() == expectedCount;
                        },
                        Duration.ofSeconds(15),
                        "Could not obtain the required records within the timeout");
            }

            List<Integer> expectedResults =
                    Stream.concat(
                                    IntStream.range(cluster0Start, cluster0End).boxed(),
                                    IntStream.range(cluster1ExtraStart, cluster1ExtraEnd.get())
                                            .boxed())
                            .collect(Collectors.toList());
            assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
        }

        @Test
        void testPerClusterOffsetsInitializersInBoundedMode() throws Throwable {
            String topic = "test-per-cluster-offsets-initializers";
            DynamicKafkaSourceTestHelper.createTopic(0, topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(1, topic, NUM_PARTITIONS);

            int cluster0Start = 0;
            int cluster0End =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            0, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, cluster0Start);
            int cluster1Start = cluster0End + 1000;
            DynamicKafkaSourceTestHelper.produceToKafka(
                    1, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, cluster1Start);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);

            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");

            Map<TopicPartition, Long> cluster1StoppingOffsets =
                    IntStream.range(0, NUM_PARTITIONS)
                            .boxed()
                            .collect(
                                    Collectors.toMap(
                                            partition -> new TopicPartition(topic, partition),
                                            partition -> 0L));

            KafkaStream kafkaStream =
                    new KafkaStream(
                            "test-per-cluster-offsets-stream",
                            ImmutableMap.of(
                                    kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                                    new ClusterMetadata(
                                            Collections.singleton(topic),
                                            kafkaClusterTestEnvMetadata0.getStandardProperties(),
                                            OffsetsInitializer.earliest(),
                                            OffsetsInitializer.latest()),
                                    kafkaClusterTestEnvMetadata1.getKafkaClusterId(),
                                    new ClusterMetadata(
                                            Collections.singleton(topic),
                                            kafkaClusterTestEnvMetadata1.getStandardProperties(),
                                            OffsetsInitializer.earliest(),
                                            OffsetsInitializer.offsets(cluster1StoppingOffsets))));

            MockKafkaMetadataService mockKafkaMetadataService =
                    new MockKafkaMetadataService(Collections.singleton(kafkaStream));

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(kafkaStream.getStreamId()))
                            .setKafkaMetadataService(mockKafkaMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setBounded(OffsetsInitializer.latest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");

            List<Integer> results = new ArrayList<>();
            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                while (iterator.hasNext()) {
                    results.add(iterator.next());
                }
            }

            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(cluster0Start, cluster0End)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testRestoreFromV1EnumeratorState() throws Throwable {
            String topic = "test-v1-enum-state-restore";
            DynamicKafkaSourceTestHelper.createTopic(0, topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.produceToKafka(
                    0, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);

            String streamId = "test-v1-enum-stream";
            String clusterId = kafkaClusterTestEnvMetadata0.getKafkaClusterId();
            Properties clusterProperties = kafkaClusterTestEnvMetadata0.getStandardProperties();
            String bootstrapServers = kafkaClusterTestEnvMetadata0.getBrokerConnectionStrings();
            clusterProperties.setProperty(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            byte[] serializedState =
                    DynamicKafkaSourceEnumStateTestUtils.serializeV1State(
                            streamId, clusterId, Collections.singleton(topic), bootstrapServers);
            DynamicKafkaSourceEnumState restoredState =
                    new DynamicKafkaSourceEnumStateSerializer().deserialize(1, serializedState);

            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");

            KafkaStream kafkaStream =
                    new KafkaStream(
                            streamId,
                            Collections.singletonMap(
                                    clusterId,
                                    new ClusterMetadata(
                                            Collections.singleton(topic), clusterProperties)));
            MockKafkaMetadataService mockKafkaMetadataService =
                    new MockKafkaMetadataService(Collections.singleton(kafkaStream));

            try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                            new MockSplitEnumeratorContext<>(2);
                    DynamicKafkaSourceEnumerator enumerator =
                            new DynamicKafkaSourceEnumerator(
                                    new KafkaStreamSetSubscriber(Collections.singleton(streamId)),
                                    mockKafkaMetadataService,
                                    context,
                                    OffsetsInitializer.earliest(),
                                    new NoStoppingOffsetsInitializer(),
                                    properties,
                                    Boundedness.CONTINUOUS_UNBOUNDED,
                                    restoredState)) {
                enumerator.start();
                registerReader(context, enumerator, 0);
                registerReader(context, enumerator, 1);
                runAllOneTimeCallables(context);

                List<DynamicKafkaSourceSplit> assignedSplits =
                        context.getSplitsAssignmentSequence().stream()
                                .map(SplitsAssignment::assignment)
                                .flatMap(assignments -> assignments.values().stream())
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());

                assertThat(assignedSplits).isNotEmpty();
                assertThat(assignedSplits)
                        .allSatisfy(
                                split ->
                                        assertThat(split.getKafkaClusterId()).isEqualTo(clusterId));
                assertThat(assignedSplits)
                        .allSatisfy(
                                split ->
                                        assertThat(
                                                        split.getKafkaPartitionSplit()
                                                                .getTopicPartition()
                                                                .topic())
                                                .isEqualTo(topic));

                DynamicKafkaSourceEnumState snapshot = enumerator.snapshotState(1L);
                ClusterMetadata snapshotMetadata =
                        snapshot.getKafkaStreams().stream()
                                .filter(stream -> stream.getStreamId().equals(streamId))
                                .findFirst()
                                .orElseThrow()
                                .getClusterMetadataMap()
                                .get(clusterId);
                assertThat(snapshotMetadata.getStartingOffsetsInitializer()).isNull();
                assertThat(snapshotMetadata.getStoppingOffsetsInitializer()).isNull();
            }
        }

        @Test
        void testMigrationUsingFileMetadataService() throws Throwable {
            // setup topics on two clusters
            String fixedTopic = "test-file-metadata-service";
            DynamicKafkaSourceTestHelper.createTopic(fixedTopic, NUM_PARTITIONS);

            // Flink job config and env
            Configuration configuration = new Configuration();
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(2);
            Properties properties = new Properties();
            properties.setProperty(
                    KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "dynamic-kafka-src");

            // create new metadata file to consume from 1 cluster
            String testStreamId = "test-file-metadata-service-stream";
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));
            writeClusterMetadataToFile(
                    metadataFile,
                    testStreamId,
                    fixedTopic,
                    ImmutableList.of(
                            DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(0)));

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(testStreamId))
                            .setKafkaMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            List<Integer> results = new ArrayList<>();

            AtomicInteger latestValueOffset =
                    new AtomicInteger(
                            DynamicKafkaSourceTestHelper.produceToKafka(
                                    0, fixedTopic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0));

            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                results.add(iterator.next());

                                // trigger metadata update to consume from two clusters
                                if (results.size() == NUM_RECORDS_PER_SPLIT) {
                                    latestValueOffset.set(
                                            DynamicKafkaSourceTestHelper.produceToKafka(
                                                    0,
                                                    fixedTopic,
                                                    NUM_PARTITIONS,
                                                    NUM_RECORDS_PER_SPLIT,
                                                    latestValueOffset.get()));
                                    writeClusterMetadataToFile(
                                            metadataFile,
                                            testStreamId,
                                            fixedTopic,
                                            ImmutableList.of(
                                                    DynamicKafkaSourceTestHelper
                                                            .getKafkaClusterTestEnvMetadata(0),
                                                    DynamicKafkaSourceTestHelper
                                                            .getKafkaClusterTestEnvMetadata(1)));
                                }

                                // trigger another metadata update to remove old cluster
                                if (results.size() == latestValueOffset.get()) {
                                    latestValueOffset.set(
                                            DynamicKafkaSourceTestHelper.produceToKafka(
                                                    1,
                                                    fixedTopic,
                                                    NUM_PARTITIONS,
                                                    NUM_RECORDS_PER_SPLIT,
                                                    latestValueOffset.get()));
                                    writeClusterMetadataToFile(
                                            metadataFile,
                                            testStreamId,
                                            fixedTopic,
                                            ImmutableList.of(
                                                    DynamicKafkaSourceTestHelper
                                                            .getKafkaClusterTestEnvMetadata(1)));
                                }
                            } catch (NoSuchElementException e) {
                                // swallow and wait
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }

                            // we will produce 3x
                            return results.size() == NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 3;
                        },
                        Duration.ofSeconds(15),
                        "Could not schedule callable within timeout");
            }

            // verify no data loss / duplication in metadata changes
            // cluster0 contains 0-10
            // cluster 1 contains 10-30
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 3)
                                    .boxed()
                                    .collect(Collectors.toList()));
        }

        @Test
        void testTopicReAddMigrationUsingFileMetadataService() throws Throwable {
            // setup topics
            int kafkaClusterIdx = 0;
            String topic1 = "test-topic-re-add-1";
            String topic2 = "test-topic-re-add-2";
            DynamicKafkaSourceTestHelper.createTopic(kafkaClusterIdx, topic1, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(kafkaClusterIdx, topic2, NUM_PARTITIONS);

            // Flink job config and env
            Configuration configuration = new Configuration();
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(2);
            Properties properties = new Properties();
            properties.setProperty(
                    KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "dynamic-kafka-src");

            // create new metadata file to consume from 1 cluster
            String testStreamId = "test-topic-re-add-stream";
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));
            writeClusterMetadataToFile(
                    metadataFile,
                    testStreamId,
                    ImmutableList.of(topic1),
                    ImmutableList.of(
                            DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(
                                    kafkaClusterIdx)));

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(testStreamId))
                            .setKafkaMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            List<Integer> results = new ArrayList<>();

            int stage1Records =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            kafkaClusterIdx, topic1, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);
            int stage2Records =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            kafkaClusterIdx,
                            topic2,
                            NUM_PARTITIONS,
                            NUM_RECORDS_PER_SPLIT,
                            stage1Records);

            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                results.add(iterator.next());

                                // switch to second topic after first is read
                                if (results.size() == stage1Records) {
                                    writeClusterMetadataToFile(
                                            metadataFile,
                                            testStreamId,
                                            ImmutableList.of(topic2),
                                            ImmutableList.of(
                                                    DynamicKafkaSourceTestHelper
                                                            .getKafkaClusterTestEnvMetadata(
                                                                    kafkaClusterIdx)));
                                }

                                // re-add first topic again after second is read
                                // produce another batch to first topic
                                if (results.size() == stage2Records) {
                                    DynamicKafkaSourceTestHelper.produceToKafka(
                                            kafkaClusterIdx,
                                            topic1,
                                            NUM_PARTITIONS,
                                            NUM_RECORDS_PER_SPLIT,
                                            stage2Records);
                                    writeClusterMetadataToFile(
                                            metadataFile,
                                            testStreamId,
                                            ImmutableList.of(topic1, topic2),
                                            ImmutableList.of(
                                                    DynamicKafkaSourceTestHelper
                                                            .getKafkaClusterTestEnvMetadata(
                                                                    kafkaClusterIdx)));
                                }
                            } catch (NoSuchElementException e) {
                                // swallow and wait
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }

                            // first batch of topic 1 * 2 + topic 2 + second batch of topic 1
                            return results.size() == NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 4;
                        },
                        Duration.ofSeconds(15),
                        "Could not schedule callable within timeout");
            }

            // verify data
            Stream<Integer> expectedFullRead =
                    IntStream.range(0, NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 3).boxed();
            Stream<Integer> expectedReRead =
                    IntStream.range(0, NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT).boxed();
            List<Integer> expectedResults =
                    Stream.concat(expectedFullRead, expectedReRead).collect(Collectors.toList());
            assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
        }

        @Test
        void testStreamPatternSubscriber() throws Throwable {
            DynamicKafkaSourceTestHelper.createTopic(0, "stream-pattern-test-1", NUM_PARTITIONS);
            int lastValueOffset =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            0, "stream-pattern-test-1", NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);
            DynamicKafkaSourceTestHelper.createTopic(0, "stream-pattern-test-2", NUM_PARTITIONS);
            lastValueOffset =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            0,
                            "stream-pattern-test-2",
                            NUM_PARTITIONS,
                            NUM_RECORDS_PER_SPLIT,
                            lastValueOffset);
            DynamicKafkaSourceTestHelper.createTopic(1, "stream-pattern-test-3", NUM_PARTITIONS);
            final int totalRecords =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            1,
                            "stream-pattern-test-3",
                            NUM_PARTITIONS,
                            NUM_RECORDS_PER_SPLIT,
                            lastValueOffset);

            // create new metadata file to consume from 1 cluster
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));

            Set<KafkaStream> kafkaStreams =
                    getKafkaStreams(
                            kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                            kafkaClusterTestEnvMetadata0.getStandardProperties(),
                            ImmutableSet.of("stream-pattern-test-1", "stream-pattern-test-2"));

            writeClusterMetadataToFile(metadataFile, kafkaStreams);

            // Flink job config and env
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            Properties properties = new Properties();
            properties.setProperty(
                    KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "dynamic-kafka-src");

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamPattern(Pattern.compile("stream-pattern-test-.+"))
                            .setKafkaMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");
            List<Integer> results = new ArrayList<>();

            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                Integer record = iterator.next();
                                results.add(record);

                                // add third stream that matches the regex
                                if (results.size() == NUM_RECORDS_PER_SPLIT) {
                                    kafkaStreams.add(
                                            getKafkaStream(
                                                    kafkaClusterTestEnvMetadata1
                                                            .getKafkaClusterId(),
                                                    kafkaClusterTestEnvMetadata1
                                                            .getStandardProperties(),
                                                    "stream-pattern-test-3"));
                                    writeClusterMetadataToFile(metadataFile, kafkaStreams);
                                }
                            } catch (NoSuchElementException e) {
                                // swallow
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }

                            return results.size() == totalRecords;
                        },
                        Duration.ofSeconds(15),
                        "Could not obtain the required records within the timeout");
            }
            // verify no data loss / duplication in metadata changes
            assertThat(results)
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, totalRecords).boxed().collect(Collectors.toList()));
        }

        @Test
        void testMetricsLifecycleManagement() throws Throwable {
            // setup topics on two clusters
            String fixedTopic = "test-metrics-lifecycle-mgmt";
            DynamicKafkaSourceTestHelper.createTopic(fixedTopic, NUM_PARTITIONS);

            // Flink job config and env
            Configuration configuration = new Configuration();
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(2);
            Properties properties = new Properties();
            properties.setProperty(
                    KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "1000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "5000");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(
                    CommonClientConfigs.GROUP_ID_CONFIG, "testMetricsLifecycleManagement");

            // create new metadata file to consume from 1 cluster
            String testStreamId = "test-file-metadata-service-stream";
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));
            writeClusterMetadataToFile(
                    metadataFile,
                    testStreamId,
                    fixedTopic,
                    ImmutableList.of(
                            DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(0)));

            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(testStreamId))
                            .setKafkaMetadataService(yamlFileMetadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            DataStreamSource<Integer> stream =
                    env.fromSource(
                            dynamicKafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "dynamic-kafka-src");

            int latestValueOffset =
                    DynamicKafkaSourceTestHelper.produceToKafka(
                            0, fixedTopic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);
            List<Integer> results = new ArrayList<>();
            try (CloseableIterator<Integer> iterator = stream.executeAndCollect()) {
                while (results.size() < latestValueOffset && iterator.hasNext()) {
                    results.add(iterator.next());
                }

                assertThat(results)
                        .containsOnlyOnceElementsOf(
                                IntStream.range(0, latestValueOffset)
                                        .boxed()
                                        .collect(Collectors.toList()));

                // should contain cluster 0 metrics
                assertThat(findMetrics(reporter, DYNAMIC_KAFKA_SOURCE_METRIC_GROUP))
                        .allSatisfy(
                                metricName ->
                                        assertThat(metricName)
                                                .containsPattern(
                                                        ".*"
                                                                + DYNAMIC_KAFKA_SOURCE_METRIC_GROUP
                                                                + "\\.kafkaCluster\\.kafka-cluster-0.*"));

                // setup test data for cluster 1 and stop consuming from cluster 0
                latestValueOffset =
                        DynamicKafkaSourceTestHelper.produceToKafka(
                                1,
                                fixedTopic,
                                NUM_PARTITIONS,
                                NUM_RECORDS_PER_SPLIT,
                                latestValueOffset);
                writeClusterMetadataToFile(
                        metadataFile,
                        testStreamId,
                        fixedTopic,
                        ImmutableList.of(
                                DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(1)));
                while (results.size() < latestValueOffset && iterator.hasNext()) {
                    results.add(iterator.next());
                }

                // cluster 0 is not being consumed from, metrics should not appear
                assertThat(findMetrics(reporter, DYNAMIC_KAFKA_SOURCE_METRIC_GROUP))
                        .allSatisfy(
                                metricName ->
                                        assertThat(metricName)
                                                .doesNotContainPattern(
                                                        ".*"
                                                                + DYNAMIC_KAFKA_SOURCE_METRIC_GROUP
                                                                + "\\.kafkaCluster\\.kafka-cluster-0.*"));

                assertThat(findMetrics(reporter, DYNAMIC_KAFKA_SOURCE_METRIC_GROUP))
                        .allSatisfy(
                                metricName ->
                                        assertThat(metricName)
                                                .containsPattern(
                                                        ".*"
                                                                + DYNAMIC_KAFKA_SOURCE_METRIC_GROUP
                                                                + "\\.kafkaCluster\\.kafka-cluster-1.*"));
            }
        }

        private void writeClusterMetadataToFile(File metadataFile, Set<KafkaStream> kafkaStreams)
                throws IOException {
            List<YamlFileMetadataService.StreamMetadata> streamMetadataList = new ArrayList<>();
            for (KafkaStream kafkaStream : kafkaStreams) {
                List<YamlFileMetadataService.StreamMetadata.ClusterMetadata> clusterMetadataList =
                        new ArrayList<>();

                for (Map.Entry<String, ClusterMetadata> entry :
                        kafkaStream.getClusterMetadataMap().entrySet()) {
                    YamlFileMetadataService.StreamMetadata.ClusterMetadata clusterMetadata =
                            new YamlFileMetadataService.StreamMetadata.ClusterMetadata();
                    clusterMetadata.setClusterId(entry.getKey());
                    clusterMetadata.setBootstrapServers(
                            entry.getValue()
                                    .getProperties()
                                    .getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
                    clusterMetadata.setTopics(new ArrayList<>(entry.getValue().getTopics()));
                    clusterMetadataList.add(clusterMetadata);
                }

                YamlFileMetadataService.StreamMetadata streamMetadata =
                        new YamlFileMetadataService.StreamMetadata();
                streamMetadata.setStreamId(kafkaStream.getStreamId());
                streamMetadata.setClusterMetadataList(clusterMetadataList);
                streamMetadataList.add(streamMetadata);
            }

            YamlFileMetadataService.saveToYaml(streamMetadataList, metadataFile);
        }

        private void writeClusterMetadataToFile(
                File metadataFile,
                String streamId,
                List<String> topics,
                List<KafkaTestBase.KafkaClusterTestEnvMetadata> kafkaClusterTestEnvMetadataList)
                throws IOException {
            List<YamlFileMetadataService.StreamMetadata.ClusterMetadata> clusterMetadata =
                    kafkaClusterTestEnvMetadataList.stream()
                            .map(
                                    KafkaClusterTestEnvMetadata ->
                                            new YamlFileMetadataService.StreamMetadata
                                                    .ClusterMetadata(
                                                    KafkaClusterTestEnvMetadata.getKafkaClusterId(),
                                                    KafkaClusterTestEnvMetadata
                                                            .getBrokerConnectionStrings(),
                                                    topics))
                            .collect(Collectors.toList());
            YamlFileMetadataService.StreamMetadata streamMetadata =
                    new YamlFileMetadataService.StreamMetadata(streamId, clusterMetadata);
            YamlFileMetadataService.saveToYaml(
                    Collections.singletonList(streamMetadata), metadataFile);
        }

        private void writeClusterMetadataToFile(
                File metadataFile,
                String streamId,
                String topic,
                List<KafkaTestBase.KafkaClusterTestEnvMetadata> kafkaClusterTestEnvMetadataList)
                throws IOException {
            writeClusterMetadataToFile(
                    metadataFile,
                    streamId,
                    ImmutableList.of(topic),
                    kafkaClusterTestEnvMetadataList);
        }

        private Set<String> findMetrics(InMemoryReporter inMemoryReporter, String groupPattern) {
            Optional<MetricGroup> groups = inMemoryReporter.findGroup(groupPattern);
            assertThat(groups).isPresent();
            return inMemoryReporter.getMetricsByGroup(groups.get()).keySet().stream()
                    .map(metricName -> groups.get().getMetricIdentifier(metricName))
                    .collect(Collectors.toSet());
        }

        private void registerReader(
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
                DynamicKafkaSourceEnumerator enumerator,
                int readerId) {
            context.registerReader(new ReaderInfo(readerId, "location " + readerId));
            enumerator.addReader(readerId);
            enumerator.handleSourceEvent(readerId, new GetMetadataUpdateEvent());
        }

        private void runAllOneTimeCallables(
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context) throws Throwable {
            while (!context.getOneTimeCallables().isEmpty()) {
                context.runNextOneTimeCallable();
            }
        }

        private Set<KafkaStream> getKafkaStreams(
                String kafkaClusterId, Properties properties, Collection<String> topics) {
            return topics.stream()
                    .map(topic -> getKafkaStream(kafkaClusterId, properties, topic))
                    .collect(Collectors.toSet());
        }

        private KafkaStream getKafkaStream(
                String kafkaClusterId, Properties properties, String topic) {
            return new KafkaStream(
                    topic,
                    Collections.singletonMap(
                            kafkaClusterId,
                            new ClusterMetadata(Collections.singleton(topic), properties)));
        }
    }

    /** Integration test based on connector testing framework. */
    @Nested
    class IntegrationTests extends SourceTestSuiteBase<String> {
        @TestSemantics
        CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

        // Defines test environment on Flink MiniCluster
        @SuppressWarnings("unused")
        @TestEnv
        MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

        @TestExternalSystem
        DefaultContainerizedExternalSystem<TwoKafkaContainers> twoKafkas =
                DefaultContainerizedExternalSystem.builder()
                        .fromContainer(new TwoKafkaContainers())
                        .build();

        @SuppressWarnings("unused")
        @TestContext
        DynamicKafkaSourceExternalContextFactory twoClusters =
                new DynamicKafkaSourceExternalContextFactory(
                        twoKafkas.getContainer().getKafka0(),
                        twoKafkas.getContainer().getKafka1(),
                        Collections.emptyList());
    }
}
