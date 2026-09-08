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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsSplitReassignmentOnRecovery;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
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
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.CloseableIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.apache.flink.connector.kafka.dynamic.source.metrics.KafkaClusterMetricGroup.DYNAMIC_KAFKA_SOURCE_METRIC_GROUP;
import static org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper.committedConsumer;
import static org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper.drainCommittedRecords;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource}.
 */
class DynamicKafkaSourceITTest {

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
        @RegisterExtension
        final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

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
        void testGlobalEnumeratorModeBalancesAssignments() throws Throwable {
            // This verifies the global mode wiring from DynamicKafkaSource builder -> enumerator.
            // In global mode, split ownership should be balanced across all readers (not per
            // cluster).
            final int numSubtasks = 4;
            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                    DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());

            MockKafkaMetadataService metadataService =
                    new MockKafkaMetadataService(
                            Collections.singleton(
                                    DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC)));

            DynamicKafkaSource<Integer> source =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(
                                    metadataService.getAllStreams().stream()
                                            .map(KafkaStream::getStreamId)
                                            .collect(Collectors.toSet()))
                            .setKafkaMetadataService(metadataService)
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .setProperties(properties)
                            .build();

            try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                            new MockSplitEnumeratorContext<>(numSubtasks);
                    SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState>
                            splitEnumerator = source.createEnumerator(context)) {
                DynamicKafkaSourceEnumerator enumerator =
                        (DynamicKafkaSourceEnumerator) splitEnumerator;
                enumerator.start();

                for (int readerId = 0; readerId < numSubtasks; readerId++) {
                    registerReader(context, enumerator, readerId);
                }
                waitForInitialSplitAssignments(context);

                verifyAllSplitsAssignedOnce(
                        context.getSplitsAssignmentSequence(), metadataService.getAllStreams());
                assertAssignmentsBalanced(context.getSplitsAssignmentSequence(), numSubtasks);
            }
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
                waitForInitialSplitAssignments(context);

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
        void testRemovedClusterOffsetsRetainedAcrossCheckpointRestoreAndRescale() throws Throwable {
            int kafkaClusterIdx = 0;
            String topic = "test-retained-removed-cluster-" + System.currentTimeMillis();
            DynamicKafkaSourceTestHelper.createTopic(kafkaClusterIdx, topic, NUM_PARTITIONS);

            String testStreamId = "test-retained-removed-cluster-stream";
            File metadataFile = File.createTempFile(testDir.getPath() + "/metadata", ".yaml");
            writeClusterMetadataToFile(
                    metadataFile,
                    testStreamId,
                    topic,
                    ImmutableList.of(
                            DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(
                                    kafkaClusterIdx)));

            SharedReference<List<Integer>> collectedRecords = sharedObjects.add(new ArrayList<>());
            Configuration checkpointConfiguration = createCheckpointConfiguration();
            JobClient phase1JobClient = null;
            JobClient phase2JobClient = null;
            try {
                int stage1End =
                        DynamicKafkaSourceTestHelper.produceToKafka(
                                kafkaClusterIdx, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, 0);
                phase1JobClient =
                        startRetainedRemovedClusterJob(
                                checkpointConfiguration,
                                metadataFile,
                                testStreamId,
                                collectedRecords,
                                1);
                waitForCollectedRecords(
                        collectedRecords,
                        phase1JobClient,
                        stage1End,
                        "Could not read initial retained-cluster records before removal");
                assertThat(copyCollectedRecords(collectedRecords))
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, stage1End).boxed().collect(Collectors.toList()));

                writeClusterMetadataToFile(
                        metadataFile, testStreamId, topic, Collections.emptyList());
                waitForKafkaClusterMetricsToDisappear(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                String retainedCheckpoint = triggerAndCompleteCheckpoint(phase1JobClient);

                cancelJob(phase1JobClient);
                phase1JobClient = null;
                // The selected checkpoint can be subsumed before cancellation completes.
                retainedCheckpoint = retainedCheckpointAfterCancellation(retainedCheckpoint);

                writeClusterMetadataToFile(
                        metadataFile,
                        testStreamId,
                        topic,
                        ImmutableList.of(
                                DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(
                                        kafkaClusterIdx)));
                int stage2End =
                        DynamicKafkaSourceTestHelper.produceToKafka(
                                kafkaClusterIdx,
                                topic,
                                NUM_PARTITIONS,
                                NUM_RECORDS_PER_SPLIT,
                                stage1End);

                Configuration restoreConfiguration = new Configuration(checkpointConfiguration);
                restoreConfiguration.set(SAVEPOINT_PATH, retainedCheckpoint);
                phase2JobClient =
                        startRetainedRemovedClusterJob(
                                restoreConfiguration,
                                metadataFile,
                                testStreamId,
                                collectedRecords,
                                2);
                waitForCollectedRecords(
                        collectedRecords,
                        phase2JobClient,
                        stage2End,
                        "Could not read records after retained cluster re-add and restore");

                assertThat(copyCollectedRecords(collectedRecords))
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, stage2End).boxed().collect(Collectors.toList()));
            } finally {
                cancelJob(phase2JobClient);
                cancelJob(phase1JobClient);
            }
        }

        @Test
        void testHandoffReportCollectionAndLocalRecoveryPreserveCommittedRecords()
                throws Throwable {
            String topic = "handoff-recovery-" + UUID.randomUUID();
            String outputTopic = topic + "-output";
            DynamicKafkaSourceTestHelper.createTopic(topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(0, outputTopic, 1);
            File metadataFile = new File(testDir, "handoff-metadata.yaml");
            writeRecoveryMetadata(metadataFile, topic, true);
            SharedReference<ReaderAttemptObservations> observations =
                    sharedObjects.add(new ReaderAttemptObservations());
            Configuration configuration = createCheckpointConfiguration();
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 5);
            configuration.set(
                    RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                    Duration.ofMillis(100));

            JobClient job = null;
            try (KafkaConsumer<Integer, Integer> consumer = committedConsumer(0, outputTopic)) {
                List<Integer> committedRecords = new ArrayList<>();
                int nextValue = produceRecoveryRecords(topic, 0);
                job =
                        startCommittedRecoveryJob(
                                configuration,
                                metadataFile,
                                topic,
                                outputTopic,
                                observations,
                                2,
                                60_000L,
                                OffsetsInitializer.earliest());
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                int recoveringReader = observations.applySync(state -> state.readerByRecord.get(0));
                int healthyReader = 1 - recoveringReader;

                writeRecoveryMetadata(metadataFile, topic, false);
                waitForKafkaClusterMetricsToDisappear(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                triggerAndCompleteCheckpoint(job);

                // The removed cluster accumulates records while its offsets remain dormant.
                nextValue = produceRecoveryRecords(topic, nextValue);
                waitForCommittedRecords(
                        consumer,
                        committedRecords,
                        job,
                        nextValue - NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT);

                observations.consumeSync(state -> state.readerToFail = recoveringReader);
                nextValue =
                        DynamicKafkaSourceTestHelper.produceToKafka(
                                1, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, nextValue);
                waitForReaderAttempt(observations, job, recoveringReader, 1, healthyReader);
                waitForCommittedRecords(
                        consumer,
                        committedRecords,
                        job,
                        nextValue - NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT);

                observations.consumeSync(
                        state -> {
                            state.handoffClusterId =
                                    kafkaClusterTestEnvMetadata0.getKafkaClusterId();
                            state.failOnRequestReader = recoveringReader;
                        });
                writeRecoveryMetadata(metadataFile, topic, true);
                // Lose one retained-offset response through a real local task failure.
                waitForReaderAttempt(observations, job, recoveringReader, 2, healthyReader);
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                waitForActiveSplitCounts(3, 3);
                int requestFailures = observations.applySync(state -> state.requestFailures);
                assertThat(requestFailures).isEqualTo(1);

                nextValue = produceRecoveryRecords(topic, nextValue);
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                triggerAndCompleteCheckpoint(job);
                Integer finalHealthyAttempt =
                        observations.applySync(state -> state.attempts.get(healthyReader));
                assertThat(finalHealthyAttempt)
                        .as("healthy peer stays on its original attempt through all recoveries")
                        .isZero();
                cancelJob(job);
                job = null;
                drainCommittedRecords(consumer, committedRecords);
                assertThat(committedRecords)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, nextValue).boxed().collect(Collectors.toList()));
            } finally {
                cancelJob(job);
            }
        }

        @Test
        void testManualCheckpointHandoffKeepsActiveReadersProgressing() throws Throwable {
            String topic = "manual-checkpoint-handoff-" + UUID.randomUUID();
            String outputTopic = topic + "-output";
            DynamicKafkaSourceTestHelper.createTopic(topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(0, outputTopic, 1);
            File metadataFile = new File(testDir, "manual-checkpoint-metadata.yaml");
            writeRecoveryMetadata(metadataFile, topic, true);
            SharedReference<ReaderAttemptObservations> observations =
                    sharedObjects.add(new ReaderAttemptObservations());
            observations.consumeSync(
                    state -> {
                        state.observeReportBarrier = true;
                        state.handoffClusterId = kafkaClusterTestEnvMetadata0.getKafkaClusterId();
                    });

            JobClient job = null;
            try (KafkaConsumer<Integer, Integer> consumer = committedConsumer(0, outputTopic)) {
                List<Integer> committedRecords = new ArrayList<>();
                int nextValue = produceRecoveryRecords(topic, 0);
                job =
                        startCommittedRecoveryJob(
                                createCheckpointConfiguration(),
                                metadataFile,
                                topic,
                                outputTopic,
                                observations,
                                2,
                                60_000L,
                                OffsetsInitializer.earliest(),
                                false);
                waitForObservedRecords(observations, job, nextValue);
                triggerAndCompleteCheckpoint(job);
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);

                writeRecoveryMetadata(metadataFile, topic, false);
                waitForKafkaClusterMetricsToDisappear(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                nextValue = produceRecoveryRecords(topic, nextValue);
                int activeRecords = nextValue - NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT;
                waitForObservedRecords(observations, job, activeRecords);
                triggerAndCompleteCheckpoint(job);
                waitForCommittedRecords(consumer, committedRecords, job, activeRecords);

                writeRecoveryMetadata(metadataFile, topic, true);
                // Wait for both real retained-offset reports to reach the coordinator.
                waitForHandoffReportBarrier(observations, 2);
                int healthyBatchStart = nextValue;
                nextValue =
                        DynamicKafkaSourceTestHelper.produceToKafka(
                                1, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, nextValue);
                waitForObservedRecords(
                        observations, job, activeRecords + NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT);
                Set<Integer> observedWhileWaiting =
                        observations.applySync(
                                state -> new TreeSet<>(state.readerByRecord.keySet()));
                assertThat(observedWhileWaiting)
                        .as(
                                "healthy active partitions keep emitting while handoff awaits its"
                                        + " checkpoint")
                        .containsAll(
                                IntStream.range(healthyBatchStart, nextValue)
                                        .boxed()
                                        .collect(Collectors.toList()))
                        .doesNotContainAnyElementsOf(
                                IntStream.range(
                                                NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 2,
                                                NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT * 3)
                                        .boxed()
                                        .collect(Collectors.toList()));
                Set<Integer> assignedBeforeCheckpoint =
                        observations.applySync(state -> new TreeSet<>(state.assignmentReaders));
                assertThat(assignedBeforeCheckpoint)
                        .as("manual checkpoint history requires a completed handoff checkpoint")
                        .isEmpty();
                triggerAndCompleteCheckpoint(job);
                waitForObservedRecords(observations, job, nextValue);
                waitForActiveSplitCounts(3, 3);
                triggerAndCompleteCheckpoint(job);
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                cancelJob(job);
                job = null;
                drainCommittedRecords(consumer, committedRecords);
                assertThat(committedRecords)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, nextValue).boxed().collect(Collectors.toList()));
            } finally {
                cancelJob(job);
            }
        }

        @Test
        void testReturningSubsetFillsIdleReaderWithoutLosingCommittedRecords() throws Throwable {
            String streamId = "returning-subset-" + UUID.randomUUID();
            String topicA = streamId + "-a";
            String topicB = streamId + "-b";
            String topicC = streamId + "-c";
            String outputTopic = streamId + "-output";
            DynamicKafkaSourceTestHelper.createTopic(0, topicA, 1);
            DynamicKafkaSourceTestHelper.createTopic(1, topicB, 1);
            DynamicKafkaSourceTestHelper.createTopic(0, topicC, 1);
            DynamicKafkaSourceTestHelper.createTopic(0, outputTopic, 1);
            Map<String, ClusterMetadata> allClusters = new TreeMap<>();
            allClusters.put("a", clusterMetadata(0, Collections.singleton(topicA)));
            allClusters.put("b", clusterMetadata(1, Collections.singleton(topicB)));
            allClusters.put("c", clusterMetadata(0, Collections.singleton(topicC)));
            File metadataFile = new File(testDir, "returning-subset.yaml");
            writeClusterMetadataToFile(
                    metadataFile, Collections.singleton(new KafkaStream(streamId, allClusters)));
            SharedReference<ReaderAttemptObservations> observations =
                    sharedObjects.add(new ReaderAttemptObservations());
            Configuration configuration = createCheckpointConfiguration();
            JobClient job = null;
            try (KafkaConsumer<Integer, Integer> consumer = committedConsumer(0, outputTopic)) {
                List<Integer> committedRecords = new ArrayList<>();
                int nextValue = produceTopicRecords(0, topicA, 1, 0);
                nextValue = produceTopicRecords(1, topicB, 1, nextValue);
                nextValue = produceTopicRecords(0, topicC, 1, nextValue);
                job =
                        startCommittedRecoveryJob(
                                configuration,
                                metadataFile,
                                streamId,
                                outputTopic,
                                observations,
                                2,
                                60_000L,
                                OffsetsInitializer.earliest());
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                writeClusterMetadataToFile(
                        metadataFile,
                        Collections.singleton(
                                new KafkaStream(
                                        streamId,
                                        Collections.singletonMap("a", allClusters.get("a")))));
                waitForKafkaClusterMetricsToDisappear("b");
                waitForKafkaClusterMetricsToDisappear("c");
                String checkpoint = triggerAndCompleteCheckpoint(job);
                cancelJob(job);
                job = null;
                checkpoint = retainedCheckpointAfterCancellation(checkpoint);

                nextValue = produceTopicRecords(0, topicA, 1, nextValue);
                nextValue = produceTopicRecords(1, topicB, 1, nextValue);
                nextValue = produceTopicRecords(0, topicC, 1, nextValue);
                Configuration restored = new Configuration(configuration);
                restored.set(SAVEPOINT_PATH, checkpoint);
                job =
                        startCommittedRecoveryJob(
                                restored,
                                metadataFile,
                                streamId,
                                outputTopic,
                                observations,
                                2,
                                60_000L,
                                OffsetsInitializer.earliest());
                waitForCommittedRecords(
                        consumer, committedRecords, job, nextValue - 2 * NUM_RECORDS_PER_SPLIT);
                waitForActiveSplitCounts(1, 0);

                // Full recovery puts A first, then dormant B and C: A/C share reader0. Returning
                // only C must rebalance to [1,1], rather than resume that dormant placement [2,0].
                writeClusterMetadataToFile(
                        metadataFile,
                        Collections.singleton(
                                new KafkaStream(
                                        streamId,
                                        ImmutableMap.of(
                                                "a", allClusters.get("a"),
                                                "c", allClusters.get("c")))));
                waitForCommittedRecords(
                        consumer, committedRecords, job, nextValue - NUM_RECORDS_PER_SPLIT);
                waitForActiveSplitCounts(1, 1);

                writeClusterMetadataToFile(
                        metadataFile,
                        Collections.singleton(new KafkaStream(streamId, allClusters)));
                nextValue = produceTopicRecords(0, topicA, 1, nextValue);
                nextValue = produceTopicRecords(1, topicB, 1, nextValue);
                nextValue = produceTopicRecords(0, topicC, 1, nextValue);
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                waitForBalancedActiveSplits(3, 2);
                triggerAndCompleteCheckpoint(job);
                cancelJob(job);
                job = null;
                drainCommittedRecords(consumer, committedRecords);
                assertThat(committedRecords)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, nextValue).boxed().collect(Collectors.toList()));
            } finally {
                cancelJob(job);
            }
        }

        @Test
        void testReturningSplitsFillIdleReaderWithoutMovingActiveSplits() throws Throwable {
            runReturningSplitHandoff(-1, false);
        }

        @Test
        void testOldOwnerLocalRecoveryAfterReturningSplitTransferPreservesCommittedRecords()
                throws Throwable {
            runReturningSplitHandoff(0, false);
        }

        @Test
        void testNewOwnerLocalRecoveryAfterReturningSplitTransferPreservesCommittedRecords()
                throws Throwable {
            runReturningSplitHandoff(1, false);
        }

        @Test
        void testOldOwnerRestoresRetainedShadowAfterNewOwnerEmitsRecords() throws Throwable {
            runReturningSplitHandoff(0, true);
        }

        private void runReturningSplitHandoff(int failingReader, boolean holdCleanup)
                throws Throwable {
            String streamId = "returning-handoff-" + UUID.randomUUID();
            String returningTopic = streamId + "-returning";
            String outputTopic = streamId + "-output";
            List<String> activeTopics =
                    IntStream.range(0, 8)
                            .mapToObj(index -> streamId + "-active-" + index)
                            .collect(Collectors.toList());
            for (String topic : activeTopics) {
                DynamicKafkaSourceTestHelper.createTopic(0, topic, 1);
            }
            DynamicKafkaSourceTestHelper.createTopic(1, returningTopic, 2);
            DynamicKafkaSourceTestHelper.createTopic(0, outputTopic, 1);
            File metadataFile = new File(testDir, "returning-handoff.yaml");
            writeHandoffMetadata(metadataFile, streamId, activeTopics, returningTopic);
            SharedReference<ReaderAttemptObservations> observations =
                    sharedObjects.add(new ReaderAttemptObservations());
            Configuration configuration = createCheckpointConfiguration();
            if (failingReader >= 0) {
                configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
                configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
                configuration.set(
                        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                        Duration.ofMillis(100));
            }
            JobClient job = null;
            try (KafkaConsumer<Integer, Integer> consumer = committedConsumer(0, outputTopic)) {
                List<Integer> committedRecords = new ArrayList<>();
                Map<String, Integer> initialRecordByTopic = new TreeMap<>();
                int nextValue = 0;
                for (String topic : activeTopics) {
                    initialRecordByTopic.put(topic, nextValue);
                    nextValue = produceTopicRecords(0, topic, 1, nextValue);
                }
                int initialReturningValue = nextValue;
                nextValue = produceTopicRecords(1, returningTopic, 2, nextValue);
                job =
                        startCommittedRecoveryJob(
                                configuration,
                                metadataFile,
                                streamId,
                                outputTopic,
                                observations,
                                2,
                                60_000L,
                                OffsetsInitializer.earliest());
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                waitForActiveSplitCounts(5, 5);
                List<String> retainedActiveTopics =
                        activeTopics.stream()
                                .filter(
                                        topic ->
                                                observations.applySync(
                                                        state ->
                                                                state.readerByRecord.get(
                                                                                initialRecordByTopic
                                                                                        .get(topic))
                                                                        == 0))
                                .collect(Collectors.toList());
                assertThat(retainedActiveTopics).hasSize(4);
                int transferredPartition =
                        observations.applySync(
                                state ->
                                        state.readerByRecord.get(initialReturningValue) == 0
                                                ? 0
                                                : 1);
                writeHandoffMetadata(metadataFile, streamId, retainedActiveTopics, null);
                waitForKafkaClusterMetricsToDisappear("b");
                waitForActiveSplitCounts(4, 0);
                triggerAndCompleteCheckpoint(job);
                for (String topic : retainedActiveTopics) {
                    nextValue = produceTopicRecords(0, topic, 1, nextValue);
                }
                int returningBatchStart = nextValue;
                nextValue = produceTopicRecords(1, returningTopic, 2, nextValue);
                waitForCommittedRecords(
                        consumer, committedRecords, job, nextValue - 2 * NUM_RECORDS_PER_SPLIT);

                // The returning partitions fill reader1. Existing active ownership stays [4,0].
                observations.consumeSync(
                        state -> {
                            state.handoffClusterId = "b";
                            state.transferredPartition =
                                    new TopicPartition(returningTopic, transferredPartition);
                            if (failingReader == 0) {
                                state.failOnCleanupReader = 0;
                                if (holdCleanup) {
                                    state.cleanupBarrier =
                                            new RetainedCleanupBarrier(
                                                    "b", state.transferredPartition);
                                }
                            } else if (failingReader == 1) {
                                state.failOnAssignmentReader = 1;
                            }
                        });
                writeHandoffMetadata(metadataFile, streamId, retainedActiveTopics, returningTopic);
                RetainedCleanupBarrier cleanupBarrier =
                        observations.applySync(state -> state.cleanupBarrier);
                if (cleanupBarrier != null) {
                    cleanupBarrier.awaitNewOwnerProgress(
                            () ->
                                    observations.applySync(
                                            state -> new HashMap<>(state.readerByRecord)),
                            returningBatchStart,
                            nextValue);
                    cleanupBarrier.releaseCleanup();
                }
                if (failingReader >= 0) {
                    waitForReaderAttempt(observations, job, failingReader, 1, 1 - failingReader);
                    int transferFailures =
                            observations.applySync(
                                    state -> state.assignmentFailures + state.cleanupFailures);
                    assertThat(transferFailures).isEqualTo(1);
                }
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                waitForActiveSplitCounts(4, 2);
                if (cleanupBarrier != null) {
                    cleanupBarrier.assertRestoredShadow();
                    assertThat(
                                    observations.<Set<Integer>>applySync(
                                            state -> state.recordsWithMultipleOwners))
                            .isEmpty();
                }
                Integer transferredOwner =
                        observations.applySync(
                                state ->
                                        state.readerByRecord.get(
                                                returningBatchStart
                                                        + transferredPartition
                                                                * NUM_RECORDS_PER_SPLIT));
                assertThat(transferredOwner)
                        .as("returning partition moved from reader0")
                        .isEqualTo(1);
                List<Integer> postHandoffActiveRecords = new ArrayList<>();
                for (String topic : retainedActiveTopics) {
                    postHandoffActiveRecords.add(nextValue);
                    nextValue = produceTopicRecords(0, topic, 1, nextValue);
                }
                nextValue = produceTopicRecords(1, returningTopic, 2, nextValue);
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                assertThat(postHandoffActiveRecords)
                        .allMatch(
                                record ->
                                        observations.applySync(
                                                state -> state.readerByRecord.get(record) == 0));
                triggerAndCompleteCheckpoint(job);
                if (failingReader >= 0) {
                    Integer healthyAttempt =
                            observations.applySync(state -> state.attempts.get(1 - failingReader));
                    assertThat(healthyAttempt)
                            .as("healthy transfer peer stays on its original execution attempt")
                            .isZero();
                }
                cancelJob(job);
                job = null;
                drainCommittedRecords(consumer, committedRecords);
                assertThat(committedRecords)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, nextValue).boxed().collect(Collectors.toList()));
            } finally {
                RetainedCleanupBarrier barrier =
                        observations.applySync(state -> state.cleanupBarrier);
                if (barrier != null) {
                    barrier.releaseCleanup();
                }
                cancelJob(job);
            }
        }

        private void writeHandoffMetadata(
                File file, String streamId, Collection<String> activeTopics, String returningTopic)
                throws IOException {
            Map<String, ClusterMetadata> clusters = new HashMap<>();
            clusters.put("a", clusterMetadata(0, activeTopics));
            if (returningTopic != null) {
                clusters.put("b", clusterMetadata(1, Collections.singleton(returningTopic)));
            }
            writeClusterMetadataToFile(
                    file, Collections.singleton(new KafkaStream(streamId, clusters)));
        }

        @ParameterizedTest
        @ValueSource(longs = {0L, 123L})
        void testGlobalClusterReAddAfterCheckpointRescalePreservesCommittedRecords(
                long legacyReaderDeadlineSkew) throws Throwable {
            String topic = "handoff-rescale-" + UUID.randomUUID();
            String outputTopic = topic + "-output";
            DynamicKafkaSourceTestHelper.createTopic(topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(0, outputTopic, 1);
            File metadataFile = new File(testDir, "handoff-rescale.yaml");
            writeRecoveryMetadata(metadataFile, topic, true);
            SharedReference<ReaderAttemptObservations> observations =
                    sharedObjects.add(new ReaderAttemptObservations());
            observations.consumeSync(
                    state -> state.legacyReaderDeadlineSkew = legacyReaderDeadlineSkew);
            Configuration configuration = createCheckpointConfiguration();
            JobClient job = null;
            try (KafkaConsumer<Integer, Integer> consumer = committedConsumer(0, outputTopic)) {
                List<Integer> committedRecords = new ArrayList<>();
                int nextValue = produceRecoveryRecords(topic, 0);
                job =
                        startCommittedRecoveryJob(
                                configuration,
                                metadataFile,
                                topic,
                                outputTopic,
                                observations,
                                1,
                                60_000L,
                                OffsetsInitializer.earliest());
                waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                writeRecoveryMetadata(metadataFile, topic, false);
                waitForKafkaClusterMetricsToDisappear(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                triggerAndCompleteCheckpoint(job);
                nextValue = produceRecoveryRecords(topic, nextValue);
                waitForCommittedRecords(
                        consumer,
                        committedRecords,
                        job,
                        nextValue - NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT);
                observations.consumeSync(
                        state -> {
                            state.handoffClusterId =
                                    kafkaClusterTestEnvMetadata0.getKafkaClusterId();
                            state.holdReturningAssignment = true;
                            state.firstReportedCheckpointByReader.clear();
                            state.assignmentReaders.clear();
                        });
                writeRecoveryMetadata(metadataFile, topic, true);
                waitForHeldReturningAssignment(observations, 1);
                // Returning assignments are sent only after the eligible checkpoint completes.
                // Hold before addSplits so this reader still checkpoints its dormant offset.
                String checkpoint = latestCompletedCheckpoint(job).toURI().toString();
                long checkpointId = checkpointId(new File(URI.create(checkpoint)));
                Long firstReportedCheckpoint =
                        observations.applySync(
                                state -> state.firstReportedCheckpointByReader.get(0));
                assertThat(firstReportedCheckpoint).isNotNull();
                assertThat(checkpointId).isGreaterThanOrEqualTo(firstReportedCheckpoint);
                cancelJob(job);
                job = null;
                checkpoint = retainedCheckpointAfterCancellation(checkpoint);
                observations.consumeSync(
                        state -> {
                            state.holdReturningAssignment = false;
                            state.assignmentReaders.clear();
                            if (legacyReaderDeadlineSkew != 0) {
                                assertThat(state.legacySkewedSnapshots).isGreaterThan(0);
                            }
                            state.legacyReaderDeadlineSkew = 0L;
                        });

                // This partition is absent from the restored enumerator inventory. Discovery must
                // assign it only after complete reader-state reconciliation has established owners.
                try (AdminClient admin =
                        AdminClient.create(kafkaClusterTestEnvMetadata1.getStandardProperties())) {
                    admin.createPartitions(
                                    Collections.singletonMap(
                                            topic, NewPartitions.increaseTo(NUM_PARTITIONS + 1)))
                            .all()
                            .get(30, TimeUnit.SECONDS);
                }
                List<ProducerRecord<String, Integer>> newPartitionRecords = new ArrayList<>();
                for (int record = 0; record < NUM_RECORDS_PER_SPLIT; record++) {
                    newPartitionRecords.add(
                            new ProducerRecord<>(
                                    topic, NUM_PARTITIONS, "new-partition", nextValue++));
                }
                DynamicKafkaSourceTestHelper.produceToKafka(1, newPartitionRecords);
                writeRecoveryMetadata(metadataFile, topic, true);
                Configuration restoreConfiguration = new Configuration(configuration);
                restoreConfiguration.set(SAVEPOINT_PATH, checkpoint);
                job =
                        startCommittedRecoveryJob(
                                restoreConfiguration,
                                metadataFile,
                                topic,
                                outputTopic,
                                observations,
                                2,
                                60_000L,
                                OffsetsInitializer.earliest());
                try {
                    waitForCommittedRecords(consumer, committedRecords, job, nextValue);
                } catch (TimeoutException timeout) {
                    Set<Integer> missing =
                            IntStream.range(0, nextValue)
                                    .boxed()
                                    .collect(Collectors.toCollection(TreeSet::new));
                    missing.removeAll(committedRecords);
                    throw new AssertionError(
                            "Restore checkpoint="
                                    + checkpoint
                                    + "; missing committed IDs="
                                    + missing
                                    + "; source observations="
                                    + observations.applySync(
                                            state ->
                                                    "attempts="
                                                            + new TreeMap<>(state.attempts)
                                                            + ", record owners="
                                                            + new TreeMap<>(state.readerByRecord))
                                    + "; source offset metrics="
                                    + snapshotSourceOffsetMetrics(),
                            timeout);
                }
                waitForBalancedActiveSplits(2 * NUM_PARTITIONS + 1, 2);
                triggerAndCompleteCheckpoint(job);
                cancelJob(job);
                job = null;
                drainCommittedRecords(consumer, committedRecords);
                assertThat(committedRecords)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.range(0, nextValue).boxed().collect(Collectors.toList()));
            } finally {
                observations.consumeSync(state -> state.holdReturningAssignment = false);
                cancelJob(job);
            }
        }

        @Test
        void testExpiredClusterReAddUsesConfiguredStartingOffsets() throws Throwable {
            String topic = "handoff-expiry-" + UUID.randomUUID();
            String outputTopic = topic + "-output";
            DynamicKafkaSourceTestHelper.createTopic(topic, NUM_PARTITIONS);
            DynamicKafkaSourceTestHelper.createTopic(0, outputTopic, 1);
            File metadataFile = new File(testDir, "handoff-expiry.yaml");
            writeRecoveryMetadata(metadataFile, topic, true);
            SharedReference<ReaderAttemptObservations> observations =
                    sharedObjects.add(new ReaderAttemptObservations());
            JobClient job = null;
            try (KafkaConsumer<Integer, Integer> consumer = committedConsumer(0, outputTopic)) {
                List<Integer> committedRecords = new ArrayList<>();
                job =
                        startCommittedRecoveryJob(
                                createCheckpointConfiguration(),
                                metadataFile,
                                topic,
                                outputTopic,
                                observations,
                                2,
                                500L,
                                OffsetsInitializer.latest());
                waitForClusterPartitionAssignments(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                waitForClusterPartitionAssignments(
                        kafkaClusterTestEnvMetadata1.getKafkaClusterId());
                int initialEnd = produceRecoveryRecords(topic, 0);
                waitForCommittedRecords(consumer, committedRecords, job, initialEnd);

                writeRecoveryMetadata(metadataFile, topic, false);
                waitForKafkaClusterMetricsToDisappear(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                // Let the coordinator expire paused ownership before re-add.
                Thread.sleep(1500L);
                int removedIntervalEnd = produceRecoveryRecords(topic, initialEnd);
                int skippedRecords = NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT;
                waitForCommittedRecords(
                        consumer, committedRecords, job, removedIntervalEnd - skippedRecords);
                writeRecoveryMetadata(metadataFile, topic, true);
                waitForClusterPartitionAssignments(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId());
                int nextValue = produceRecoveryRecords(topic, removedIntervalEnd);
                waitForCommittedRecords(
                        consumer, committedRecords, job, nextValue - skippedRecords);
                triggerAndCompleteCheckpoint(job);
                cancelJob(job);
                job = null;
                drainCommittedRecords(consumer, committedRecords);
                // With latest(), expiry intentionally skips only the removed interval for cluster0.
                assertThat(committedRecords)
                        .containsExactlyInAnyOrderElementsOf(
                                IntStream.concat(
                                                IntStream.range(0, initialEnd),
                                                IntStream.range(
                                                        initialEnd + skippedRecords, nextValue))
                                        .boxed()
                                        .collect(Collectors.toList()));
            } finally {
                cancelJob(job);
            }
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

                // should contain only cluster 0 metrics
                waitForOnlyKafkaClusterMetrics("kafka-cluster-0");

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

                // cluster 0 is not being consumed from, metrics should contain only cluster 1
                waitForOnlyKafkaClusterMetrics("kafka-cluster-1");
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

        private Set<String> findKafkaClusterMetrics(InMemoryReporter inMemoryReporter) {
            // Metrics are registered per source subtask, so aggregate every matching group.
            return inMemoryReporter.findGroups(DYNAMIC_KAFKA_SOURCE_METRIC_GROUP).stream()
                    .flatMap(
                            group ->
                                    inMemoryReporter.getMetricsByGroup(group).keySet().stream()
                                            .map(
                                                    metricName ->
                                                            group.getMetricIdentifier(metricName)))
                    .filter(metricName -> metricName.contains(".kafkaCluster."))
                    .collect(Collectors.toSet());
        }

        private Configuration createCheckpointConfiguration() {
            Configuration configuration = new Configuration();
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
            configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            File checkpointDir = new File(testDir, "retained-removed-cluster-checkpoints");
            configuration.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
            configuration.set(
                    CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                    ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
            configuration.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
            return configuration;
        }

        private JobClient startRetainedRemovedClusterJob(
                Configuration configuration,
                File metadataFile,
                String streamId,
                SharedReference<List<Integer>> collectedRecords,
                int parallelism)
                throws Exception {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(parallelism);
            env.enableCheckpointing(100L);

            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "100");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "100");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                    "2");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                    "60000");
            properties.setProperty(
                    CommonClientConfigs.GROUP_ID_CONFIG, "test-retained-removed-cluster-offsets");

            YamlFileMetadataService yamlFileMetadataService =
                    new YamlFileMetadataService(metadataFile.getPath(), Duration.ofMillis(100));
            DynamicKafkaSource<Integer> dynamicKafkaSource =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(streamId))
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
            stream.uid("dynamic-kafka-src");
            stream.addSink(new CollectingSink(collectedRecords)).uid("collecting-sink");
            return env.executeAsync("test-retained-removed-cluster-offsets");
        }

        private void writeRecoveryMetadata(File metadataFile, String topic, boolean includeCluster0)
                throws IOException {
            writeClusterMetadataToFile(
                    metadataFile,
                    topic,
                    topic,
                    includeCluster0
                            ? ImmutableList.of(
                                    kafkaClusterTestEnvMetadata0, kafkaClusterTestEnvMetadata1)
                            : ImmutableList.of(kafkaClusterTestEnvMetadata1));
        }

        private int produceRecoveryRecords(String topic, int firstValue) throws Throwable {
            int nextValue = firstValue;
            for (int cluster = 0;
                    cluster < DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS;
                    cluster++) {
                nextValue =
                        DynamicKafkaSourceTestHelper.produceToKafka(
                                cluster, topic, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT, nextValue);
            }
            return nextValue;
        }

        private ClusterMetadata clusterMetadata(int cluster, Collection<String> topics) {
            KafkaTestBase.KafkaClusterTestEnvMetadata environment =
                    cluster == 0 ? kafkaClusterTestEnvMetadata0 : kafkaClusterTestEnvMetadata1;
            return new ClusterMetadata(new TreeSet<>(topics), environment.getStandardProperties());
        }

        private int produceTopicRecords(int cluster, String topic, int partitions, int firstValue)
                throws Throwable {
            return DynamicKafkaSourceTestHelper.produceToKafka(
                    cluster, topic, partitions, NUM_RECORDS_PER_SPLIT, firstValue);
        }

        private JobClient startCommittedRecoveryJob(
                Configuration configuration,
                File metadataFile,
                String streamId,
                String outputTopic,
                SharedReference<ReaderAttemptObservations> observations,
                int parallelism,
                long retentionMs,
                OffsetsInitializer startingOffsets)
                throws Exception {
            return startCommittedRecoveryJob(
                    configuration,
                    metadataFile,
                    streamId,
                    outputTopic,
                    observations,
                    parallelism,
                    retentionMs,
                    startingOffsets,
                    true);
        }

        private JobClient startCommittedRecoveryJob(
                Configuration configuration,
                File metadataFile,
                String streamId,
                String outputTopic,
                SharedReference<ReaderAttemptObservations> observations,
                int parallelism,
                long retentionMs,
                OffsetsInitializer startingOffsets,
                boolean periodicCheckpointing)
                throws Exception {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setParallelism(parallelism);
            if (periodicCheckpointing) {
                env.enableCheckpointing(100L);
            }
            Properties properties = new Properties();
            properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "100");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "100");
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                    Long.toString(retentionMs));
            properties.setProperty(
                    DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), "global");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, streamId);
            DynamicKafkaSource<Integer> source =
                    DynamicKafkaSource.<Integer>builder()
                            .setStreamIds(Collections.singleton(streamId))
                            .setKafkaMetadataService(
                                    new YamlFileMetadataService(
                                            metadataFile.getPath(), Duration.ofMillis(100)))
                            .setDeserializer(
                                    KafkaRecordDeserializationSchema.valueOnly(
                                            IntegerDeserializer.class))
                            .setStartingOffsets(startingOffsets)
                            .setProperties(properties)
                            .build();
            Properties producerProperties = new Properties();
            producerProperties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000");
            KafkaSink<Integer> sink =
                    KafkaSink.<Integer>builder()
                            .setBootstrapServers(
                                    kafkaClusterTestEnvMetadata0.getBrokerConnectionStrings())
                            .setKafkaProducerConfig(producerProperties)
                            .setRecordSerializer(
                                    KafkaRecordSerializationSchema.<Integer>builder()
                                            .setTopic(outputTopic)
                                            .setKafkaValueSerializer(IntegerSerializer.class)
                                            .build())
                            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .setTransactionalIdPrefix(outputTopic)
                            .build();
            // Forward/chained edges leave separate failover regions for the source subtasks.
            env.fromSource(
                            new ObservedHandoffSource(source, observations),
                            WatermarkStrategy.noWatermarks(),
                            "recovery-source")
                    .uid("recovery-source")
                    .map(new ReaderAttemptObserver(observations))
                    .uid("recovery-observer")
                    .sinkTo(sink)
                    .uid("recovery-output");
            return env.executeAsync(streamId);
        }

        private void waitForCommittedRecords(
                KafkaConsumer<Integer, Integer> consumer,
                List<Integer> records,
                JobClient job,
                int expectedCount)
                throws Exception {
            try {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                throwIfJobFailed(job);
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }
                            consumer.poll(Duration.ofMillis(100))
                                    .forEach(record -> records.add(record.value()));
                            return records.size() >= expectedCount;
                        },
                        Duration.ofSeconds(60),
                        "Did not observe " + expectedCount + " committed Kafka output records");
            } catch (TimeoutException timeout) {
                Map<TopicPartition, Long> positions = new HashMap<>();
                consumer.assignment()
                        .forEach(
                                partition ->
                                        positions.put(partition, consumer.position(partition)));
                TimeoutException detailed =
                        new TimeoutException(
                                "Expected "
                                        + expectedCount
                                        + " committed records; received="
                                        + records
                                        + "; consumer positions="
                                        + positions
                                        + "; last stable offsets="
                                        + consumer.endOffsets(
                                                consumer.assignment(), Duration.ofSeconds(5)));
                detailed.initCause(timeout);
                throw detailed;
            }
        }

        private void waitForObservedRecords(
                SharedReference<ReaderAttemptObservations> observations,
                JobClient job,
                int expectedCount)
                throws Exception {
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            throwIfJobFailed(job);
                        } catch (Exception exception) {
                            throw new RuntimeException(exception);
                        }
                        return observations.applySync(
                                state -> state.readerByRecord.size() >= expectedCount);
                    },
                    Duration.ofSeconds(30),
                    "Did not observe "
                            + expectedCount
                            + " source records before manual checkpoint");
        }

        private void waitForHandoffReportBarrier(
                SharedReference<ReaderAttemptObservations> observations, int readers)
                throws Exception {
            CommonTestUtils.waitUtil(
                    () ->
                            observations.applySync(
                                    state ->
                                            state.reportedHandoffIds.size() == readers
                                                    && new TreeSet<>(
                                                                            state.reportedHandoffIds
                                                                                    .values())
                                                                    .size()
                                                            == 1
                                                    && state.reportedHandoffIds.equals(
                                                            state.reportBarrierAcks)),
                    Duration.ofSeconds(30),
                    "Readers did not acknowledge the barrier after all retained-offset reports");
        }

        private Map<String, Object> snapshotSourceOffsetMetrics() {
            Map<String, Object> offsets = new TreeMap<>();
            reporter.findGroups(DYNAMIC_KAFKA_SOURCE_METRIC_GROUP)
                    .forEach(
                            group ->
                                    reporter.getMetricsByGroup(group)
                                            .forEach(
                                                    (name, metric) -> {
                                                        if (metric instanceof Gauge
                                                                && (name.endsWith("currentOffset")
                                                                        || name.endsWith(
                                                                                "committedOffset"))) {
                                                            offsets.put(
                                                                    group.getMetricIdentifier(name),
                                                                    ((Gauge<?>) metric).getValue());
                                                        }
                                                    }));
            return offsets;
        }

        private void waitForReaderAttempt(
                SharedReference<ReaderAttemptObservations> observations,
                JobClient job,
                int recoveringReader,
                int expectedAttempt,
                int healthyReader)
                throws Exception {
            try {
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                throwIfJobFailed(job);
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }
                            return observations.applySync(
                                    state ->
                                            state.attempts.getOrDefault(recoveringReader, -1)
                                                    == expectedAttempt);
                        },
                        Duration.ofSeconds(30),
                        "The requested reader did not recover independently");
            } catch (TimeoutException timeout) {
                TimeoutException detailed =
                        new TimeoutException(
                                "Reader "
                                        + recoveringReader
                                        + " did not reach attempt "
                                        + expectedAttempt
                                        + "; observations="
                                        + observations.applySync(
                                                state ->
                                                        "attempts="
                                                                + state.attempts
                                                                + ", requestFailures="
                                                                + state.requestFailures
                                                                + ", assignmentFailures="
                                                                + state.assignmentFailures
                                                                + ", cleanupFailures="
                                                                + state.cleanupFailures
                                                                + ", assignmentReaders="
                                                                + state.assignmentReaders
                                                                + ", reportedCheckpoints="
                                                                + state.firstReportedCheckpointByReader)
                                        + "; source offsets="
                                        + snapshotSourceOffsetMetrics());
                detailed.initCause(timeout);
                throw detailed;
            }
            Integer healthyAttempt =
                    observations.applySync(state -> state.attempts.get(healthyReader));
            assertThat(healthyAttempt).as("healthy peer execution attempt").isZero();
        }

        private Map<Integer, Integer> snapshotActiveSplitCounts() {
            Map<Integer, Integer> counts = new TreeMap<>();
            reporter.findGroups(DYNAMIC_KAFKA_SOURCE_METRIC_GROUP)
                    .forEach(
                            group ->
                                    reporter.getMetricsByGroup(group)
                                            .forEach(
                                                    (name, metric) -> {
                                                        if (name.equals("activeSplitCount")
                                                                && metric instanceof Gauge) {
                                                            String subtask =
                                                                    group.getAllVariables()
                                                                            .get("<subtask_index>");
                                                            counts.put(
                                                                    Integer.parseInt(subtask),
                                                                    ((Number)
                                                                                    ((Gauge<?>)
                                                                                                    metric)
                                                                                            .getValue())
                                                                            .intValue());
                                                        }
                                                    }));
            return counts;
        }

        private void waitForActiveSplitCounts(int... counts) throws Exception {
            Map<Integer, Integer> expected = new TreeMap<>();
            for (int reader = 0; reader < counts.length; reader++) {
                expected.put(reader, counts[reader]);
            }
            CommonTestUtils.waitUtil(
                    () -> snapshotActiveSplitCounts().equals(expected),
                    Duration.ofSeconds(30),
                    "Active split counts did not reach " + expected);
        }

        private void waitForBalancedActiveSplits(int totalSplits, int readers) throws Exception {
            CommonTestUtils.waitUtil(
                    () -> {
                        Map<Integer, Integer> counts = snapshotActiveSplitCounts();
                        return counts.size() == readers
                                && counts.values().stream().mapToInt(Integer::intValue).sum()
                                        == totalSplits
                                && Collections.max(counts.values())
                                                - Collections.min(counts.values())
                                        <= 1;
                    },
                    Duration.ofSeconds(30),
                    "Active split counts did not balance " + totalSplits + " splits");
        }

        private void waitForHeldReturningAssignment(
                SharedReference<ReaderAttemptObservations> observations, int readers)
                throws Exception {
            CommonTestUtils.waitUtil(
                    () ->
                            observations.applySync(
                                    state -> state.assignmentReaders.size() == readers),
                    Duration.ofSeconds(30),
                    "Readers did not receive returning assignments after the handoff checkpoint");
        }

        private File latestCompletedCheckpoint(JobClient job) {
            File jobDirectory =
                    new File(
                            new File(testDir, "retained-removed-cluster-checkpoints"),
                            job.getJobID().toString());
            File[] files = jobDirectory.listFiles();
            assertThat(files).isNotNull();
            return Stream.of(files)
                    .filter(File::isDirectory)
                    .filter(file -> file.getName().matches("chk-\\d+"))
                    .filter(file -> new File(file, "_metadata").isFile())
                    .max(Comparator.comparingLong(this::checkpointId))
                    .orElseThrow(
                            () ->
                                    new AssertionError(
                                            "No completed checkpoint for " + job.getJobID()));
        }

        private long checkpointId(File checkpoint) {
            return Long.parseLong(checkpoint.getName().substring("chk-".length()));
        }

        private void waitForClusterPartitionAssignments(String clusterId) throws Exception {
            CommonTestUtils.waitUtil(
                    () ->
                            findKafkaClusterMetrics(reporter).stream()
                                            .filter(
                                                    name ->
                                                            name.contains(
                                                                    ".kafkaCluster."
                                                                            + clusterId
                                                                            + "."))
                                            .filter(name -> name.endsWith(".currentOffset"))
                                            .count()
                                    == NUM_PARTITIONS,
                    Duration.ofSeconds(30),
                    "Kafka partitions were not assigned for " + clusterId);
        }

        private void waitForCollectedRecords(
                SharedReference<List<Integer>> collectedRecords,
                JobClient jobClient,
                int expectedCount,
                String message)
                throws Exception {
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            throwIfJobFailed(jobClient);
                        } catch (Exception exception) {
                            throw new RuntimeException(exception);
                        }
                        return collectedRecords.applySync(
                                records -> records.size() >= expectedCount);
                    },
                    Duration.ofSeconds(30),
                    message);
        }

        private List<Integer> copyCollectedRecords(
                SharedReference<List<Integer>> collectedRecords) {
            return collectedRecords.applySync(ArrayList::new);
        }

        private void waitForKafkaClusterMetricsToDisappear(String kafkaClusterId) throws Exception {
            CommonTestUtils.waitUtil(
                    () -> !hasKafkaClusterMetrics(kafkaClusterId),
                    Duration.ofSeconds(30),
                    "Could not observe removed Kafka cluster metrics disappear");
        }

        private void waitForOnlyKafkaClusterMetrics(String kafkaClusterId) throws Exception {
            CommonTestUtils.waitUtil(
                    () -> {
                        Set<String> metrics = findKafkaClusterMetrics(reporter);
                        return !metrics.isEmpty()
                                && metrics.stream()
                                        .allMatch(
                                                metricName ->
                                                        metricName.contains(
                                                                ".kafkaCluster." + kafkaClusterId));
                    },
                    Duration.ofSeconds(30),
                    "Could not observe only Kafka cluster metrics for " + kafkaClusterId);
        }

        private boolean hasKafkaClusterMetrics(String kafkaClusterId) {
            return findKafkaClusterMetrics(reporter).stream()
                    .anyMatch(metricName -> metricName.contains(".kafkaCluster." + kafkaClusterId));
        }

        private String triggerAndCompleteCheckpoint(JobClient job) throws Exception {
            // Submit after the condition under test, and await a checkpoint of this exact job.
            // Filesystem lookup can return a checkpoint from an earlier, canceled job.
            return miniClusterResource
                    .getMiniCluster()
                    .triggerCheckpoint(job.getJobID())
                    .get(30, TimeUnit.SECONDS);
        }

        private String retainedCheckpointAfterCancellation(String postRemovalCheckpoint)
                throws Exception {
            File minimumCheckpoint = new File(URI.create(postRemovalCheckpoint)).getCanonicalFile();
            File[] jobFiles = minimumCheckpoint.getParentFile().listFiles();
            assertThat(jobFiles).isNotNull();
            File retainedCheckpoint =
                    Stream.of(jobFiles)
                            .filter(File::isDirectory)
                            .filter(file -> file.getName().matches("chk-\\d+"))
                            .filter(file -> new File(file, "_metadata").isFile())
                            .max(
                                    Comparator.comparingLong(
                                            file ->
                                                    Long.parseLong(
                                                            file.getName()
                                                                    .substring("chk-".length()))))
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "No retained completed checkpoint for "
                                                            + minimumCheckpoint));
            // Periodic checkpoints may subsume the explicitly triggered one before cancellation
            // finishes. After terminal cancellation no newer checkpoint can replace this file.
            assertThat(retainedCheckpoint).isDirectory();
            assertThat(new File(retainedCheckpoint, "_metadata")).isFile();
            assertThat(retainedCheckpoint.getParentFile())
                    .as("checkpoint belongs to the canceled job")
                    .isEqualTo(minimumCheckpoint.getParentFile());
            assertThat(retainedCheckpoint.getName()).matches("chk-\\d+");
            assertThat(minimumCheckpoint.getName()).matches("chk-\\d+");
            long retainedCheckpointId =
                    Long.parseLong(retainedCheckpoint.getName().substring("chk-".length()));
            long minimumCheckpointId =
                    Long.parseLong(minimumCheckpoint.getName().substring("chk-".length()));
            assertThat(retainedCheckpointId)
                    .as("restored checkpoint was triggered after cluster removal")
                    .isGreaterThanOrEqualTo(minimumCheckpointId);
            return retainedCheckpoint.toURI().toString();
        }

        private void cancelJob(JobClient jobClient) throws Exception {
            if (jobClient != null) {
                try {
                    jobClient.cancel().get(30, TimeUnit.SECONDS);
                } catch (ExecutionException executionException) {
                    if (!(executionException.getCause()
                            instanceof FlinkJobTerminatedWithoutCancellationException)) {
                        throw executionException;
                    }
                }
                // cancel() acknowledges the request before task shutdown. Reusing the transactional
                // IDs while the old Kafka writers are still closing can fence the restored job.
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                return jobClient
                                        .getJobStatus()
                                        .get(5, TimeUnit.SECONDS)
                                        .isGloballyTerminalState();
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }
                        },
                        Duration.ofSeconds(30),
                        "Canceled job did not finish shutting down before checkpoint restore");
            }
        }

        private void throwIfJobFailed(JobClient jobClient) throws Exception {
            if (jobClient.getJobStatus().get(30, TimeUnit.SECONDS) != JobStatus.FAILED) {
                return;
            }

            try {
                jobClient.getJobExecutionResult().get(30, TimeUnit.SECONDS);
            } catch (ExecutionException executionException) {
                throw new RuntimeException(
                        "Dynamic source job failed before expected records were collected",
                        executionException.getCause());
            }
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

        private void waitForInitialSplitAssignments(
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context) throws Exception {
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            runAllOneTimeCallables(context);
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                        return !context.getSplitsAssignmentSequence().isEmpty();
                    },
                    Duration.ofSeconds(10),
                    "Initial dynamic Kafka split assignment did not complete");
        }

        private void verifyAllSplitsAssignedOnce(
                List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments,
                Set<KafkaStream> kafkaStreams) {
            Map<String, Integer> assignmentFrequency = new HashMap<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> step : assignments) {
                for (List<DynamicKafkaSourceSplit> splits : step.assignment().values()) {
                    for (DynamicKafkaSourceSplit split : splits) {
                        assignmentFrequency.merge(split.splitId(), 1, Integer::sum);
                    }
                }
            }

            int expectedSplits =
                    kafkaStreams.stream()
                            .flatMap(stream -> stream.getClusterMetadataMap().entrySet().stream())
                            .mapToInt(entry -> entry.getValue().getTopics().size() * NUM_PARTITIONS)
                            .sum();
            assertThat(assignmentFrequency).hasSize(expectedSplits);
            assertThat(assignmentFrequency.values()).allMatch(count -> count == 1);
        }

        private void assertAssignmentsBalanced(
                List<SplitsAssignment<DynamicKafkaSourceSplit>> assignments, int numReaders) {
            Map<Integer, Integer> assignedSplitCountByReader = new HashMap<>();
            for (int readerId = 0; readerId < numReaders; readerId++) {
                assignedSplitCountByReader.put(readerId, 0);
            }
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment : assignments) {
                for (Map.Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assignedSplitCountByReader.merge(
                            entry.getKey(), entry.getValue().size(), Integer::sum);
                }
            }

            int minAssignedSplits = Collections.min(assignedSplitCountByReader.values());
            int maxAssignedSplits = Collections.max(assignedSplitCountByReader.values());
            assertThat(maxAssignedSplits - minAssignedSplits).isLessThanOrEqualTo(1);
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

    private static final class ReaderAttemptObservations {
        private final Map<Integer, Integer> attempts = new HashMap<>();
        private final Map<Integer, Integer> readerByRecord = new HashMap<>();
        private final Set<Integer> recordsWithMultipleOwners = new TreeSet<>();
        private RetainedCleanupBarrier cleanupBarrier;
        private final Map<Integer, Long> firstReportedCheckpointByReader = new HashMap<>();
        private final Set<Integer> assignmentReaders = new TreeSet<>();
        private final Map<Integer, Long> reportedHandoffIds = new HashMap<>();
        private final Map<Integer, Long> reportBarrierAcks = new HashMap<>();
        private boolean observeReportBarrier;
        private String handoffClusterId;
        private TopicPartition transferredPartition;
        private int readerToFail = -1;
        private int failOnRequestReader = -1;
        private int failOnAssignmentReader = -1;
        private int failOnCleanupReader = -1;
        private int requestFailures;
        private int assignmentFailures;
        private int cleanupFailures;
        private boolean holdReturningAssignment;
        // Test-only legacy producer: old readers computed a deadline independently of the JM.
        private long legacyReaderDeadlineSkew;
        private int legacySkewedSnapshots;
    }

    /** Runs the real connector; only observes protocol events and injects task failures. */
    private static final class ObservedHandoffSource
            implements Source<Integer, DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState>,
                    SupportsSplitReassignmentOnRecovery,
                    ResultTypeQueryable<Integer> {
        private final DynamicKafkaSource<Integer> delegate;
        private final SharedReference<ReaderAttemptObservations> observations;

        private ObservedHandoffSource(
                DynamicKafkaSource<Integer> delegate,
                SharedReference<ReaderAttemptObservations> observations) {
            this.delegate = delegate;
            this.observations = observations;
        }

        @Override
        public Boundedness getBoundedness() {
            return delegate.getBoundedness();
        }

        @Override
        public SourceReader<Integer, DynamicKafkaSourceSplit> createReader(
                SourceReaderContext context) throws Exception {
            return new ObservedHandoffReader(delegate.createReader(context), context, observations);
        }

        @Override
        public SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState>
                createEnumerator(SplitEnumeratorContext<DynamicKafkaSourceSplit> context)
                        throws Exception {
            return new ObservedHandoffEnumerator(
                    delegate.createEnumerator(context), context, observations);
        }

        @Override
        public SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState>
                restoreEnumerator(
                        SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
                        DynamicKafkaSourceEnumState checkpoint)
                        throws Exception {
            return new ObservedHandoffEnumerator(
                    delegate.restoreEnumerator(context, checkpoint), context, observations);
        }

        @Override
        public SimpleVersionedSerializer<DynamicKafkaSourceSplit> getSplitSerializer() {
            return delegate.getSplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<DynamicKafkaSourceEnumState>
                getEnumeratorCheckpointSerializer() {
            return delegate.getEnumeratorCheckpointSerializer();
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return delegate.getProducedType();
        }
    }

    /** Observes real reports without changing handoff or checkpoint decisions. */
    private static final class ObservedHandoffEnumerator
            implements SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState> {
        private final SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState>
                delegate;
        private final SplitEnumeratorContext<DynamicKafkaSourceSplit> context;
        private final SharedReference<ReaderAttemptObservations> observations;
        private long lastBarrierRound = -1;

        private ObservedHandoffEnumerator(
                SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState> delegate,
                SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
                SharedReference<ReaderAttemptObservations> observations) {
            this.delegate = delegate;
            this.context = context;
            this.observations = observations;
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            delegate.handleSplitRequest(subtaskId, requesterHostname);
        }

        @Override
        public void addSplitsBack(List<DynamicKafkaSourceSplit> splits, int subtaskId) {
            delegate.addSplitsBack(splits, subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            RetainedCleanupBarrier barrier = observations.applySync(state -> state.cleanupBarrier);
            if (barrier != null) {
                barrier.recordRestoredState(context, subtaskId);
            }
            delegate.addReader(subtaskId);
        }

        @Override
        public DynamicKafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            delegate.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            delegate.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void handleSourceEvent(int subtaskId, SourceEvent event) {
            if (event instanceof HandoffObservationBarrier) {
                long handoffId = ((HandoffObservationBarrier) event).handoffId;
                observations.consumeSync(
                        state -> state.reportBarrierAcks.put(subtaskId, handoffId));
                return;
            }
            delegate.handleSourceEvent(subtaskId, event);
            if (event instanceof RetainedSplitOffsetsEvent) {
                long handoffId = ((RetainedSplitOffsetsEvent) event).getHandoffId();
                boolean allReports =
                        observations.applySync(
                                state -> {
                                    state.reportedHandoffIds.put(subtaskId, handoffId);
                                    return state.observeReportBarrier
                                            && state.reportedHandoffIds.size()
                                                    == context.currentParallelism()
                                            && state.reportedHandoffIds.values().stream()
                                                    .allMatch(id -> id == handoffId);
                                });
                if (allReports && handoffId > lastBarrierRound) {
                    lastBarrierRound = handoffId;
                    for (int reader : context.registeredReaders().keySet()) {
                        context.sendEventToSourceReader(
                                reader, new HandoffObservationBarrier(handoffId));
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class HandoffObservationBarrier implements SourceEvent {
        private static final long serialVersionUID = 1L;
        private final long handoffId;

        private HandoffObservationBarrier(long handoffId) {
            this.handoffId = handoffId;
        }
    }

    private static final class ObservedHandoffReader
            implements SourceReader<Integer, DynamicKafkaSourceSplit> {
        private final SourceReader<Integer, DynamicKafkaSourceSplit> delegate;
        private final SourceReaderContext context;
        private final int subtask;
        private final SharedReference<ReaderAttemptObservations> observations;
        private boolean reportedHandoff;

        private ObservedHandoffReader(
                SourceReader<Integer, DynamicKafkaSourceSplit> delegate,
                SourceReaderContext context,
                SharedReference<ReaderAttemptObservations> observations) {
            this.delegate = delegate;
            this.context = context;
            this.subtask = context.getIndexOfSubtask();
            this.observations = observations;
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) throws Exception {
            return delegate.pollNext(output);
        }

        @Override
        public List<DynamicKafkaSourceSplit> snapshotState(long checkpointId) {
            List<DynamicKafkaSourceSplit> splits = delegate.snapshotState(checkpointId);
            long skew = observations.applySync(state -> state.legacyReaderDeadlineSkew);
            if (checkpointId >= 0
                    && skew != 0
                    && splits.stream().anyMatch(DynamicKafkaSourceSplit::isRetained)) {
                observations.consumeSync(state -> state.legacySkewedSnapshots++);
                splits =
                        splits.stream()
                                .map(
                                        split ->
                                                split.isRetained()
                                                        ? split.retainUntil(
                                                                split.getRetainedUntilMs() + skew)
                                                        : split)
                                .collect(Collectors.toList());
            }
            if (reportedHandoff && checkpointId >= 0) {
                observations.consumeSync(
                        state ->
                                state.firstReportedCheckpointByReader.putIfAbsent(
                                        subtask, checkpointId));
            }
            return splits;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return delegate.isAvailable();
        }

        @Override
        public void addSplits(List<DynamicKafkaSourceSplit> splits) {
            boolean returning =
                    reportedHandoff
                            && observations.applySync(
                                    state ->
                                            splits.stream()
                                                    .anyMatch(
                                                            split ->
                                                                    split.getKafkaClusterId()
                                                                                    .equals(
                                                                                            state.handoffClusterId)
                                                                            && !split
                                                                                    .isRetained()));
            if (returning) {
                observations.consumeSync(state -> state.assignmentReaders.add(subtask));
                while (observations.applySync(state -> state.holdReturningAssignment)) {
                    try {
                        Thread.sleep(10L);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(
                                "Interrupted while holding returning assignment", interrupted);
                    }
                }
            }
            delegate.addSplits(splits);
            boolean fail =
                    returning
                            && observations.applySync(
                                    state -> {
                                        if (state.failOnAssignmentReader == subtask
                                                && splits.stream()
                                                        .anyMatch(
                                                                split ->
                                                                        split.getKafkaClusterId()
                                                                                        .equals(
                                                                                                state.handoffClusterId)
                                                                                && split.getTopicPartition()
                                                                                        .equals(
                                                                                                state.transferredPartition))) {
                                            state.failOnAssignmentReader = -1;
                                            state.assignmentFailures++;
                                            return true;
                                        }
                                        return false;
                                    });
            if (fail) {
                throw new IllegalStateException(
                        "Requested new-owner failure after returning assignment");
            }
        }

        @Override
        public void notifyNoMoreSplits() {
            delegate.notifyNoMoreSplits();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            delegate.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            delegate.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            delegate.pauseOrResumeSplits(splitsToPause, splitsToResume);
        }

        @Override
        public void handleSourceEvents(SourceEvent event) {
            if (event instanceof HandoffObservationBarrier) {
                context.sendSourceEventToCoordinator(event);
                return;
            }
            if (event instanceof RequestRetainedSplitOffsetsEvent) {
                boolean fail =
                        observations.applySync(
                                state -> {
                                    if (state.failOnRequestReader == subtask) {
                                        state.failOnRequestReader = -1;
                                        state.requestFailures++;
                                        return true;
                                    }
                                    return false;
                                });
                if (fail) {
                    throw new IllegalStateException(
                            "Requested failure before retained-offset report");
                }
                reportedHandoff = true;
            }
            boolean cleanup =
                    reportedHandoff
                            && observations.applySync(
                                    state ->
                                            RetainedCleanupBarrier.isCleanupEvent(
                                                    event, state.handoffClusterId));
            RetainedCleanupBarrier barrier =
                    observations.applySync(
                            state ->
                                    state.failOnCleanupReader == subtask
                                            ? state.cleanupBarrier
                                            : null);
            if (cleanup && barrier != null) {
                barrier.beforeCleanup(delegate.snapshotState(-1));
            }
            delegate.handleSourceEvents(event);
            if (cleanup
                    && observations.applySync(
                            state -> {
                                if (state.failOnCleanupReader != subtask) {
                                    return false;
                                }
                                state.failOnCleanupReader = -1;
                                state.cleanupFailures++;
                                return true;
                            })) {
                throw new IllegalStateException(
                        "Requested old-owner failure after retained-shadow cleanup");
            }
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    /** Observes actual task attempts; output correctness is checked in transactional Kafka. */
    private static final class ReaderAttemptObserver extends RichMapFunction<Integer, Integer> {
        private final SharedReference<ReaderAttemptObservations> observations;
        private transient int subtask;

        private ReaderAttemptObserver(SharedReference<ReaderAttemptObservations> observations) {
            this.observations = observations;
        }

        @Override
        public void open(OpenContext openContext) {
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
            observations.consumeSync(state -> state.attempts.put(subtask, attempt));
        }

        @Override
        public Integer map(Integer value) {
            boolean fail =
                    observations.applySync(
                            state -> {
                                Integer previousOwner = state.readerByRecord.put(value, subtask);
                                if (previousOwner != null && previousOwner != subtask) {
                                    state.recordsWithMultipleOwners.add(value);
                                }
                                if (state.readerToFail == subtask) {
                                    state.readerToFail = -1;
                                    return true;
                                }
                                return false;
                            });
            if (fail) {
                throw new IllegalStateException(
                        "Requested source reader region failure " + subtask);
            }
            return value;
        }
    }

    private static final class CollectingSink extends RichSinkFunction<Integer> {
        private final SharedReference<List<Integer>> collectedRecords;

        private CollectingSink(SharedReference<List<Integer>> collectedRecords) {
            this.collectedRecords = collectedRecords;
        }

        @Override
        public void invoke(Integer value, Context context) {
            collectedRecords.consumeSync(records -> records.add(value));
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
