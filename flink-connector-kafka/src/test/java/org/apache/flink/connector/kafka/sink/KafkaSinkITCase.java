/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.testutils.KafkaSinkExternalContextFactory;
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for using KafkaSink writing to a Kafka cluster. */
@Testcontainers
public class KafkaSinkITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static AdminClient admin;

    private String topic;

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(
                            "kafka.KafkaSinkITCase-"
                                    + System.nanoTime()
                                    + "-"
                                    + Thread.currentThread().getId())
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @RegisterExtension
    public final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @BeforeAll
    public static void setupAdmin() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
    }

    @AfterAll
    public static void teardownAdmin() {
        admin.close();
    }

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        topic = UUID.randomUUID().toString();
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        checkProducerLeak();
        deleteTestTopic(topic);
    }

    /** Integration test based on connector testing framework. */
    @SuppressWarnings("unused")
    @Nested
    class IntegrationTests extends SinkTestSuiteBase<String> {
        // Defines test environment on Flink MiniCluster
        @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

        // Defines external system
        @TestExternalSystem
        DefaultContainerizedExternalSystem<KafkaContainer> kafka =
                DefaultContainerizedExternalSystem.builder()
                        .fromContainer(
                                new KafkaContainer(
                                        DockerImageName.parse(DockerImageVersions.KAFKA)))
                        .build();

        @TestSemantics
        CheckpointingMode[] semantics =
                new CheckpointingMode[] {
                    CheckpointingMode.EXACTLY_ONCE, CheckpointingMode.AT_LEAST_ONCE
                };

        @TestContext
        KafkaSinkExternalContextFactory incrementing =
                new KafkaSinkExternalContextFactory(
                        kafka.getContainer(),
                        Collections.emptyList(),
                        TransactionNamingStrategy.INCREMENTING);

        @TestContext
        KafkaSinkExternalContextFactory pooling =
                new KafkaSinkExternalContextFactory(
                        kafka.getContainer(),
                        Collections.emptyList(),
                        TransactionNamingStrategy.POOLING);
    }

    //    @Test
    //    public void testWriteRecordsToKafkaWithAtLeastOnceGuarantee() throws Exception {
    //        writeRecordsToKafka(DeliveryGuarantee.AT_LEAST_ONCE);
    //    }
    //
    //    @Test
    //    public void testWriteRecordsToKafkaWithNoneGuarantee() throws Exception {
    //        writeRecordsToKafka(DeliveryGuarantee.NONE);
    //    }
    //
    //    @ParameterizedTest(name = "{0}, chained={1}")
    //    @MethodSource("getEOSParameters")
    //    public void testWriteRecordsToKafkaWithExactlyOnceGuarantee(
    //            TransactionNamingStrategy namingStrategy, boolean chained) throws Exception {
    //        writeRecordsToKafka(DeliveryGuarantee.EXACTLY_ONCE, namingStrategy, chained);
    //    }
    //
    //    @Test
    //    public void testWriteRecordsToKafkaWithExactlyOnceGuaranteeBatch() throws Exception {
    //        final StreamExecutionEnvironment env =
    //                StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration(1));
    //        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    //        int count = 1000;
    //        final DataStream<Long> source = createSource(env, false, count);
    //        source.sinkTo(
    //                new KafkaSinkBuilder<Long>()
    //                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
    //                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    //                        .setRecordSerializer(
    //                                KafkaRecordSerializationSchema.builder()
    //                                        .setTopic(topic)
    //                                        .setValueSerializationSchema(new RecordSerializer())
    //                                        .build())
    //                        .setTransactionalIdPrefix("kafka-sink")
    //                        .build());
    //        env.execute();
    //
    //        final List<Long> collectedRecords =
    //                deserializeValues(drainAllRecordsFromTopic(topic, true));
    //        assertThat(collectedRecords).hasSize(count);
    //    }

    static Stream<Arguments> getEOSParameters() {
        return Arrays.stream(TransactionNamingStrategy.values())
                .flatMap(
                        strategy ->
                                Stream.of(true, false)
                                        .map(chained -> Arguments.of(strategy, chained)));
    }

    //    @Test
    //    public void testRecoveryWithAtLeastOnceGuarantee() throws Exception {
    //        testRecoveryWithAssertion(DeliveryGuarantee.AT_LEAST_ONCE, 1);
    //    }

    @ParameterizedTest(name = "{0}, chained={1}")
    @MethodSource("getEOSParameters")
    public void testRecoveryWithExactlyOnceGuarantee(
            TransactionNamingStrategy namingStrategy, boolean chained) throws Exception {
        LOG.info(
                "========== STARTING testRecoveryWithExactlyOnceGuarantee with namingStrategy={}, chained={} ==========",
                namingStrategy,
                chained);
        testRecoveryWithAssertion(DeliveryGuarantee.EXACTLY_ONCE, 1, namingStrategy, chained);
    }

    //    @ParameterizedTest(name = "{0}, chained={1}")
    //    @MethodSource("getEOSParameters")
    //    public void testRecoveryWithExactlyOnceGuaranteeAndConcurrentCheckpoints(
    //            TransactionNamingStrategy namingStrategy, boolean chained) throws Exception {
    //        LOG.info(
    //                "========== STARTING
    // testRecoveryWithExactlyOnceGuaranteeAndConcurrentCheckpoints with namingStrategy={},
    // chained={} ==========",
    //                namingStrategy,
    //                chained);
    //        testRecoveryWithAssertion(DeliveryGuarantee.EXACTLY_ONCE, 2, namingStrategy, chained);
    //    }
    //
    //    @ParameterizedTest(name = "{0}, chained={1}")
    //    @MethodSource("getEOSParameters")
    //    public void testAbortTransactionsOfPendingCheckpointsAfterFailure(
    //            TransactionNamingStrategy namingStrategy,
    //            boolean chained,
    //            @TempDir File checkpointDir,
    //            @InjectMiniCluster MiniCluster miniCluster,
    //            @InjectClusterClient ClusterClient<?> clusterClient)
    //            throws Exception {
    //        // Run a first job failing during the async phase of a checkpoint to leave some
    //        // lingering transactions
    //        final Configuration config = createConfiguration(4);
    //        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    //        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
    // checkpointDir.toURI().toString());
    //        config.set(
    //                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
    //                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    //        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
    //        JobID firstJobId = null;
    //        SharedReference<Set<Long>> checkpointedRecords =
    //                sharedObjects.add(new ConcurrentSkipListSet<>());
    //        try {
    //            firstJobId =
    //                    executeWithMapper(
    //                            new FailAsyncCheckpointMapper(1),
    //                            checkpointedRecords,
    //                            config,
    //                            namingStrategy,
    //                            chained,
    //                            "firstPrefix",
    //                            clusterClient);
    //        } catch (Exception e) {
    //            assertThat(e).hasStackTraceContaining("Exceeded checkpoint tolerable failure");
    //        }
    //        final Optional<String> completedCheckpoint =
    //                CommonTestUtils.getLatestCompletedCheckpointPath(firstJobId, miniCluster);
    //
    //        assertThat(completedCheckpoint).isPresent();
    //        config.set(SAVEPOINT_PATH, completedCheckpoint.get());
    //
    //        // Run a second job which aborts all lingering transactions and new consumer should
    //        // immediately see the newly written records
    //        SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(true));
    //        executeWithMapper(
    //                new FailingCheckpointMapper(failed),
    //                checkpointedRecords,
    //                config,
    //                namingStrategy,
    //                chained,
    //                "newPrefix",
    //                clusterClient);
    //        final List<Long> committedRecords =
    //                deserializeValues(drainAllRecordsFromTopic(topic, true));
    //
    // assertThat(committedRecords).containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
    //    }
    //
    //    @ParameterizedTest(name = "{0}, chained={1}")
    //    @MethodSource("getEOSParameters")
    //    public void testAbortTransactionsAfterScaleInBeforeFirstCheckpoint(
    //            TransactionNamingStrategy namingStrategy,
    //            boolean chained,
    //            @InjectClusterClient ClusterClient<?> clusterClient)
    //            throws Exception {
    //        // Run a first job opening 5 transactions one per subtask and fail in async checkpoint
    // phase
    //        try {
    //            SharedReference<Set<Long>> checkpointedRecords =
    //                    sharedObjects.add(new ConcurrentSkipListSet<>());
    //            Configuration config = createConfiguration(5);
    //            executeWithMapper(
    //                    new FailAsyncCheckpointMapper(0),
    //                    checkpointedRecords,
    //                    config,
    //                    namingStrategy,
    //                    chained,
    //                    null,
    //                    clusterClient);
    //        } catch (Exception e) {
    //            assertThat(e.getCause().getCause().getMessage())
    //                    .contains("Exceeded checkpoint tolerable failure");
    //        }
    //        assertThat(deserializeValues(drainAllRecordsFromTopic(topic, true))).isEmpty();
    //
    //        // Second job aborts all transactions from previous runs with higher parallelism
    //        SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(true));
    //        SharedReference<Set<Long>> checkpointedRecords =
    //                sharedObjects.add(new ConcurrentSkipListSet<>());
    //        Configuration config = createConfiguration(1);
    //        executeWithMapper(
    //                new FailingCheckpointMapper(failed),
    //                checkpointedRecords,
    //                config,
    //                namingStrategy,
    //                chained,
    //                null,
    //                clusterClient);
    //        final List<Long> committedRecords =
    //                deserializeValues(drainAllRecordsFromTopic(topic, true));
    //
    // assertThat(committedRecords).containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
    //    }
    //
    //    @ParameterizedTest(name = "{0}->{1}")
    //    @CsvSource({"1,2", "2,3", "2,5", "3,5", "5,6", "6,5", "5,2", "5,3", "3,2", "2,1"})
    //    public void rescaleListing(
    //            int oldParallelism,
    //            int newParallelsm,
    //            @TempDir File checkpointDir,
    //            @InjectMiniCluster MiniCluster miniCluster,
    //            @InjectClusterClient ClusterClient<?> clusterClient)
    //            throws Exception {
    //        // Run a first job failing during the async phase of a checkpoint to leave some
    //        // lingering transactions
    //        final Configuration config = createConfiguration(oldParallelism);
    //        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    //        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
    // checkpointDir.toURI().toString());
    //        config.set(
    //                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
    //                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    //        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
    //        SharedReference<Set<Long>> checkpointedRecords =
    //                sharedObjects.add(new ConcurrentSkipListSet<>());
    //
    //        JobID firstJobId =
    //                executeWithMapper(
    //                        new FailAsyncCheckpointMapper(1),
    //                        checkpointedRecords,
    //                        config,
    //                        TransactionNamingStrategy.POOLING,
    //                        false,
    //                        "firstPrefix",
    //                        clusterClient);
    //
    //        config.set(SAVEPOINT_PATH, getCheckpointPath(miniCluster, firstJobId));
    //        config.set(CoreOptions.DEFAULT_PARALLELISM, newParallelsm);
    //
    //        // Run a second job which aborts all lingering transactions and new consumer should
    //        // immediately see the newly written records
    //        JobID secondJobId =
    //                executeWithMapper(
    //                        new FailAsyncCheckpointMapper(1),
    //                        checkpointedRecords,
    //                        config,
    //                        TransactionNamingStrategy.POOLING,
    //                        false,
    //                        "secondPrefix",
    //                        clusterClient);
    //
    //        config.set(SAVEPOINT_PATH, getCheckpointPath(miniCluster, secondJobId));
    //        config.set(CoreOptions.DEFAULT_PARALLELISM, oldParallelism);
    //
    //        SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(true));
    //        executeWithMapper(
    //                new FailingCheckpointMapper(failed),
    //                checkpointedRecords,
    //                config,
    //                TransactionNamingStrategy.POOLING,
    //                false,
    //                "thirdPrefix",
    //                clusterClient);
    //
    //        final List<Long> committedRecords =
    //                deserializeValues(drainAllRecordsFromTopic(topic, true));
    //
    // assertThat(committedRecords).containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
    //    }
    //
    //    private String getCheckpointPath(MiniCluster miniCluster, JobID secondJobId)
    //            throws InterruptedException, ExecutionException, FlinkJobNotFoundException {
    //        final Optional<String> completedCheckpoint =
    //                CommonTestUtils.getLatestCompletedCheckpointPath(secondJobId, miniCluster);
    //
    //        assertThat(completedCheckpoint).isPresent();
    //        return completedCheckpoint.get();
    //    }
    //
    //    @ParameterizedTest
    //    @ValueSource(booleans = {true, false})
    //    public void checkMigration(
    //            boolean supportedMigration,
    //            @TempDir File checkpointDir,
    //            @InjectMiniCluster MiniCluster miniCluster,
    //            @InjectClusterClient ClusterClient<?> clusterClient)
    //            throws Exception {
    //        // Run a first job failing during the async phase of a checkpoint to leave some
    //        // lingering transactions
    //        final Configuration config = createConfiguration(5);
    //        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    //        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
    // checkpointDir.toURI().toString());
    //        config.set(
    //                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
    //                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    //        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 2);
    //        SharedReference<Set<Long>> checkpointedRecords =
    //                sharedObjects.add(new ConcurrentSkipListSet<>());
    //
    //        JobID firstJobId =
    //                executeWithMapper(
    //                        new FailAsyncCheckpointMapper(1),
    //                        checkpointedRecords,
    //                        config,
    //                        TransactionNamingStrategy.INCREMENTING,
    //                        true,
    //                        "firstPrefix",
    //                        clusterClient);
    //
    //        // Run a second job which switching to POOLING
    //        config.set(SAVEPOINT_PATH, getCheckpointPath(miniCluster, firstJobId));
    //        config.set(CoreOptions.DEFAULT_PARALLELISM, 5);
    //        JobID secondJobId2 =
    //                executeWithMapper(
    //                        new FailAsyncCheckpointMapper(1),
    //                        checkpointedRecords,
    //                        config,
    //                        TransactionNamingStrategy.POOLING,
    //                        true,
    //                        "secondPrefix",
    //                        clusterClient);
    //
    //        // Run a third job with downscaling
    //        config.set(SAVEPOINT_PATH, getCheckpointPath(miniCluster, secondJobId2));
    //        config.set(CoreOptions.DEFAULT_PARALLELISM, 3);
    //        JobID thirdJobId =
    //                executeWithMapper(
    //                        v -> v,
    //                        checkpointedRecords,
    //                        config,
    //                        supportedMigration
    //                                ? TransactionNamingStrategy.POOLING
    //                                : TransactionNamingStrategy.INCREMENTING,
    //                        true,
    //                        "thirdPrefix",
    //                        clusterClient);
    //
    //        JobResult jobResult = clusterClient.requestJobResult(thirdJobId).get();
    //        assertThat(jobResult.getApplicationStatus())
    //                .isEqualTo(
    //                        supportedMigration
    //                                ? ApplicationStatus.SUCCEEDED
    //                                : ApplicationStatus.FAILED);
    //
    //        if (supportedMigration) {
    //            final List<Long> committedRecords =
    //                    deserializeValues(drainAllRecordsFromTopic(topic, true));
    //            assertThat(committedRecords)
    //                    .containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
    //        } else {
    //            assertThat(jobResult.getSerializedThrowable())
    //                    .get()
    //                    .asInstanceOf(InstanceOfAssertFactories.THROWABLE)
    //                    .rootCause()
    //                    .hasMessageContaining(
    //                            "Attempted to switch the transaction naming strategy back to
    // INCREMENTING");
    //        }
    //    }
    //
    //    @ParameterizedTest
    //    @EnumSource(DeliveryGuarantee.class)
    //    void ensureUniqueTransactionalIdPrefixIfNeeded(DeliveryGuarantee guarantee) throws
    // Exception {
    //        KafkaSinkBuilder<Integer> builder =
    //                new KafkaSinkBuilder<Integer>()
    //                        .setDeliveryGuarantee(guarantee)
    //                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
    //                        .setRecordSerializer(new IntegerRecordSerializer("topic"));
    //
    //        Configuration config = new Configuration();
    //        config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
    //        StreamExecutionEnvironment env =
    // StreamExecutionEnvironment.getExecutionEnvironment(config);
    //        env.enableCheckpointing(100);
    //        DataStreamSource<Integer> source = env.fromData(1, 2);
    //        if (guarantee == DeliveryGuarantee.EXACTLY_ONCE) {
    //            assertThatThrownBy(builder::build).hasMessageContaining("unique");
    //        } else {
    //            source.sinkTo(builder.build());
    //            source.sinkTo(builder.build());
    //
    //            env.execute();
    //        }
    //    }

    private static Configuration createConfiguration(int parallelism) {
        final Configuration config = new Configuration();
        config.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        return config;
    }

    private JobID executeWithMapper(
            MapFunction<Long, Long> mapper,
            SharedReference<Set<Long>> checkpointedRecords,
            Configuration config,
            TransactionNamingStrategy namingStrategy,
            boolean chained,
            @Nullable String transactionalIdPrefix,
            ClusterClient<?> clusterClient)
            throws Exception {

        config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(100L);
        if (!chained) {
            env.disableOperatorChaining();
        }
        final DataStream<Long> source = createThrottlingSource(env);
        final DataStream<Long> stream =
                source.map(mapper).map(new RecordFetcher(checkpointedRecords)).uid("fetcher");
        final KafkaSinkBuilder<Long> builder =
                new KafkaSinkBuilder<Long>()
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new RecordSerializer())
                                        .build())
                        .setTransactionNamingStrategy(namingStrategy);
        if (transactionalIdPrefix == null) {
            transactionalIdPrefix = "kafka-sink";
        }
        builder.setTransactionalIdPrefix(transactionalIdPrefix);
        stream.sinkTo(builder.build());
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        JobID jobID = clusterClient.submitJob(jobGraph).get();
        clusterClient.requestJobResult(jobID).get();
        return jobID;
    }

    private void testRecoveryWithAssertion(
            DeliveryGuarantee guarantee, int maxConcurrentCheckpoints) throws Exception {
        testRecoveryWithAssertion(
                guarantee, maxConcurrentCheckpoints, TransactionNamingStrategy.DEFAULT, true);
    }

    private void testRecoveryWithAssertion(
            DeliveryGuarantee guarantee,
            int maxConcurrentCheckpoints,
            TransactionNamingStrategy namingStrategy,
            boolean chained)
            throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration(1));
        if (!chained) {
            env.disableOperatorChaining();
        }
        env.enableCheckpointing(300L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        final DataStream<Long> source = createThrottlingSource(env);
        SharedReference<Set<Long>> checkpointedRecords =
                sharedObjects.add(new ConcurrentSkipListSet<>());
        DataStream<Long> stream =
                source.map(new FailingCheckpointMapper(sharedObjects.add(new AtomicBoolean(false))))
                        .map(new RecordFetcher(checkpointedRecords));

        stream.sinkTo(
                new KafkaSinkBuilder<Long>()
                        .setDeliveryGuarantee(guarantee)
                        .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new RecordSerializer())
                                        .build())
                        .setTransactionalIdPrefix("kafka-sink")
                        .setTransactionNamingStrategy(namingStrategy)
                        .build());
        LOG.info(
                "========== ABOUT TO CALL env.execute() with namingStrategy={}, chained={} ==========",
                namingStrategy,
                chained);
        env.execute();
        LOG.info(
                "========== SUCCESSFULLY COMPLETED env.execute() with namingStrategy={}, chained={} ==========",
                namingStrategy,
                chained);

        List<Long> committedRecords =
                deserializeValues(
                        drainAllRecordsFromTopic(
                                topic, guarantee == DeliveryGuarantee.EXACTLY_ONCE));

        if (guarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
            assertThat(committedRecords).containsAll(checkpointedRecords.get());
        } else if (guarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            assertThat(committedRecords)
                    .containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
        }
    }

    private void writeRecordsToKafka(DeliveryGuarantee deliveryGuarantee) throws Exception {
        writeRecordsToKafka(deliveryGuarantee, TransactionNamingStrategy.DEFAULT, true);
    }

    private void writeRecordsToKafka(
            DeliveryGuarantee deliveryGuarantee,
            TransactionNamingStrategy namingStrategy,
            boolean chained)
            throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration(1));
        if (!chained) {
            env.disableOperatorChaining();
        }
        env.enableCheckpointing(100L);
        final DataStream<Long> source = createThrottlingSource(env);
        SharedReference<Set<Long>> checkpointedRecords =
                sharedObjects.add(new ConcurrentSkipListSet<>());
        source.map(new RecordFetcher(checkpointedRecords))
                .sinkTo(
                        new KafkaSinkBuilder<Long>()
                                .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                                .setDeliveryGuarantee(deliveryGuarantee)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(topic)
                                                .setValueSerializationSchema(new RecordSerializer())
                                                .build())
                                .setTransactionalIdPrefix("kafka-sink")
                                .setTransactionNamingStrategy(namingStrategy)
                                .build());
        env.execute();

        final List<Long> collectedRecords =
                deserializeValues(
                        drainAllRecordsFromTopic(
                                topic, deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE));
        assertThat(collectedRecords).containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
    }

    private DataStream<Long> createThrottlingSource(StreamExecutionEnvironment env) {
        return createSource(env, true, 1000);
    }

    private DataStream<Long> createSource(
            StreamExecutionEnvironment env, boolean throttled, int count) {
        return env.fromSource(
                new DataGeneratorSource<>(
                        value -> value,
                        count,
                        throttled
                                ? RateLimiterStrategy.perCheckpoint(10)
                                : RateLimiterStrategy.noOp(),
                        BasicTypeInfo.LONG_TYPE_INFO),
                WatermarkStrategy.noWatermarks(),
                "Generator Source");
    }

    private static List<Long> deserializeValues(List<ConsumerRecord<byte[], byte[]>> records) {
        return records.stream()
                .map(
                        record -> {
                            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                            final byte[] value = record.value();
                            buffer.put(value, 0, value.length);
                            buffer.flip();
                            return buffer.getLong();
                        })
                .collect(Collectors.toList());
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
        return standardProps;
    }

    private void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }

    private void deleteTestTopic(String topic) throws ExecutionException, InterruptedException {
        final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
        result.all().get();
    }

    private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, boolean committed) {
        Properties properties = getKafkaClientConfiguration();
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties, committed);
    }

    private static class RecordSerializer implements SerializationSchema<Long> {

        @Override
        public byte[] serialize(Long element) {
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(element);
            return buffer.array();
        }
    }

    /**
     * Fetches records that have been successfully checkpointed. It relies on final checkpoints and
     * subsumption to ultimately, emit all records that have been checkpointed.
     *
     * <p>Note that the current implementation only works by operating on a set because on failure,
     * we may up with duplicate records being added to the {@link #checkpointedRecords}.
     *
     * <p>The fetcher uses three states to manage the records:
     *
     * <ol>
     *   <li>{@link #recordsSinceLastCheckpoint} is used to buffer records between checkpoints.
     *   <li>{@link #snapshottedRecords} is used to store the records that have been checkpointed.
     *   <li>{@link #checkpointedRecords} is used to store snapshottedRecords where the checkpoint
     *       has been acknowledged.
     * </ol>
     *
     * <p>Records are promoted from data structure to the next (e.g. removed from the lower level).
     */
    private static class RecordFetcher
            implements MapFunction<Long, Long>, CheckpointedFunction, CheckpointListener {
        private final SharedReference<Set<Long>> checkpointedRecords;
        private final List<Long> recordsSinceLastCheckpoint = new ArrayList<>();
        private static final ListStateDescriptor<Long> STATE_DESCRIPTOR =
                new ListStateDescriptor<>("committed-records", BasicTypeInfo.LONG_TYPE_INFO);
        private ListState<Long> snapshottedRecords;

        private RecordFetcher(SharedReference<Set<Long>> checkpointedRecords) {
            this.checkpointedRecords = checkpointedRecords;
        }

        @Override
        public Long map(Long value) {
            recordsSinceLastCheckpoint.add(value);
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            // sync with shared object, this is guaranteed to sync eventually because of final
            // checkpoint
            ArrayList<Long> committedRecords = new ArrayList<>();
            snapshottedRecords.get().forEach(committedRecords::add);
            checkpointedRecords.get().addAll(committedRecords);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            LOG.info(
                    "snapshotState {} @ {}", recordsSinceLastCheckpoint, context.getCheckpointId());
            snapshottedRecords.addAll(recordsSinceLastCheckpoint);
            recordsSinceLastCheckpoint.clear();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            snapshottedRecords = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
        }
    }

    private static class FailAsyncCheckpointMapper
            implements MapFunction<Long, Long>, CheckpointedFunction {
        private static final ListStateDescriptor<Integer> stateDescriptor =
                new ListStateDescriptor<>("test-state", new SlowSerializer());
        private int failAfterCheckpoint;

        private ListState<Integer> state;

        public FailAsyncCheckpointMapper(int failAfterCheckpoint) {
            this.failAfterCheckpoint = failAfterCheckpoint;
        }

        @Override
        public Long map(Long value) throws Exception {
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            if (failAfterCheckpoint <= 0) {
                // Trigger a failure in the serializer
                state.add(-1);
            } else {
                state.add(1);
            }
            failAfterCheckpoint--;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(stateDescriptor);
        }
    }

    private static class SlowSerializer extends TypeSerializerSingleton<Integer> {

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public Integer createInstance() {
            return 1;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) {
            if (record != -1) {
                return;
            }
            throw new RuntimeException("Expected failure during async checkpoint phase");
        }

        @Override
        public Integer deserialize(DataInputView source) {
            return 1;
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) {
            return 1;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {}

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new SlowSerializerSnapshot();
        }
    }

    /** Snapshot used in {@link FailAsyncCheckpointMapper}. */
    public static class SlowSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {
        public SlowSerializerSnapshot() {
            super(SlowSerializer::new);
        }
    }

    /** Fails after a checkpoint is taken and the next record was emitted. */
    private static class FailingCheckpointMapper
            implements MapFunction<Long, Long>, CheckpointListener {

        private final SharedReference<AtomicBoolean> failed;
        private long lastCheckpointId = 0;
        private int emittedBetweenCheckpoint = 0;

        FailingCheckpointMapper(SharedReference<AtomicBoolean> failed) {
            this.failed = failed;
        }

        @Override
        public Long map(Long value) throws Exception {
            if (lastCheckpointId >= 1 && emittedBetweenCheckpoint > 0 && !failed.get().get()) {
                failed.get().set(true);
                throw new RuntimeException("Planned exception.");
            }
            emittedBetweenCheckpoint++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            lastCheckpointId = checkpointId;
            emittedBetweenCheckpoint = 0;
        }
    }
}
