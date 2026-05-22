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

package org.apache.flink.connector.kafka.tool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaWriterState;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class KafkaTransactionManagerITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransactionManagerITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";

    @Nested
    class AbortAndCommitTests {

        @Container
        final TestKafkaContainer kafkaContainer =
                createKafkaContainer(KafkaTransactionManagerITCase.class)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

        @Test
        void testAbortSpecificTransactionOnly() {
            // given
            final String topic = "test-abort-isolation";
            final String transactionIdPrefix = "testAbortTarget-";

            // Two ongoing transactions
            // Target - the one we want to abort
            final String targetTxId = transactionIdPrefix + "target";
            final String targetData = "data-to-abort";
            createLingeringTransaction(topic, targetTxId, targetData);

            // Neighbor - must remain unaffected
            final String neighborTxId = transactionIdPrefix + "neighbor";
            final String neighborData = "data-keep-alive";
            createLingeringTransaction(topic, neighborTxId, neighborData);

            // Verify Initial State
            try (KafkaAdminAssert adminAssert = KafkaAdminAssert.assertThat(kafkaContainer)) {
                // Verify no other transactions were created
                adminAssert
                        .transactions()
                        .extractingIds()
                        .containsExactlyInAnyOrder(targetTxId, neighborTxId);

                adminAssert
                        .transactions()
                        .extractingStates()
                        .containsEntry(targetTxId, TransactionState.ONGOING)
                        .containsEntry(neighborTxId, TransactionState.ONGOING);
            }
            assertThat(consumeRecords(topic, "read_committed")).isEmpty();

            // when
            KafkaTransactionManager tool = new KafkaTransactionManager();
            tool.abortTransaction(kafkaContainer.getBootstrapServers(), targetTxId);

            // then
            try (KafkaAdminAssert adminAssert = KafkaAdminAssert.assertThat(kafkaContainer)) {
                // COMPLETE_ABORT is transient and may transition to EMPTY.
                adminAssert
                        .transactions()
                        .extractingStates()
                        .extractingByKey(targetTxId)
                        .isIn(TransactionState.COMPLETE_ABORT, TransactionState.EMPTY);

                adminAssert
                        .transactions()
                        .extractingStates()
                        .containsEntry(neighborTxId, TransactionState.ONGOING);

                // Verify we didn't create any unrelated transactions during abort
                adminAssert
                        .transactions()
                        .extractingIds()
                        .containsExactlyInAnyOrder(targetTxId, neighborTxId);
            }

            // Committed Consumer should see NOTHING (Target aborted, Neighbor still open)
            assertThat(consumeRecords(topic, "read_committed")).isEmpty();

            // Uncommitted Consumer should see everything (aborted messages exist in log, just
            // marked aborted)
            assertThat(consumeRecords(topic, "read_uncommitted"))
                    .contains(targetData, neighborData);
        }

        @Test
        void testCommitSpecificTransactionOnly() {
            // given
            final String topic = "test-commit-isolation";
            final String transactionIdPrefix = "testCommitTarget-";

            // Two ongoing transactions on the SAME topic
            // Target - the one we want to commit
            final String targetTxId = transactionIdPrefix + "target";
            final String targetData = "data-target";
            final long[] targetMetadata = createLingeringTransaction(topic, targetTxId, targetData);
            final long targetProducerId = targetMetadata[0];
            final short targetEpoch = (short) targetMetadata[1];

            // Neighbor - must remain unaffected
            final String neighborTxId = transactionIdPrefix + "neighbor";
            final String neighborData = "data-neighbor";
            createLingeringTransaction(topic, neighborTxId, neighborData);

            // Verify Initial State
            try (KafkaAdminAssert adminAssert = KafkaAdminAssert.assertThat(kafkaContainer)) {
                // Verify no other transactions were created
                adminAssert
                        .transactions()
                        .extractingIds()
                        .containsExactlyInAnyOrder(targetTxId, neighborTxId);

                adminAssert
                        .transactions()
                        .extractingStates()
                        .containsEntry(targetTxId, TransactionState.ONGOING)
                        .containsEntry(neighborTxId, TransactionState.ONGOING);
            }
            assertThat(consumeRecords(topic, "read_committed")).isEmpty();
            assertThat(consumeRecords(topic, "read_uncommitted"))
                    .contains(targetData, neighborData);

            // when
            KafkaTransactionManager kafkaTransactionManager = new KafkaTransactionManager();
            kafkaTransactionManager.commitTransaction(
                    kafkaContainer.getBootstrapServers(),
                    targetTxId,
                    targetProducerId,
                    targetEpoch);

            // then
            try (KafkaAdminAssert adminAssert = KafkaAdminAssert.assertThat(kafkaContainer)) {
                adminAssert
                        .transactions()
                        .extractingStates()
                        .extractingByKey(targetTxId)
                        .isIn(TransactionState.PREPARE_COMMIT, TransactionState.COMPLETE_COMMIT);

                adminAssert
                        .transactions()
                        .extractingStates()
                        .containsEntry(neighborTxId, TransactionState.ONGOING);

                // Verify no other transactions were created
                adminAssert
                        .transactions()
                        .extractingIds()
                        .containsExactlyInAnyOrder(targetTxId, neighborTxId);
            }

            // Target Data should now be visible in read_committed
            assertThat(consumeRecords(topic, "read_committed")).contains(targetData);

            // Neighbor Data should STILL be invisible in read_committed
            assertThat(consumeRecords(topic, "read_committed")).doesNotContain(neighborData);
        }

        @Test
        void testCommitWithWrongProducerIdFails() {
            // given
            final String topic = "test-commit-wrong-pid";
            final String transactionalId = "testWrongPid-target";
            final long invalidProducerId = 999999L;
            createLingeringTransaction(topic, transactionalId, "data");

            KafkaTransactionManager manager = new KafkaTransactionManager();

            // when/then
            assertThatThrownBy(
                            () ->
                                    manager.commitTransaction(
                                            kafkaContainer.getBootstrapServers(),
                                            transactionalId,
                                            invalidProducerId,
                                            (short) 0))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to commit transaction");
        }

        private long[] createLingeringTransaction(
                String topic, String transactionalId, String data) {
            final Properties props = new Properties();
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // We rely on FlinkKafkaInternalProducer's specific close behavior (Duration.ZERO)
            // to leave the transaction hanging on the broker without sending an abort signal.
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(props, transactionalId)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, data));
                producer.flush();

                long pid = producer.getProducerId();
                short epoch = producer.getEpoch();
                return new long[] {pid, epoch};
            }
        }

        private List<String> consumeRecords(String topic, String isolationLevel) {
            Properties props = new Properties();
            props.put(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            boolean committed = "read_committed".equals(isolationLevel);
            return KafkaUtil.drainAllRecordsFromTopic(topic, props, committed).stream()
                    .map(r -> new String(r.value()))
                    .collect(Collectors.toList());
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class SavepointRecoveryTests {

        @Container
        private final TestKafkaContainer kafkaContainer =
                createKafkaContainer(KafkaTransactionManagerITCase.class)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

        @RegisterExtension
        final MiniClusterExtension miniClusterExtension =
                new MiniClusterExtension(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .setConfiguration(new Configuration())
                                .build());

        @RegisterExtension
        final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

        @TempDir Path tempDir;

        @Test
        void testCommitLingeringTransactionAfterSavepointWithoutSinkCommit(
                @InjectMiniCluster MiniCluster miniCluster,
                @InjectClusterClient ClusterClient<?> clusterClient)
                throws Exception {
            // given
            final String topic = UUID.randomUUID().toString();
            createTestTopic(topic);

            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration(1));
            env.enableCheckpointing(
                    1_000_000); // some high value only to allow savepoint triggering

            final DataStream<Long> source = createSource(env, 100);
            SharedReference<Set<Long>> checkpointedRecords =
                    sharedObjects.add(new ConcurrentSkipListSet<>());

            final String kafkaSinkUid = "kafka-sink-operator-uid";
            final String transactionalIdPrefix = "kafka-sink";

            final KafkaSink<Long> kafkaSink =
                    KafkaSink.<Long>builder()
                            .setBootstrapServers(kafkaContainer.getBootstrapServers())
                            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .setRecordSerializer(
                                    KafkaRecordSerializationSchema.builder()
                                            .setTopic(topic)
                                            .setValueSerializationSchema(new RecordSerializer())
                                            .build())
                            .setTransactionalIdPrefix(transactionalIdPrefix)
                            .build();

            source.map(new RecordFetcher(checkpointedRecords))
                    .sinkTo(NoCommitKafkaSinkWrapper.wrap(kafkaSink))
                    .uid(kafkaSinkUid);

            StreamGraph streamGraph = env.getStreamGraph();
            JobGraph jobGraph = streamGraph.getJobGraph();

            JobID jobId = clusterClient.submitJob(jobGraph).get();
            CommonTestUtils.waitForAllTaskRunning(miniCluster, jobId, true);

            Path flinkInternalSavepointTargetDirRestored =
                    tempDir.resolve("native-savepoint-restored");
            Files.createDirectories(flinkInternalSavepointTargetDirRestored);
            CompletableFuture<String> savepointFutureRestored =
                    clusterClient.stopWithSavepoint(
                            jobGraph.getJobID(),
                            false,
                            flinkInternalSavepointTargetDirRestored.toAbsolutePath().toString(),
                            SavepointFormatType.NATIVE);
            // Wait for savepoint completion
            String savepointPathStringRestored = savepointFutureRestored.get();

            List<TransactionListing> transactions =
                    listTransactionsForPrefix(transactionalIdPrefix);

            // Find the ONGOING transaction (the transactional ID format is
            // prefix-subtask-offset)
            List<TransactionListing> ongoingTransactions =
                    transactions.stream()
                            .filter(t -> t.state() == TransactionState.ONGOING)
                            .collect(Collectors.toList());
            assertThat(ongoingTransactions).hasSize(1);
            String transactionalId = ongoingTransactions.get(0).transactionalId();

            List<Long> collectedRecords = deserializeValues(drainAllRecordsFromTopic(topic, true));
            // NoCommitKafkaSink should leave all transactions not committed hence collected
            // records should be empty
            assertThat(collectedRecords).isEmpty();

            // when
            TransactionDescription description = describeTransaction(transactionalId);
            KafkaTransactionManager kafkaTransactionManager = new KafkaTransactionManager();
            kafkaTransactionManager.commitTransaction(
                    kafkaContainer.getBootstrapServers(),
                    transactionalId,
                    description.producerId(),
                    (short) description.producerEpoch());

            // then
            // after committing transactions via the cleanup process, we should finally see the
            // expected records
            collectedRecords = deserializeValues(drainAllRecordsFromTopic(topic, true));
            assertThat(collectedRecords)
                    .containsExactlyInAnyOrderElementsOf(checkpointedRecords.get());
            assertThat(describeTransaction(transactionalId).state())
                    .isEqualTo(TransactionState.COMPLETE_COMMIT);
        }

        private void createTestTopic(String topic) throws Exception {
            Properties adminProps = new Properties();
            adminProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
            try (Admin admin = Admin.create(adminProps)) {
                admin.createTopics(
                                Collections.singletonList(
                                        new org.apache.kafka.clients.admin.NewTopic(
                                                topic, 1, (short) 1)))
                        .all()
                        .get();
            }
        }

        private TransactionDescription describeTransaction(String transactionalId)
                throws Exception {
            Properties props = new Properties();
            props.put(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());

            try (Admin admin = Admin.create(props)) {
                TransactionDescription description =
                        admin.describeTransactions(List.of(transactionalId))
                                .all()
                                .get()
                                .get(transactionalId);

                LOG.info("Transaction {} state is: {}", transactionalId, description.state());
                return description;
            }
        }

        private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
                String topic, boolean committed) {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
            properties.put("group.id", UUID.randomUUID().toString());
            properties.put("enable.auto.commit", false);
            properties.put("auto.offset.reset", "earliest");
            return KafkaUtil.drainAllRecordsFromTopic(topic, properties, committed);
        }

        private List<TransactionListing> listTransactionsForPrefix(String prefix) throws Exception {
            Properties adminProps = new Properties();
            adminProps.put("bootstrap.servers", kafkaContainer.getBootstrapServers());

            try (Admin admin = Admin.create(adminProps)) {
                KafkaFuture<Collection<TransactionListing>> transactionsFuture =
                        admin.listTransactions(new ListTransactionsOptions()).all();
                Collection<TransactionListing> transactions = transactionsFuture.get();
                return transactions.stream()
                        .filter(t -> t.transactionalId().contains(prefix))
                        .collect(Collectors.toList());
            }
        }

        private Configuration createConfiguration(int parallelism) {
            final Configuration config = new Configuration();
            config.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
            return config;
        }

        private DataStream<Long> createSource(StreamExecutionEnvironment env, int count) {
            return env.fromSource(
                    new DataGeneratorSource<>(
                            value -> value,
                            count,
                            RateLimiterStrategy.noOp(),
                            BasicTypeInfo.LONG_TYPE_INFO),
                    WatermarkStrategy.noWatermarks(),
                    "Generator Source");
        }

        private List<Long> deserializeValues(List<ConsumerRecord<byte[], byte[]>> records) {
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
     * we may end up with duplicate records being added to the {@link #checkpointedRecords}.
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

    /**
     * A Facade (Wrapper) around KafkaSink that intercepts the Committer to prevent actual commits.
     */
    private static class NoCommitKafkaSinkWrapper<IN>
            implements TwoPhaseCommittingStatefulSink<IN, KafkaWriterState, KafkaCommittable> {

        private final KafkaSink<IN> kafkaSink;

        private NoCommitKafkaSinkWrapper(KafkaSink<IN> kafkaSink) {
            this.kafkaSink = kafkaSink;
        }

        static <IN> NoCommitKafkaSinkWrapper<IN> wrap(KafkaSink<IN> sink) {
            return new NoCommitKafkaSinkWrapper<>(sink);
        }

        @Override
        public Committer<KafkaCommittable> createCommitter(CommitterInitContext context) {
            return new NoCommitCommitter(kafkaSink.createCommitter(context));
        }

        @Override
        public SimpleVersionedSerializer<KafkaCommittable> getCommittableSerializer() {
            return kafkaSink.getCommittableSerializer();
        }

        @Override
        public PrecommittingStatefulSinkWriter<IN, KafkaWriterState, KafkaCommittable> createWriter(
                WriterInitContext context) throws IOException {
            return kafkaSink.createWriter(context);
        }

        @Override
        public PrecommittingStatefulSinkWriter<IN, KafkaWriterState, KafkaCommittable>
                restoreWriter(
                        WriterInitContext context, Collection<KafkaWriterState> recoveredState)
                        throws IOException {
            return kafkaSink.restoreWriter(context, recoveredState);
        }

        @Override
        public SimpleVersionedSerializer<KafkaWriterState> getWriterStateSerializer() {
            return kafkaSink.getWriterStateSerializer();
        }

        /** Inner Committer that delegates everything except the commit() method. */
        private static class NoCommitCommitter implements Committer<KafkaCommittable> {
            private final Committer<KafkaCommittable> committerDelegate;

            NoCommitCommitter(Committer<KafkaCommittable> committerDelegate) {
                this.committerDelegate = committerDelegate;
            }

            @Override
            public void commit(Collection<CommitRequest<KafkaCommittable>> collection)
                    throws IOException, InterruptedException {
                // noop - ignore and do not commit
            }

            @Override
            public void close() throws Exception {
                committerDelegate.close();
            }
        }
    }
}
