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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.connector.kafka.util.AdminUtils;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.IOUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises transactional id reuse across checkpoint recovery with a real sink pipeline.
 *
 * <p>C1 completes and commits its transaction. C2's asynchronous snapshot is blocked rather than
 * failed so processing can continue through C3, which precommits a transaction reusing C1's
 * transactional id, while C1 remains the latest completed checkpoint. The job is then cancelled and
 * restored from C1 with the same transactional id prefix and operator identities.
 *
 * <p>This exercises the recovery fencing scenario handled by {@link KafkaCommitter#commit}. The
 * restored job must replay the uncheckpointed records and successfully commit new transactions
 * without losing or duplicating records. The first checkpoint after restoration must make the
 * replayed record visible to {@code read_committed} consumers; an abandoned transaction under the
 * reused transactional id must not block that output.
 */
@Testcontainers
class KafkaSinkRecoveryITCase {
    private static final long TIMEOUT_SECONDS = 60;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @Container
    static final TestKafkaContainer KAFKA =
            KafkaUtil.createKafkaContainer(KafkaSinkRecoveryITCase.class);

    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private final List<AutoCloseable> cleanupActions = new ArrayList<>();

    @AfterEach
    void tearDown() throws Throwable {
        cleanupActions.add(KafkaUtil::checkProducerLeak);
        IOUtils.closeAll(cleanupActions, Throwable.class);
    }

    @Test
    void recoversPooledTransactionsWithoutLosingNewRecords(
            @TempDir Path checkpointDirectory, @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        final String topic = "pooling-recovery-" + UUID.randomUUID();
        final String transactionalIdPrefix = "pooling-" + UUID.randomUUID();
        final Properties kafkaProperties = kafkaProperties();
        KafkaUtil.createNewTopicAndWaitForPartitionAssignment(topic, 1, 1, kafkaProperties);
        cleanupActions.add(
                () -> {
                    try (AdminClient admin = AdminClient.create(kafkaProperties)) {
                        admin.deleteTopics(Collections.singleton(topic))
                                .all()
                                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                });

        final SharedReference<PipelineControls> originalControls =
                sharedObjects.add(PipelineControls.blockingSnapshotsAfterFirstCheckpoint());
        final SharedReference<PipelineControls> recoveredControls =
                sharedObjects.add(PipelineControls.withoutSnapshotBlocking());
        final JobClient original =
                startJob(topic, transactionalIdPrefix, checkpointDirectory, originalControls, null);
        emit(originalControls, 0L);
        final String checkpointOnePath =
                miniCluster
                        .triggerCheckpoint(original.getJobID())
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final long checkpointOneId =
                awaitCheckpointId(originalControls.get().snapshottedCheckpoints);
        awaitCommittedRecords(topic, List.of(0L));

        // C1's completed transaction is returned to the pool at C2.
        emit(originalControls, 1L);
        final CompletableFuture<String> checkpointTwo =
                miniCluster.triggerCheckpoint(original.getJobID());
        final long checkpointTwoId = awaitCheckpointId(originalControls.get().blockedCheckpoints);
        assertThat(checkpointTwoId).isGreaterThan(checkpointOneId);
        assertThat(awaitCheckpointId(originalControls.get().snapshottedCheckpoints))
                .isEqualTo(checkpointTwoId);
        assertThat(checkpointTwo).isNotDone();

        // C3 precommits the transaction reusing C1's transactional id.
        emit(originalControls, 2L);
        final CompletableFuture<String> checkpointThree =
                miniCluster.triggerCheckpoint(original.getJobID());
        final long checkpointThreeId =
                awaitCheckpointId(originalControls.get().snapshottedCheckpoints);
        // C2 can occupy the only async snapshot worker. The next record proves that C3
        // finished its entire synchronous phase, including the sink precommit.
        emit(originalControls, 3L);
        assertThat(checkpointThreeId).isGreaterThan(checkpointTwoId);
        assertThat(checkpointTwo).isNotDone();
        assertThat(checkpointThree).isNotDone();

        try (AdminClient admin = AdminClient.create(kafkaProperties)) {
            assertThat(AdminUtils.getOpenTransactionsForTopics(admin, Collections.singleton(topic)))
                    .extracting(TransactionListing::transactionalId)
                    .as("C3 reused C1's transactional id before recovery")
                    .contains(
                            TransactionalIdFactory.buildTransactionalId(
                                    transactionalIdPrefix, 0, 0));
        }

        cancelAndWait(original, originalControls.get());
        assertThat(checkpointTwo).failsWithin(Duration.ofSeconds(TIMEOUT_SECONDS));
        assertThat(checkpointThree).failsWithin(Duration.ofSeconds(TIMEOUT_SECONDS));

        // Restore exactly C1, preserving both the sink prefix and operator identities.
        final JobClient recovered =
                startJob(
                        topic,
                        transactionalIdPrefix,
                        checkpointDirectory,
                        recoveredControls,
                        checkpointOnePath);
        emit(recoveredControls, 1L);
        miniCluster.triggerCheckpoint(recovered.getJobID()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        awaitCommittedRecords(topic, List.of(0L, 1L));

        emit(recoveredControls, 2L);
        emit(recoveredControls, 3L);
        miniCluster.triggerCheckpoint(recovered.getJobID()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        awaitCommittedRecords(topic, List.of(0L, 1L, 2L, 3L));
        cancelAndWait(recovered, recoveredControls.get());

        final List<Long> records =
                KafkaUtil.drainAllRecordsFromTopic(topic, kafkaProperties, true).stream()
                        .map(KafkaSinkRecoveryITCase::decodeRecord)
                        .collect(Collectors.toList());
        assertThat(records).containsExactly(0L, 1L, 2L, 3L);
    }

    private JobClient startJob(
            String topic,
            String transactionalIdPrefix,
            Path checkpointDirectory,
            SharedReference<PipelineControls> controls,
            @Nullable String restorePath)
            throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory.toUri().toString());
        configuration.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        if (restorePath != null) {
            configuration.set(SAVEPOINT_PATH, restorePath);
        }
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // Long.MAX_VALUE disables periodic scheduling while allowing manual checkpoints.
        environment.enableCheckpointing(Long.MAX_VALUE);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        final DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        value -> {
                            controls.get().emittedRecords.add(value);
                            return value;
                        },
                        Long.MAX_VALUE,
                        parallelism -> new PermitRateLimiter(controls),
                        BasicTypeInfo.LONG_TYPE_INFO);
        environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "controlled source")
                .uid("source")
                .map(new CheckpointGate(controls))
                .uid("checkpoint-gate")
                .sinkTo(
                        KafkaSink.<Long>builder()
                                .setBootstrapServers(KAFKA.getBootstrapServers())
                                .setTransactionalIdPrefix(transactionalIdPrefix)
                                .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(topic)
                                                .setValueSerializationSchema(new RecordSerializer())
                                                .build())
                                .build())
                .uid("kafka-sink");
        final JobClient job = environment.executeAsync("pooled transaction recovery");
        cleanupActions.add(0, () -> cancelAndWait(job, controls.get()));
        return job;
    }

    private static void emit(SharedReference<PipelineControls> controls, long expectedValue)
            throws InterruptedException {
        controls.get().allowRecord();
        assertThat(controls.get().emittedRecords.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("next record from the checkpointed source")
                .isEqualTo(expectedValue);
    }

    private static long awaitCheckpointId(BlockingQueue<Long> checkpoints)
            throws InterruptedException {
        final Long checkpointId = checkpoints.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(checkpointId).as("checkpoint reached the test gate").isNotNull();
        return checkpointId;
    }

    private static void cancelAndWait(JobClient job, PipelineControls controls) throws Exception {
        try {
            try {
                job.cancel().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException cancellationFailure) {
                // Cancellation may be rejected for a terminal job. Ignore that rejection so
                // cleanup does not obscure an earlier test failure.
                if (!job.getJobStatus()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .isGloballyTerminalState()) {
                    throw cancellationFailure;
                }
            }
            job.getJobExecutionResult()
                    .handle((result, failure) -> null)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            // Cancellation interrupts blocked snapshots. Release the gate here
            // in case cancellation fails.
            controls.releaseCheckpointGates.countDown();
        }
    }

    private static void awaitCommittedRecords(String topic, List<Long> expected) {
        final Properties properties = kafkaProperties();
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("isolation.level", "read_committed");
        properties.put("enable.auto.commit", false);
        final List<Long> records = new ArrayList<>();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            final List<TopicPartition> partitions =
                    Collections.singletonList(new TopicPartition(topic, 0));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (records.size() < expected.size() && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record :
                        consumer.poll(Duration.ofMillis(100))) {
                    records.add(decodeRecord(record));
                }
            }
        }
        assertThat(records)
                .as("visible committed Kafka records")
                .containsExactlyElementsOf(expected);
    }

    private static Properties kafkaProperties() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        return properties;
    }

    private static long decodeRecord(ConsumerRecord<byte[], byte[]> record) {
        return ByteBuffer.wrap(record.value()).getLong();
    }

    private static final class RecordSerializer implements SerializationSchema<Long> {
        @Override
        public byte[] serialize(Long value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        }
    }

    /**
     * Coordinates the test thread, source task, and asynchronous snapshot threads through source
     * permits, checkpoint notifications, and snapshot gates.
     */
    private static final class PipelineControls {
        private final boolean blockAfterFirstCheckpoint;
        private final AtomicLong firstCheckpoint = new AtomicLong(-1L);
        private final BlockingQueue<Long> emittedRecords = new LinkedBlockingQueue<>();
        private final BlockingQueue<Long> blockedCheckpoints = new LinkedBlockingQueue<>();
        private final BlockingQueue<Long> snapshottedCheckpoints = new LinkedBlockingQueue<>();
        private final CountDownLatch releaseCheckpointGates = new CountDownLatch(1);
        private int permits;
        @Nullable private CompletableFuture<Void> pendingPermit;

        private PipelineControls(boolean blockAfterFirstCheckpoint) {
            this.blockAfterFirstCheckpoint = blockAfterFirstCheckpoint;
        }

        private static PipelineControls blockingSnapshotsAfterFirstCheckpoint() {
            return new PipelineControls(true);
        }

        private static PipelineControls withoutSnapshotBlocking() {
            return new PipelineControls(false);
        }

        private synchronized CompletionStage<Void> acquire() {
            if (permits > 0) {
                permits--;
                return CompletableFuture.completedFuture(null);
            }
            pendingPermit = new CompletableFuture<>();
            return pendingPermit;
        }

        private void allowRecord() {
            final CompletableFuture<Void> permit;
            synchronized (this) {
                if (pendingPermit == null) {
                    permits++;
                    return;
                }
                permit = pendingPermit;
                pendingPermit = null;
            }
            permit.complete(null);
        }
    }

    private static final class PermitRateLimiter implements RateLimiter<NumberSequenceSplit> {
        private final SharedReference<PipelineControls> controls;

        private PermitRateLimiter(SharedReference<PipelineControls> controls) {
            this.controls = controls;
        }

        @Override
        public CompletionStage<Void> acquire(int numberOfEvents) {
            return controls.get().acquire();
        }
    }

    private static final class CheckpointGate
            implements MapFunction<Long, Long>, CheckpointedFunction {
        private final SharedReference<PipelineControls> controls;
        private transient ListState<Long> state;

        private CheckpointGate(SharedReference<PipelineControls> controls) {
            this.controls = controls;
        }

        @Override
        public Long map(Long value) {
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            controls.get().firstCheckpoint.compareAndSet(-1L, context.getCheckpointId());
            // Persist a value so GateSerializer can block the asynchronous snapshot.
            state.update(Collections.singletonList(context.getCheckpointId()));
            controls.get().snapshottedCheckpoints.add(context.getCheckpointId());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "checkpoint-id", new GateSerializer(controls)));
        }
    }

    private static final class GateSerializer extends TypeSerializerSingleton<Long> {
        @Nullable private final SharedReference<PipelineControls> controls;

        private GateSerializer(@Nullable SharedReference<PipelineControls> controls) {
            this.controls = controls;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public Long createInstance() {
            return 0L;
        }

        @Override
        public Long copy(Long value) {
            return value;
        }

        @Override
        public Long copy(Long value, Long reuse) {
            return value;
        }

        @Override
        public int getLength() {
            return Long.BYTES;
        }

        @Override
        public void serialize(Long checkpointId, DataOutputView output) throws IOException {
            if (controls != null
                    && controls.get().blockAfterFirstCheckpoint
                    && checkpointId != controls.get().firstCheckpoint.get()) {
                controls.get().blockedCheckpoints.add(checkpointId);
                try {
                    // Block until cancellation interrupts this thread or teardown releases
                    // the gate. Record and checkpoint waits are bounded by the test.
                    controls.get().releaseCheckpointGates.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Checkpoint cancelled by test", e);
                }
            }
            output.writeLong(checkpointId);
        }

        @Override
        public Long deserialize(DataInputView input) throws IOException {
            return input.readLong();
        }

        @Override
        public Long deserialize(Long reuse, DataInputView input) throws IOException {
            return deserialize(input);
        }

        @Override
        public void copy(DataInputView input, DataOutputView output) throws IOException {
            output.writeLong(input.readLong());
        }

        @Override
        public TypeSerializerSnapshot<Long> snapshotConfiguration() {
            return new GateSerializerSnapshot();
        }
    }

    /** The gate is test coordination only; the checkpoint's persisted value is a plain long. */
    public static final class GateSerializerSnapshot extends SimpleTypeSerializerSnapshot<Long> {
        public GateSerializerSnapshot() {
            super(() -> new GateSerializer(null));
        }
    }
}
