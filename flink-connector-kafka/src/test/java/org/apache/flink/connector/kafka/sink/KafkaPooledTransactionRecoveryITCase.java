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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.advanceEpoch;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.createHarness;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.getProperties;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.recoverCheckpoint;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createNewTopicAndWaitForPartitionAssignment;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Recovery after the pooling writer reuses a completed transaction's identity. */
@Testcontainers
class KafkaPooledTransactionRecoveryITCase {

    private static final long CHECKPOINT_ID = 1;

    @Container
    private static final TestKafkaContainer KAFKA =
            new TestKafkaContainer(DockerImageVersions.APACHE_KAFKA)
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1");

    @AfterEach
    void check() {
        checkProducerLeak();
    }

    @ParameterizedTest(name = "epoch={0}")
    @ValueSource(ints = {0, Short.MAX_VALUE - 1})
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testRecoverCheckpointAfterWriterReusesTransactionalId(int epochBeforeCommit)
            throws Exception {
        String prefix = "pooled-recovery-" + epochBeforeCommit;
        String topic = prefix;
        Properties properties = getProperties(KAFKA.getBootstrapServers());
        createNewTopicAndWaitForPartitionAssignment(topic, 1, (short) 1, properties);
        KafkaSink<byte[]> sink =
                KafkaSink.<byte[]>builder()
                        .setBootstrapServers(KAFKA.getBootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                        .setTransactionalIdPrefix(prefix)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<byte[]>builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(element -> element)
                                        .build())
                        .build();

        OperatorSubtaskState checkpoint;
        String transactionalId;
        byte[] committedValue = "committed-before-reuse".getBytes(StandardCharsets.UTF_8);
        try (ExactlyOnceKafkaWriter<byte[]> writer = createWriter(sink);
                OneInputStreamOperatorTestHarness<
                                CommittableMessage<KafkaCommittable>,
                                CommittableMessage<KafkaCommittable>>
                        harness = createHarness(sink)) {
            harness.open();
            FlinkKafkaInternalProducer<byte[], byte[]> originalProducer =
                    writer.getCurrentProducer();
            transactionalId = originalProducer.getTransactionalId();
            long producerId = originalProducer.getProducerId();
            // The writer starts an empty transaction. End only that local transaction before
            // advancing the actual coordinator state, then resume the writer's transaction.
            originalProducer.abortTransaction();
            advanceEpoch(originalProducer, epochBeforeCommit);
            originalProducer.beginTransaction();

            writer.write(committedValue, new KafkaWriterTestBase.DummySinkWriterContext());
            writer.flush(false);
            KafkaCommittable committable = getOnlyCommittable(writer.prepareCommit());
            writer.snapshotState(CHECKPOINT_ID);
            harness.processElement(
                    new StreamRecord<>(new CommittableSummary<>(0, 1, CHECKPOINT_ID, 1, 0)));
            harness.processElement(
                    new StreamRecord<>(
                            new CommittableWithLineage<>(committable, CHECKPOINT_ID, 0)));
            checkpoint = harness.snapshot(CHECKPOINT_ID, 0);
            harness.notifyOfCompletedCheckpoint(CHECKPOINT_ID);

            if (epochBeforeCommit == Short.MAX_VALUE - 1) {
                assertThat(originalProducer.getProducerId()).isNotEqualTo(producerId);
                assertThat(originalProducer.getEpoch()).isZero();
            } else {
                assertThat(originalProducer.getProducerId()).isEqualTo(producerId);
                assertThat(originalProducer.getEpoch()).isEqualTo((short) 1);
            }
            long producerIdAfterCommit = originalProducer.getProducerId();
            short epochAfterCommit = originalProducer.getEpoch();

            // Keep the next producer occupied so the pool must reuse the producer acknowledged
            // through the committer's backchannel. Snapshot 2 starts another transaction with
            // the original transactional ID and calls initTransactions through ProducerPoolImpl.
            writer.write(
                    "uncompleted-checkpoint".getBytes(StandardCharsets.UTF_8),
                    new KafkaWriterTestBase.DummySinkWriterContext());
            writer.flush(false);
            assertThat(writer.prepareCommit()).hasSize(1);
            writer.snapshotState(CHECKPOINT_ID + 1);
            assertThat(writer.getCurrentProducer()).isSameAs(originalProducer);
            assertThat(writer.getCurrentProducer().getTransactionalId()).isEqualTo(transactionalId);
            assertThat(writer.getCurrentProducer().getProducerId())
                    .isEqualTo(producerIdAfterCommit);
            assertThat(writer.getCurrentProducer().getEpoch())
                    .isEqualTo((short) (epochAfterCommit + 1));

            // Fail before checkpoint 2 completes. Only checkpoint 1, which still contains the
            // old producer identity, is available for recovery.
            // Closing leaves checkpoint 2's precommitted transaction pending at the broker
            // until timeout; the current producer has only begun an empty local transaction.
            // The read_committed drain below stops at the last stable offset, so it does not
            // need to wait for the pending transaction to time out.
        }

        assertSoftly(
                softly -> {
                    for (int attempt = 1; attempt <= 3; attempt++) {
                        softly.assertThatCode(
                                        () ->
                                                recoverCheckpoint(
                                                        sink,
                                                        checkpoint,
                                                        CHECKPOINT_ID,
                                                        prefix,
                                                        TransactionFinished.erroneously(
                                                                transactionalId)))
                                .as(
                                        "Recovery attempt %s after reusing epoch %s",
                                        attempt, epochBeforeCommit)
                                .doesNotThrowAnyException();
                    }
                });
        assertThat(drainAllRecordsFromTopic(topic, properties, true))
                .singleElement()
                .satisfies(record -> assertThat(record.value()).isEqualTo(committedValue));
    }

    private static KafkaCommittable getOnlyCommittable(Collection<KafkaCommittable> committables) {
        assertThat(committables).hasSize(1);
        return committables.iterator().next();
    }

    private static ExactlyOnceKafkaWriter<byte[]> createWriter(KafkaSink<byte[]> sink)
            throws Exception {
        return (ExactlyOnceKafkaWriter<byte[]>)
                sink.createWriter(
                        new KafkaWriterTestBase.SinkInitContext(
                                InternalSinkWriterMetricGroup.wrap(
                                        new KafkaWriterTestBase.DummyOperatorMetricGroup(
                                                new MetricListener().getMetricGroup())),
                                new KafkaWriterTestBase.TriggerTimeService(),
                                null));
    }
}
