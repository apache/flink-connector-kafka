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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.advanceEpoch;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.createHarness;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.getProperties;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.recoverCheckpoint;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Recovery of checkpointed Kafka transactions across protocol versions and producer epochs. */
@Testcontainers
class KafkaCommitterRecoveryITCase {

    private static final long CHECKPOINT_ID = 1;

    @Container
    private static final TestKafkaContainer KAFKA_V1_CONTAINER =
            new TestKafkaContainer(DockerImageVersions.CP_KAFKA)
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1");

    @Container
    private static final TestKafkaContainer KAFKA_V2_CONTAINER =
            new TestKafkaContainer(DockerImageVersions.APACHE_KAFKA)
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1");

    @AfterEach
    void check() {
        checkProducerLeak();
    }

    @ParameterizedTest(name = "protocol={0}, epoch={1}")
    @MethodSource("recoveryScenarios")
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testRecoverCheckpointAfterTransactionCommit(
            TransactionProtocol expectedProtocol, int epochBeforeCommit) throws Exception {
        TestKafkaContainer kafkaContainer =
                expectedProtocol == TransactionProtocol.V2
                        ? KAFKA_V2_CONTAINER
                        : KAFKA_V1_CONTAINER;
        String transactionalId = "commit-recovery-" + epochBeforeCommit;
        String topic = transactionalId;
        Properties properties = getProperties(kafkaContainer.getBootstrapServers());
        assertBrokerProtocol(kafkaContainer.getBootstrapServers(), expectedProtocol);
        KafkaSink<byte[]> sink =
                KafkaSink.<byte[]>builder()
                        .setBootstrapServers(kafkaContainer.getBootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionNamingStrategy(TransactionNamingStrategy.INCREMENTING)
                        .setTransactionalIdPrefix(transactionalId)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<byte[]>builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(element -> element)
                                        .build())
                        .build();

        OperatorSubtaskState checkpoint;
        byte[] value = "committed-before-recovery".getBytes(StandardCharsets.UTF_8);
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
            producer.initTransactions();
            boolean transactionV2 = producer.isTransactionV2Enabled();
            assertThat(transactionV2)
                    .as("Producer negotiated the expected %s protocol", expectedProtocol)
                    .isEqualTo(expectedProtocol == TransactionProtocol.V2);
            long producerId = producer.getProducerId();
            advanceEpoch(producer, epochBeforeCommit);

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, value)).get();
            producer.flush();

            try (OneInputStreamOperatorTestHarness<
                            CommittableMessage<KafkaCommittable>,
                            CommittableMessage<KafkaCommittable>>
                    harness = createHarness(sink)) {
                harness.open();
                harness.processElement(
                        new StreamRecord<>(new CommittableSummary<>(0, 1, CHECKPOINT_ID, 1, 0)));
                harness.processElement(
                        new StreamRecord<>(
                                new CommittableWithLineage<>(
                                        KafkaCommittable.of(producer), CHECKPOINT_ID, 0)));
                checkpoint = harness.snapshot(CHECKPOINT_ID, 0);
            }

            // Kafka completes the commit, but the saved Flink checkpoint still contains the
            // pre-commit producer ID and epoch. This is the state recovered after a crash before
            // Flink durably records that the committable has been processed.
            producer.commitTransaction();
            if (transactionV2 && epochBeforeCommit == Short.MAX_VALUE - 1) {
                assertThat(producer.getProducerId()).isNotEqualTo(producerId);
                assertThat(producer.getEpoch()).isZero();
            } else {
                // V1 leaves the epoch unchanged; V2 advances it after a committed transaction.
                assertThat(producer.getProducerId()).isEqualTo(producerId);
                assertThat(producer.getEpoch())
                        .isEqualTo((short) (epochBeforeCommit + (transactionV2 ? 1 : 0)));
            }
        }

        assertThat(drainAllRecordsFromTopic(topic, properties, true))
                .singleElement()
                .satisfies(record -> assertThat(record.value()).isEqualTo(value));

        // Recommitting an already committed transaction must not prevent checkpoint recovery.
        // Reuse the saved checkpoint, as repeated failovers would do, rather than an in-memory
        // committable whose failure status might have been changed by the previous attempt.
        assertSoftly(
                softly -> {
                    for (int attempt = 1; attempt <= 3; attempt++) {
                        softly.assertThatCode(
                                        () ->
                                                recoverCheckpoint(
                                                        sink,
                                                        checkpoint,
                                                        CHECKPOINT_ID,
                                                        transactionalId,
                                                        TransactionFinished.successful(
                                                                transactionalId)))
                                .as(
                                        "Recovery attempt %s: protocol=%s, epoch=%s",
                                        attempt, expectedProtocol, epochBeforeCommit)
                                .doesNotThrowAnyException();
                    }
                });
    }

    private static Stream<Arguments> recoveryScenarios() {
        // This matrix exercises the shared committer path. Naming and producer reuse need an
        // actual writer lifecycle, which is covered by KafkaPooledTransactionRecoveryITCase.
        // Keep both protocols as required coverage even when the shared image versions change.
        // V1 does not advance the epoch on commit, so an ordinary epoch is sufficient as control.
        return Stream.of(
                Arguments.of(TransactionProtocol.V1, 0),
                Arguments.of(TransactionProtocol.V2, 0),
                Arguments.of(TransactionProtocol.V2, Short.MAX_VALUE - 1));
    }

    private static void assertBrokerProtocol(
            String bootstrapServers, TransactionProtocol expectedProtocol) throws Exception {
        try (Admin admin =
                Admin.create(
                        Collections.singletonMap(
                                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            FinalizedVersionRange transactionVersion =
                    admin.describeFeatures()
                            .featureMetadata()
                            .get(30, TimeUnit.SECONDS)
                            .finalizedFeatures()
                            .get("transaction.version");
            // Use the configured feature level, not the image name or the broker's maximum
            // supported level. A future V3 fixture must not silently replace V2 coverage.
            if (expectedProtocol == TransactionProtocol.V2) {
                assertThat(transactionVersion)
                        .as(
                                "V2 broker fixture must have transaction.version=2; check image/config changes")
                        .isEqualTo(new FinalizedVersionRange((short) 2, (short) 2));
            } else {
                // Older brokers do not advertise this feature, and levels below 2 use V1.
                assertThat(transactionVersion == null ? 0 : transactionVersion.maxVersionLevel())
                        .as(
                                "V1 broker fixture must not enable V2 or later; check image/config changes")
                        .isLessThan((short) 2);
            }
        }
    }

    private enum TransactionProtocol {
        V1,
        V2
    }
}
