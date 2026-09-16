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
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Recovery of checkpointed Kafka transactions across protocol versions and producer epochs. */
@Testcontainers
class KafkaCommitterRecoveryITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitterRecoveryITCase.class);
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

    @ParameterizedTest(name = "broker={0}, naming={1}, epoch={2}")
    @MethodSource("recoveryScenarios")
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testRecoverCheckpointAfterTransactionCommit(
            String dockerImage, TransactionNamingStrategy namingStrategy, int epochBeforeCommit)
            throws Exception {
        boolean transactionV2 = dockerImage.equals(DockerImageVersions.APACHE_KAFKA);
        TestKafkaContainer kafkaContainer = transactionV2 ? KAFKA_V2_CONTAINER : KAFKA_V1_CONTAINER;
        String transactionalId = "commit-recovery-" + namingStrategy + "-" + epochBeforeCommit;
        String topic = transactionalId;
        Properties properties = getProperties(kafkaContainer.getBootstrapServers());
        KafkaSink<byte[]> sink =
                KafkaSink.<byte[]>builder()
                        .setBootstrapServers(kafkaContainer.getBootstrapServers())
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionNamingStrategy(namingStrategy)
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
            long producerId = producer.getProducerId();
            advanceEpoch(producer, transactionalId, epochBeforeCommit);
            assertThat(producer.getProducerId()).isEqualTo(producerId);
            assertThat(producer.getEpoch()).isEqualTo((short) epochBeforeCommit);

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
                // Verify the negotiated protocol behavior. Even at epoch 32766, a V1 commit
                // leaves the producer ID and epoch unchanged, so it does not exercise rollover.
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
                        softly.assertThatCode(() -> recoverCheckpoint(sink, checkpoint))
                                .as(
                                        "Recovery attempt %s: broker=%s, naming=%s, epoch=%s",
                                        attempt, dockerImage, namingStrategy, epochBeforeCommit)
                                .doesNotThrowAnyException();
                    }
                });
    }

    private static Stream<Arguments> recoveryScenarios() {
        // Naming selects PROBING or LISTING for writer cleanup. This test exercises the shared
        // committer recovery path; it does not run either writer cleanup algorithm.
        return Stream.of(DockerImageVersions.CP_KAFKA, DockerImageVersions.APACHE_KAFKA)
                .flatMap(
                        dockerImage ->
                                Arrays.stream(TransactionNamingStrategy.values())
                                        .flatMap(
                                                namingStrategy ->
                                                        IntStream.of(0, Short.MAX_VALUE - 1)
                                                                .mapToObj(
                                                                        epoch ->
                                                                                Arguments.of(
                                                                                        dockerImage,
                                                                                        namingStrategy,
                                                                                        epoch))));
    }

    private static void advanceEpoch(
            FlinkKafkaInternalProducer<?, ?> producer, String transactionalId, int targetEpoch) {
        // Advance the real coordinator state with one producer and no data transactions. Setting
        // only the client's epoch via reflection would not reproduce broker-side PID rotation.
        for (int epoch = 1; epoch <= targetEpoch; epoch++) {
            producer.setTransactionId(transactionalId);
            producer.initTransactions();
            if (epoch % 4096 == 0) {
                LOG.info("Advanced producer epoch to {} of {}", epoch, targetEpoch);
            }
        }
    }

    private static OneInputStreamOperatorTestHarness<
                    CommittableMessage<KafkaCommittable>, CommittableMessage<KafkaCommittable>>
            createHarness(KafkaSink<byte[]> sink) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, false, true));
    }

    private static void recoverCheckpoint(KafkaSink<byte[]> sink, OperatorSubtaskState checkpoint)
            throws Exception {
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<KafkaCommittable>, CommittableMessage<KafkaCommittable>>
                recovered = createHarness(sink)) {
            recovered.setup();
            recovered.setRestoredCheckpointId(CHECKPOINT_ID);
            recovered.initializeState(checkpoint);
            recovered.open();
        }
    }

    private static Properties getProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        return properties;
    }
}
