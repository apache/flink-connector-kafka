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

import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies transaction retries and terminal outcomes with both Kafka transaction protocols. */
@Testcontainers
class KafkaTransactionManagerIdempotenceITCase {

    @Container
    private static final TestKafkaContainer KAFKA_V1 =
            KafkaUtil.createKafkaContainer(KafkaTransactionManagerIdempotenceITCase.class)
                    .withEnv(
                            "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
                            "100");

    @Container
    private static final TestKafkaContainer KAFKA_V2 =
            new TestKafkaContainer(DockerImageVersions.APACHE_KAFKA)
                    .withEnv(
                            "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
                            "100");

    @BeforeAll
    static void verifyTransactionV2IsEnabled() throws Exception {
        try (Admin admin = createAdmin(KAFKA_V2)) {
            assertThat(admin.describeFeatures().featureMetadata().get().finalizedFeatures())
                    .containsKey("transaction.version");
            assertThat(
                            admin.describeFeatures()
                                    .featureMetadata()
                                    .get()
                                    .finalizedFeatures()
                                    .get("transaction.version")
                                    .maxVersionLevel())
                    .isGreaterThanOrEqualTo((short) 2);
        }
    }

    static Stream<TestKafkaContainer> brokers() {
        return Stream.of(KAFKA_V1, KAFKA_V2);
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("brokers")
    void testAbortAfterBrokerTimeout(TestKafkaContainer kafka) throws Exception {
        final String topic = createTopic(kafka);
        final String transactionalId = UUID.randomUUID().toString();
        final Properties properties = producerProperties(kafka);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "2000");
        createLingeringTransaction(properties, topic, transactionalId, "expired");
        sendSentinel(kafka, topic);

        try (Admin admin = createAdmin(kafka)) {
            waitForState(admin, transactionalId, TransactionState.COMPLETE_ABORT);

            new KafkaTransactionManager()
                    .abortTransaction(kafka.getBootstrapServers(), transactionalId);

            assertThat(describe(admin, transactionalId).state())
                    .isIn(TransactionState.EMPTY, TransactionState.COMPLETE_ABORT);
            assertThat(consumeCommitted(kafka, topic)).containsExactly("sentinel");
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("brokers")
    void testCommitAfterNativeCommitWithOriginalMetadata(TestKafkaContainer kafka)
            throws Exception {
        final String topic = createTopic(kafka);
        final String transactionalId = UUID.randomUUID().toString();
        try (Admin admin = createAdmin(kafka)) {
            final TransactionDescription original;
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(producerProperties(kafka), transactionalId)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "committed"));
                producer.flush();
                original = describe(admin, transactionalId);
                assertThat(original.state()).isEqualTo(TransactionState.ONGOING);
                producer.commitTransaction();
            }
            waitForState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);
            final TransactionDescription committed = describe(admin, transactionalId);
            assertThat(committed.producerId()).isEqualTo(original.producerId());
            assertThat(committed.producerEpoch())
                    .isEqualTo(original.producerEpoch() + (kafka == KAFKA_V2 ? 1 : 0));

            new KafkaTransactionManager()
                    .commitTransaction(
                            kafka.getBootstrapServers(),
                            transactionalId,
                            original.producerId(),
                            (short) original.producerEpoch(),
                            kafka == KAFKA_V2);

            if (kafka == KAFKA_V2) {
                assertThat(
                                KafkaTransactionTool.run(
                                        new String[] {
                                            "--action",
                                            "commit",
                                            "--bootstrap-servers",
                                            kafka.getBootstrapServers(),
                                            "--transactional-id",
                                            transactionalId,
                                            "--producer-id",
                                            Long.toString(original.producerId()),
                                            "--epoch",
                                            Integer.toString(original.producerEpoch()),
                                            "--transaction-protocol",
                                            "v2"
                                        }))
                        .isZero();
            }
            assertUnchanged(admin, transactionalId, committed);
            assertThat(consumeCommitted(kafka, topic)).containsExactly("committed");
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("brokers")
    void testCommitAfterAbortFailsWithOriginalMetadata(TestKafkaContainer kafka) throws Exception {
        final String topic = createTopic(kafka);
        final String transactionalId = UUID.randomUUID().toString();
        final TransactionDescription original =
                createLingeringTransaction(
                        producerProperties(kafka), topic, transactionalId, "aborted");
        final KafkaTransactionManager manager = new KafkaTransactionManager();
        manager.abortTransaction(kafka.getBootstrapServers(), transactionalId);
        sendSentinel(kafka, topic);

        try (Admin admin = createAdmin(kafka)) {
            final TransactionDescription aborted = describe(admin, transactionalId);
            assertThat(aborted.state())
                    .isIn(TransactionState.EMPTY, TransactionState.COMPLETE_ABORT);

            assertThatThrownBy(
                            () ->
                                    manager.commitTransaction(
                                            kafka.getBootstrapServers(),
                                            transactionalId,
                                            original.producerId(),
                                            (short) original.producerEpoch(),
                                            kafka == KAFKA_V2))
                    .hasMessageContaining("Failed to commit transaction");

            assertUnchanged(admin, transactionalId, aborted);
            assertThat(consumeCommitted(kafka, topic)).containsExactly("sentinel");
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("brokers")
    void testWrongProducerIdDoesNotChangeOngoingTransaction(TestKafkaContainer kafka)
            throws Exception {
        final String topic = createTopic(kafka);
        final String transactionalId = UUID.randomUUID().toString();
        final TransactionDescription original =
                createLingeringTransaction(
                        producerProperties(kafka), topic, transactionalId, "pending");
        final KafkaTransactionManager manager = new KafkaTransactionManager();
        try (Admin admin = createAdmin(kafka)) {
            assertThatThrownBy(
                            () ->
                                    manager.commitTransaction(
                                            kafka.getBootstrapServers(),
                                            transactionalId,
                                            original.producerId() + 1,
                                            (short) original.producerEpoch(),
                                            kafka == KAFKA_V2))
                    .hasMessageContaining("Failed to commit transaction");
            assertUnchanged(admin, transactionalId, original);
            assertThat(consumeCommitted(kafka, topic)).isEmpty();

            manager.commitTransaction(
                    kafka.getBootstrapServers(),
                    transactionalId,
                    original.producerId(),
                    (short) original.producerEpoch(),
                    kafka == KAFKA_V2);

            waitForState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);
            assertThat(consumeCommitted(kafka, topic)).containsExactly("pending");
        }
    }

    @ParameterizedTest(autoCloseArguments = false)
    @MethodSource("brokers")
    void testStaleCommitDoesNotChangeNewTransaction(TestKafkaContainer kafka) throws Exception {
        final String topic = createTopic(kafka);
        final String transactionalId = UUID.randomUUID().toString();
        final TransactionDescription stale =
                createLingeringTransaction(
                        producerProperties(kafka), topic, transactionalId, "old");
        final KafkaTransactionManager manager = new KafkaTransactionManager();
        manager.abortTransaction(kafka.getBootstrapServers(), transactionalId);
        final TransactionDescription current =
                createLingeringTransaction(
                        producerProperties(kafka), topic, transactionalId, "new");
        assertThat(current.producerEpoch()).isGreaterThan(stale.producerEpoch());

        try (Admin admin = createAdmin(kafka)) {
            assertThatThrownBy(
                            () ->
                                    manager.commitTransaction(
                                            kafka.getBootstrapServers(),
                                            transactionalId,
                                            stale.producerId(),
                                            (short) stale.producerEpoch(),
                                            kafka == KAFKA_V2))
                    .hasMessageContaining("Failed to commit transaction");
            assertUnchanged(admin, transactionalId, current);

            manager.commitTransaction(
                    kafka.getBootstrapServers(),
                    transactionalId,
                    current.producerId(),
                    (short) current.producerEpoch(),
                    kafka == KAFKA_V2);

            waitForState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);
            assertThat(consumeCommitted(kafka, topic)).containsExactly("new");
        }
    }

    @Test
    void testDefaultCommitOfV2TransactionRequiresV1ForRetries() throws Exception {
        final String topic = createTopic(KAFKA_V2);
        final String transactionalId = UUID.randomUUID().toString();
        final TransactionDescription original =
                createLingeringTransaction(
                        producerProperties(KAFKA_V2), topic, transactionalId, "committed");
        final KafkaTransactionManager manager = new KafkaTransactionManager();
        try (Admin admin = createAdmin(KAFKA_V2)) {
            assertThat(
                            KafkaTransactionTool.run(
                                    new String[] {
                                        "--action",
                                        "commit",
                                        "--bootstrap-servers",
                                        KAFKA_V2.getBootstrapServers(),
                                        "--transactional-id",
                                        transactionalId,
                                        "--producer-id",
                                        Long.toString(original.producerId()),
                                        "--epoch",
                                        Integer.toString(original.producerEpoch())
                                    }))
                    .isZero();

            waitForState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);
            final TransactionDescription committed = describe(admin, transactionalId);
            assertThat(committed.producerId()).isEqualTo(original.producerId());
            assertThat(committed.producerEpoch()).isEqualTo(original.producerEpoch());
            assertThat(consumeCommitted(KAFKA_V2, topic)).containsExactly("committed");

            manager.commitTransaction(
                    KAFKA_V2.getBootstrapServers(),
                    transactionalId,
                    original.producerId(),
                    (short) original.producerEpoch(),
                    false);
            assertUnchanged(admin, transactionalId, committed);

            assertThatThrownBy(
                            () ->
                                    manager.commitTransaction(
                                            KAFKA_V2.getBootstrapServers(),
                                            transactionalId,
                                            original.producerId(),
                                            (short) original.producerEpoch(),
                                            true))
                    .hasMessageContaining("Failed to commit transaction");

            assertUnchanged(admin, transactionalId, committed);
            assertThat(consumeCommitted(KAFKA_V2, topic)).containsExactly("committed");
        }
    }

    @Test
    void testDefaultProtocolDoesNotAcceptAbortedV1TransactionAsNewerCommit() throws Exception {
        final String topic = createTopic(KAFKA_V1);
        final String transactionalId = UUID.randomUUID().toString();
        try (Admin admin = createAdmin(KAFKA_V1)) {
            final TransactionDescription aborted;
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(
                            producerProperties(KAFKA_V1), transactionalId)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "aborted"));
                producer.flush();
                aborted = describe(admin, transactionalId);
                producer.abortTransaction();
            }
            waitForState(admin, transactionalId, TransactionState.COMPLETE_ABORT);
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(
                            producerProperties(KAFKA_V1), transactionalId)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "newer"));
                producer.flush();
                producer.commitTransaction();
            }
            waitForState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);
            final TransactionDescription committed = describe(admin, transactionalId);
            assertThat(committed.producerId()).isEqualTo(aborted.producerId());
            assertThat(committed.producerEpoch()).isEqualTo(aborted.producerEpoch() + 1);

            assertThatThrownBy(
                            () ->
                                    new KafkaTransactionManager()
                                            .commitTransaction(
                                                    KAFKA_V1.getBootstrapServers(),
                                                    transactionalId,
                                                    aborted.producerId(),
                                                    (short) aborted.producerEpoch()))
                    .hasMessageContaining("Failed to commit transaction");

            assertUnchanged(admin, transactionalId, committed);
            assertThat(consumeCommitted(KAFKA_V1, topic)).containsExactly("newer");
        }
    }

    private static TransactionDescription createLingeringTransaction(
            Properties properties, String topic, String transactionalId, String value)
            throws Exception {
        try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(properties, transactionalId);
                Admin admin = Admin.create(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, null, value));
            producer.flush();
            final TransactionDescription description = describe(admin, transactionalId);
            assertThat(description.state()).isEqualTo(TransactionState.ONGOING);
            return description;
        }
    }

    private static String createTopic(TestKafkaContainer kafka) throws Exception {
        final String topic = UUID.randomUUID().toString();
        KafkaUtil.createNewTopicAndWaitForPartitionAssignment(
                topic, 1, 1, producerProperties(kafka));
        return topic;
    }

    private static void sendSentinel(TestKafkaContainer kafka, String topic) throws Exception {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(kafka))) {
            producer.send(new ProducerRecord<>(topic, 0, null, "sentinel")).get();
        }
    }

    private static List<String> consumeCommitted(TestKafkaContainer kafka, String topic) {
        final Properties properties = producerProperties(kafka);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties, true).stream()
                .map(record -> new String(record.value(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private static Properties producerProperties(TestKafkaContainer kafka) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static Admin createAdmin(TestKafkaContainer kafka) {
        return Admin.create(producerProperties(kafka));
    }

    private static TransactionDescription describe(Admin admin, String transactionalId)
            throws Exception {
        return admin.describeTransactions(Collections.singletonList(transactionalId))
                .description(transactionalId)
                .get();
    }

    private static void waitForState(Admin admin, String transactionalId, TransactionState state)
            throws Exception {
        CommonTestUtils.waitUntilCondition(() -> describe(admin, transactionalId).state() == state);
    }

    private static void assertUnchanged(
            Admin admin, String transactionalId, TransactionDescription expected) throws Exception {
        final TransactionDescription actual = describe(admin, transactionalId);
        assertThat(actual.state()).isEqualTo(expected.state());
        assertThat(actual.producerId()).isEqualTo(expected.producerId());
        assertThat(actual.producerEpoch()).isEqualTo(expected.producerEpoch());
    }
}
