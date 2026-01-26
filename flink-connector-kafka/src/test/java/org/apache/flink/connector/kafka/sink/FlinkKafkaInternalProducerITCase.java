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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class FlinkKafkaInternalProducerITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    @Container
    private static final TestKafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(FlinkKafkaInternalProducerITCase.class);

    @AfterEach
    public void check() {
        checkProducerLeak();
    }

    @Test
    void testResetTransactional() {
        final String topic = "test-init-transactions";
        final String transactionIdPrefix = "testInitTransactionId-";
        try (FlinkKafkaInternalProducer<String, String> reuse =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            int numTransactions = 20;
            for (int i = 1; i <= numTransactions; i++) {
                reuse.setTransactionId(transactionIdPrefix + i);
                reuse.initTransactions();
                reuse.beginTransaction();
                reuse.send(new ProducerRecord<>(topic, "test-value-" + i));
                if (i % 2 == 0) {
                    reuse.commitTransaction();
                } else {
                    reuse.flush();
                    reuse.abortTransaction();
                }
                assertNumTransactions(i, transactionIdPrefix);
                assertThat(readRecords(topic).count()).isEqualTo(i / 2);
            }
        }
    }

    @Test
    void testCommitResumedTransaction() {
        final String topic = "test-commit-resumed-transaction";
        final String transactionIdPrefix = "testCommitResumedTransaction-";
        final String transactionalId = transactionIdPrefix + "id";

        KafkaCommittable snapshottedCommittable;
        try (FlinkKafkaInternalProducer<String, String> producer =
                new FlinkKafkaInternalProducer<>(getProperties(), transactionalId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "test-value"));
            producer.flush();
            snapshottedCommittable = KafkaCommittable.of(producer);
        }

        try (FlinkKafkaInternalProducer<String, String> resumedProducer =
                new FlinkKafkaInternalProducer<>(getProperties(), transactionalId)) {
            resumedProducer.resumeTransaction(
                    snapshottedCommittable.getProducerId(), snapshottedCommittable.getEpoch());
            resumedProducer.commitTransaction();
        }

        assertNumTransactions(1, transactionIdPrefix);
        assertThat(readRecords(topic).count()).isEqualTo(1);
    }

    @Test
    void testCommitResumedEmptyTransactionShouldFail() {
        KafkaCommittable snapshottedCommittable;
        try (FlinkKafkaInternalProducer<String, String> producer =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            producer.initTransactions();
            producer.beginTransaction();
            snapshottedCommittable = KafkaCommittable.of(producer);
        }

        try (FlinkKafkaInternalProducer<String, String> resumedProducer =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            resumedProducer.resumeTransaction(
                    snapshottedCommittable.getProducerId(), snapshottedCommittable.getEpoch());

            assertThatThrownBy(resumedProducer::commitTransaction)
                    .isInstanceOf(InvalidTxnStateException.class);
        }
    }

    @ParameterizedTest
    @MethodSource("provideTransactionsFinalizer")
    void testResetInnerTransactionIfFinalizingTransactionFailed(
            Consumer<FlinkKafkaInternalProducer<?, ?>> transactionFinalizer) {
        final String topic = "reset-producer-internal-state";
        try (FlinkKafkaInternalProducer<String, String> fenced =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            fenced.initTransactions();
            fenced.beginTransaction();
            fenced.send(new ProducerRecord<>(topic, "test-value"));
            // Start a second producer that fences the first one
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "test-value"));
                producer.commitTransaction();
            }
            assertThatThrownBy(() -> transactionFinalizer.accept(fenced))
                    .isInstanceOf(ProducerFencedException.class);
            // Internal transaction should be reset and setting a new transactional id is possible
            fenced.setTransactionId("dummy2");
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testDoubleCommitAndAbort(boolean firstCommit, boolean secondCommit) {
        final String topic = "test-double-commit-transaction-" + firstCommit + secondCommit;
        final String transactionIdPrefix = "testDoubleCommitTransaction-";
        final String transactionalId = transactionIdPrefix + "id";

        KafkaCommittable committable;
        try (FlinkKafkaInternalProducer<String, String> producer =
                new FlinkKafkaInternalProducer<>(getProperties(), transactionalId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "test-value"));
            producer.flush();
            committable = KafkaCommittable.of(producer);
            if (firstCommit) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }

        try (FlinkKafkaInternalProducer<String, String> resumedProducer =
                new FlinkKafkaInternalProducer<>(getProperties(), transactionalId)) {
            resumedProducer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
            AbstractThrowableAssert<?, ? extends Throwable> secondOp =
                    assertThatCode(
                            () -> {
                                if (secondCommit) {
                                    resumedProducer.commitTransaction();
                                } else {
                                    resumedProducer.abortTransaction();
                                }
                            });
            if (firstCommit == secondCommit) {
                secondOp.doesNotThrowAnyException();
            } else {
                secondOp.isInstanceOf(InvalidTxnStateException.class);
            }
        }

        assertNumTransactions(1, transactionIdPrefix);
        assertThat(readRecords(topic).count()).isEqualTo(firstCommit ? 1 : 0);
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private static List<Consumer<FlinkKafkaInternalProducer<?, ?>>> provideTransactionsFinalizer() {
        return Arrays.asList(
                FlinkKafkaInternalProducer::commitTransaction,
                FlinkKafkaInternalProducer::abortTransaction);
    }

    private void assertNumTransactions(int numTransactions, String transactionIdPrefix) {
        List<KafkaTransactionLog.TransactionRecord> transactions =
                new KafkaTransactionLog(getProperties())
                        .getTransactions(id -> id.startsWith(transactionIdPrefix));
        assertThat(
                        transactions.stream()
                                .map(KafkaTransactionLog.TransactionRecord::getTransactionId)
                                .collect(Collectors.toSet()))
                .hasSize(numTransactions);
    }

    private ConsumerRecords<String, String> readRecords(String topic) {
        Properties properties = getProperties();
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.assign(
                consumer.partitionsFor(topic).stream()
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                        .collect(Collectors.toSet()));
        consumer.seekToBeginning(consumer.assignment());
        return consumer.poll(Duration.ofMillis(1000));
    }
}
