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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Unit tests for {@link KafkaTransactionManager} input validation and error handling. */
class KafkaTransactionManagerTest {

    private final KafkaTransactionManager manager = new KafkaTransactionManager();

    @Test
    void testAbortWithNullBootstrapServers() {
        assertThatThrownBy(() -> manager.abortTransaction(null, "tx-1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Bootstrap servers cannot be null");
    }

    @Test
    void testAbortWithEmptyBootstrapServers() {
        assertThatThrownBy(() -> manager.abortTransaction("", "tx-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bootstrap servers cannot be empty");
    }

    @Test
    void testAbortWithNullTransactionalId() {
        assertThatThrownBy(() -> manager.abortTransaction("localhost:9092", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Transactional ID cannot be null");
    }

    @Test
    void testAbortWithEmptyTransactionalId() {
        assertThatThrownBy(() -> manager.abortTransaction("localhost:9092", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Transactional ID cannot be empty");
    }

    @Test
    void testCommitWithNegativeProducerId() {
        assertThatThrownBy(() -> manager.commitTransaction("localhost:9092", "tx-1", -1, (short) 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Producer ID must be non-negative");
    }

    @Test
    void testCommitWithNegativeEpoch() {
        assertThatThrownBy(
                        () -> manager.commitTransaction("localhost:9092", "tx-1", 100, (short) -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Epoch must be non-negative");
    }

    @Test
    void testClientPropertiesArePreservedAndToolPropertiesTakePrecedence() {
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        clientProperties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        clientProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000");
        clientProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "configured:9092");
        clientProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "configured-tx");
        clientProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        clientProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        final Properties originalProperties = new Properties();
        originalProperties.putAll(clientProperties);
        final KafkaTransactionManager configuredManager =
                new KafkaTransactionManager(clientProperties);

        final Properties producerProperties =
                configuredManager.createProducerProperties("requested:9092", "requested-tx");

        assertThat(producerProperties)
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
                .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000")
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "requested:9092")
                .containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "requested-tx")
                .containsEntry(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName())
                .containsEntry(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName());
        assertThat(clientProperties).isEqualTo(originalProperties);

        clientProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1");
        producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "2");
        assertThat(configuredManager.createProducerProperties("other:9092", "other-tx"))
                .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000");
    }

    @Test
    void testAdminPropertiesPreserveClientConfigurationWithoutInjectingProducerSettings() {
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        clientProperties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        clientProperties.setProperty("custom.callback.option", "custom-value");
        clientProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000");
        clientProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "configured:9092");
        final Properties originalProperties = new Properties();
        originalProperties.putAll(clientProperties);
        final KafkaTransactionManager configuredManager =
                new KafkaTransactionManager(clientProperties);

        configuredManager.createProducerProperties("requested:9092", "requested-tx");
        final Properties adminProperties =
                configuredManager.createAdminProperties("requested:9092");

        assertThat(adminProperties)
                .containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
                .containsEntry("custom.callback.option", "custom-value")
                .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "180000")
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "requested:9092")
                .doesNotContainKeys(
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        assertThat(clientProperties).isEqualTo(originalProperties);

        adminProperties.setProperty("custom.callback.option", "changed-value");
        assertThat(configuredManager.createAdminProperties("other:9092"))
                .containsEntry("custom.callback.option", "custom-value");
    }

    @Test
    void testDefaultClientPropertiesKeepKafkaTimeout() {
        final Properties producerProperties =
                manager.createProducerProperties("localhost:9092", "tx-1");

        assertThat(producerProperties).doesNotContainKey(ProducerConfig.MAX_BLOCK_MS_CONFIG);
    }

    @Test
    void testCommitTimeoutReportsUnknownOutcome() {
        final TimeoutException timeout = new TimeoutException("Commit acknowledgement timed out");

        assertUnknownCommitOutcome(() -> timeout, false);
    }

    @Test
    void testCommitInterruptionReportsUnknownOutcomeAndRestoresInterrupt() {
        assertUnknownCommitOutcome(() -> new InterruptException("Commit interrupted"), true);
    }

    @Test
    void testFencedCommitRemainsAFailure() {
        final ProducerFencedException fenced = new ProducerFencedException("Producer fenced");
        final TestingProducer producer = new TestingProducer(() -> fenced);
        final KafkaTransactionManager failingManager =
                new KafkaTransactionManager(new Properties(), (properties, id) -> producer);

        assertThatThrownBy(
                        () ->
                                failingManager.commitTransaction(
                                        "localhost:9092", "tx-1", 100, (short) 2))
                .hasMessageContaining("Failed to commit transaction")
                .hasCause(fenced);
        assertThat(producer.isClosed()).isTrue();
    }

    private void assertUnknownCommitOutcome(
            Supplier<RuntimeException> commitFailure, boolean interrupted) {
        final TestingProducer producer = new TestingProducer(commitFailure);
        final KafkaTransactionManager failingManager =
                new KafkaTransactionManager(new Properties(), (properties, id) -> producer);

        try {
            final Throwable failure =
                    catchThrowable(
                            () ->
                                    failingManager.commitTransaction(
                                            "localhost:9092", "tx-1", 100, (short) 2));

            assertThat(failure)
                    .isInstanceOf(CommitOutcomeUnknownException.class)
                    .hasMessageContaining("Commit outcome is unknown")
                    .hasMessageContaining("tx-1")
                    .hasMessageContaining("ProducerId: 100")
                    .hasMessageContaining("Epoch: 2")
                    .hasMessageContaining("Retry only the same commit")
                    .hasMessageContaining("same transactional ID, producer ID and epoch")
                    .hasMessageContaining("do not abort")
                    .hasCause(producer.commitFailure);
            assertThat(Thread.currentThread().isInterrupted()).isEqualTo(interrupted);
            assertThat(producer.getProducerId()).isEqualTo(100);
            assertThat(producer.getEpoch()).isEqualTo((short) 2);
            assertThat(producer.commitAttempts).isEqualTo(1);
            assertThat(producer.isClosed()).isTrue();
        } finally {
            Thread.interrupted();
            producer.close();
        }
    }

    private static class TestingProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        private final Supplier<RuntimeException> commitFailureSupplier;
        private RuntimeException commitFailure;
        private int commitAttempts;

        private TestingProducer(Supplier<RuntimeException> commitFailureSupplier) {
            super(
                    new KafkaTransactionManager().createProducerProperties("localhost:1", "tx-1"),
                    "tx-1");
            this.commitFailureSupplier = commitFailureSupplier;
        }

        @Override
        public void commitTransaction() {
            commitAttempts++;
            commitFailure = commitFailureSupplier.get();
            throw commitFailure;
        }

        @Override
        public void initTransactions() {
            throw new AssertionError("Commit recovery must not fence the previous producer");
        }

        @Override
        public void abortTransaction() {
            throw new AssertionError("An uncertain commit must never be aborted");
        }

        @Override
        public void close() {
            // Closing Kafka's sender thread may consume the interrupt before the manager catches
            // it.
            Thread.interrupted();
            super.close();
        }
    }
}
