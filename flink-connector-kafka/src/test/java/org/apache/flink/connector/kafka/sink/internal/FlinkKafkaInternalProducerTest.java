/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkKafkaInternalProducerTest {

    @Test
    void testAcknowledgementOnlyTransactionCanBePrepared() throws Exception {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer = openTransaction()) {
            assertThat(producer.hasWorkInTransaction()).isFalse();
            producer.markShareAcksStaged();
            producer.markShareAcksStaged();

            assertThat(producer.hasRecordsInTransaction()).isFalse();
            assertThat(producer.hasWorkInTransaction()).isTrue();
            assertThat(producer.precommitTransaction()).isEmpty();
            assertThat(producer.isPrecommitted()).isTrue();
            assertThat(producer.hasWorkInTransaction()).isFalse();
            assertThatThrownBy(producer::markShareAcksStaged)
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>("output", new byte[0])))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Transaction is not open for records");
            assertThat(producer.isPrecommitted()).isTrue();
        }
    }

    @Test
    void testOutputOnlyAndMixedTransactionsCanBePrepared() throws Exception {
        for (boolean withAcknowledgements : new boolean[] {false, true}) {
            try (FlinkKafkaInternalProducer<byte[], byte[]> producer = openTransaction()) {
                setState(producer, FlinkKafkaInternalProducer.TransactionState.DATA_IN_TRANSACTION);
                if (withAcknowledgements) {
                    producer.markShareAcksStaged();
                }

                assertThat(producer.hasRecordsInTransaction()).isTrue();
                assertThat(producer.hasWorkInTransaction()).isTrue();
                assertThat(producer.precommitTransaction()).isEmpty();
            }
        }
    }

    @Test
    void testEmptyTransactionCannotBePrepared() throws Exception {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer = openTransaction()) {
            assertThatThrownBy(producer::precommitTransaction)
                    .isInstanceOf(IllegalStateException.class);
            producer.commitTransaction();
            assertThat(producer.isInTransaction()).isFalse();
        }
    }

    @Test
    void testRejectsAcknowledgementsOutsideOpenTransaction() {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer = newProducer()) {
            assertThatThrownBy(producer::markShareAcksStaged)
                    .isInstanceOf(IllegalStateException.class);
            assertThat(producer.hasWorkInTransaction()).isFalse();
        }
    }

    @Test
    void testNewTransactionDoesNotInheritAcknowledgements() throws Exception {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer = openTransaction()) {
            producer.markShareAcksStaged();
            producer.commitTransaction();
            producer.beginTransaction();

            assertThat(producer.hasWorkInTransaction()).isFalse();
            assertThat(producer.hasRecordsInTransaction()).isFalse();
            producer.abortTransaction();
            assertThat(producer.isInTransaction()).isFalse();
        }
    }

    @Test
    void testCommitTimeoutCanRetryButCannotSwitchToAbort() {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer = newProducer()) {
            producer.resumeTransaction(42L, (short) 0);

            assertThatThrownBy(producer::commitTransaction).isInstanceOf(TimeoutException.class);
            assertThat(producer.isInTransaction()).isTrue();
            assertThat(producer.hasWorkInTransaction()).isFalse();
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>("output", new byte[0])))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(producer::commitTransaction).isInstanceOf(TimeoutException.class);
            assertThatThrownBy(producer::abortTransaction)
                    .isInstanceOf(IllegalStateException.class);
            assertThat(producer.isInTransaction()).isTrue();
        }
    }

    @Test
    void testAbortTimeoutCanRetryButCannotSwitchToCommit() {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer = newProducer()) {
            producer.resumeTransaction(42L, (short) 0);

            assertThatThrownBy(producer::abortTransaction).isInstanceOf(TimeoutException.class);
            assertThat(producer.isInTransaction()).isTrue();
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>("output", new byte[0])))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(producer::abortTransaction).isInstanceOf(TimeoutException.class);
            assertThatThrownBy(producer::commitTransaction)
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testInterruptedCompletionKeepsPendingOperation() {
        for (boolean commit : new boolean[] {true, false}) {
            try (FlinkKafkaInternalProducer<byte[], byte[]> producer = newProducer()) {
                producer.resumeTransaction(42L, (short) 0);
                Runnable operation =
                        commit ? producer::commitTransaction : producer::abortTransaction;
                Runnable opposite =
                        commit ? producer::abortTransaction : producer::commitTransaction;
                try {
                    Thread.currentThread().interrupt();
                    assertThatThrownBy(operation::run).isInstanceOf(InterruptException.class);
                } finally {
                    Thread.interrupted();
                }
                assertThat(producer.isInTransaction()).isTrue();
                assertThatThrownBy(opposite::run).isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(operation::run).isInstanceOf(TimeoutException.class);
            }
        }
    }

    private static FlinkKafkaInternalProducer<byte[], byte[]> openTransaction() throws Exception {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = newProducer();
        Field managerField = KafkaProducer.class.getDeclaredField("transactionManager");
        managerField.setAccessible(true);
        Object manager = managerField.get(producer);
        Field stateField = manager.getClass().getDeclaredField("currentState");
        stateField.setAccessible(true);
        Object readyState =
                java.util.Arrays.stream(stateField.getType().getEnumConstants())
                        .filter(state -> state.toString().equals("READY"))
                        .findFirst()
                        .orElseThrow();
        stateField.set(manager, readyState);
        producer.beginTransaction();
        return producer;
    }

    private static void setState(
            FlinkKafkaInternalProducer<?, ?> producer,
            FlinkKafkaInternalProducer.TransactionState state)
            throws Exception {
        Field stateField = FlinkKafkaInternalProducer.class.getDeclaredField("transactionState");
        stateField.setAccessible(true);
        stateField.set(producer, state);
    }

    private static FlinkKafkaInternalProducer<byte[], byte[]> newProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new FlinkKafkaInternalProducer<>(properties, "share-ack-test");
    }
}
