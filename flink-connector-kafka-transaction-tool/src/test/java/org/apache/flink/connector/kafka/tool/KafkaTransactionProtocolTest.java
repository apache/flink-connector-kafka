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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the actual EndTxn request built by Kafka without requiring a broker. */
class KafkaTransactionProtocolTest {

    @ParameterizedTest
    @CsvSource({"false, 5, 4", "true, 5, 5", "true, 4, 4"})
    void testCommitRequestUsesSelectedProtocol(
            boolean useTransactionV2, short brokerVersion, short expectedVersion) {
        try (InspectingProducer producer = new InspectingProducer(brokerVersion)) {
            final KafkaTransactionManager manager =
                    new KafkaTransactionManager(new Properties(), (properties, id) -> producer);

            manager.commitTransaction(
                    "localhost:1", "protocol-test", 100, (short) 7, useTransactionV2);

            assertThat(producer.request).isNotNull();
            assertThat(producer.request.version()).isEqualTo(expectedVersion);
            assertThat(producer.request.data().transactionalId()).isEqualTo("protocol-test");
            assertThat(producer.request.data().producerId()).isEqualTo(100);
            assertThat(producer.request.data().producerEpoch()).isEqualTo((short) 7);
            assertThat(producer.request.data().committed()).isTrue();
        }
    }

    private static class InspectingProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        private final short brokerVersion;
        private EndTxnRequest request;

        private InspectingProducer(short brokerVersion) {
            super(
                    new KafkaTransactionManager()
                            .createProducerProperties("localhost:1", "protocol-test"),
                    "protocol-test");
            this.brokerVersion = brokerVersion;
        }

        @Override
        public void commitTransaction() {
            try {
                final Field managerField =
                        KafkaProducer.class.getDeclaredField("transactionManager");
                managerField.setAccessible(true);
                final TransactionManager manager = (TransactionManager) managerField.get(this);
                // Keep the sender from dequeuing the request while it is being inspected.
                // These private request internals deliberately pin the wire behavior; re-check
                // them whenever kafka.version changes, alongside the producer's reflection.
                synchronized (manager) {
                    manager.beginCommit();
                    final Field requestsField =
                            TransactionManager.class.getDeclaredField("pendingRequests");
                    requestsField.setAccessible(true);
                    final Queue<?> requests = (Queue<?>) requestsField.get(manager);
                    assertThat(requests).hasSize(1);
                    final Object handler = requests.peek();
                    final Method builderMethod =
                            handler.getClass().getDeclaredMethod("requestBuilder");
                    builderMethod.setAccessible(true);
                    final EndTxnRequest.Builder builder =
                            (EndTxnRequest.Builder) builderMethod.invoke(handler);
                    request = builder.build(brokerVersion);
                }
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("Cannot inspect Kafka's EndTxn request", e);
            }
        }

        @Override
        public void initTransactions() {
            throw new AssertionError("Resuming a transaction must not initialize or fence it");
        }
    }
}
