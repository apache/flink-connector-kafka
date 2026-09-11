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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class ProducerPoolImplITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    public static final Consumer<FlinkKafkaInternalProducer<byte[], byte[]>> INIT = p -> {};
    public static final String TRANSACTIONAL_ID = "test-transactional-id";

    @Container
    public static final TestKafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(ProducerPoolImplITCase.class);

    @AfterEach
    void checkLeak() {
        checkProducerLeak();
    }

    @Test
    void testGetTransactionalProducer() throws Exception {
        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(getProducerConfig(), INIT, Collections.emptyList())) {

            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);
            assertThat(producer).isNotNull();
            assertThat(producer.getTransactionalId()).isEqualTo(TRANSACTIONAL_ID);
            assertThat(producer.isInTransaction()).isFalse();
            // everything prepared to being the transaction
            producer.beginTransaction();
            // no explicit closing of producer - pool should also clean up the producer
        }
    }

    /** Tests direct recycling as used during abort of transactions. */
    @Test
    void testRecycleProducer() throws Exception {
        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(getProducerConfig(), INIT, Collections.emptyList())) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);

            assertThat(producerPool.getProducers()).isEmpty();
            producerPool.recycle(producer);
            assertThat(producerPool.getProducers()).contains(producer);

            FlinkKafkaInternalProducer<byte[], byte[]> newProducer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);
            assertThat(newProducer).isSameAs(producer);
        }
    }

    /** Tests indirect recycling triggered through the backchannel. */
    @Test
    void testRecycleByTransactionId() throws Exception {
        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(getProducerConfig(), INIT, Collections.emptyList())) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);

            assertThat(producerPool.getProducers()).isEmpty();
            producer.beginTransaction();
            producerPool.recycleByTransactionId(TRANSACTIONAL_ID, true);
            assertThat(producerPool.getProducers()).contains(producer);
            // forcefully reset transaction state for split brain scenarios
            assertThat(producer.isInTransaction()).isFalse();

            FlinkKafkaInternalProducer<byte[], byte[]> newProducer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);
            assertThat(newProducer).isSameAs(producer);
        }
    }

    /**
     * Ongoing transactions opened by the pool carry the producer id and epoch of their producer.
     */
    @Test
    void testOngoingTransactionsCarryProducerEpoch() throws Exception {
        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(getProducerConfig(), INIT, Collections.emptyList())) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);

            CheckpointTransaction ongoing = producerPool.getOngoingTransactions().iterator().next();
            assertThat(ongoing.getTransactionalId()).isEqualTo(TRANSACTIONAL_ID);
            assertThat(ongoing.getCheckpointId()).isEqualTo(1L);
            assertThat(ongoing.hasKnownEpoch()).isTrue();
            assertThat(ongoing.getProducerId()).isEqualTo(producer.getProducerId());
            assertThat(ongoing.getEpoch()).isEqualTo(producer.getEpoch());
        }
    }

    /**
     * A transaction restored from state can be abandoned on recovery when the broker no longer
     * holds it; the id is then free for a new producer, and the remaining restored transactions are
     * untouched.
     */
    @Test
    void testAbandonRestoredTransaction() throws Exception {
        CheckpointTransaction superseded =
                new CheckpointTransaction(TRANSACTIONAL_ID + "-0", 1L, 42L, (short) 3);
        CheckpointTransaction kept =
                new CheckpointTransaction(TRANSACTIONAL_ID + "-1", 1L, 43L, (short) 4);
        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(getProducerConfig(), INIT, List.of(superseded, kept))) {
            producerPool.abandonTransaction(superseded.getTransactionalId());
            assertThat(producerPool.getOngoingTransactions()).containsExactly(kept);

            // the id can be used again, which is how the recovery aborts the open transaction
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(superseded.getTransactionalId(), 2L);
            assertThat(producer.getTransactionalId()).isEqualTo(superseded.getTransactionalId());
            assertThat(producerPool.getOngoingTransactions())
                    .extracting(CheckpointTransaction::getTransactionalId)
                    .containsExactlyInAnyOrder(
                            kept.getTransactionalId(), superseded.getTransactionalId());

            assertThatThrownBy(() -> producerPool.abandonTransaction("unknown"))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(
                            () -> producerPool.abandonTransaction(superseded.getTransactionalId()))
                    .as("a transaction with a live producer cannot be abandoned")
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    /** Tests the edge case where some transaction ids are implicitly closed. */
    @ParameterizedTest
    @ValueSource(longs = {2, 3})
    void testEarlierTransactionRecycleByTransactionId(long finishedCheckpoint) throws Exception {
        CheckpointTransaction oldTransaction1 =
                new CheckpointTransaction(TRANSACTIONAL_ID + "-0", 1L);
        CheckpointTransaction oldTransaction2 =
                new CheckpointTransaction(TRANSACTIONAL_ID + "-1", 2L);

        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(
                        getProducerConfig(),
                        INIT,
                        Arrays.asList(oldTransaction1, oldTransaction2))) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(
                            TRANSACTIONAL_ID + "-2", finishedCheckpoint);

            assertThat(producerPool.getOngoingTransactions()).hasSize(3);

            assertThat(producerPool.getProducers()).isEmpty();
            producer.beginTransaction();
            producerPool.recycleByTransactionId(TRANSACTIONAL_ID + "-2", true);
            assertThat(producerPool.getProducers()).contains(producer);

            // expect that old transactions have been removed where checkpoint id is smaller
            if (finishedCheckpoint == 2) {
                assertThat(producerPool.getOngoingTransactions()).hasSize(1);
            } else {
                assertThat(producerPool.getOngoingTransactions()).hasSize(0);
            }
        }
    }

    /** Tests indirect recycling triggered through the backchannel. */
    @Test
    void testCloseByTransactionId() throws Exception {
        try (ProducerPoolImpl producerPool =
                new ProducerPoolImpl(getProducerConfig(), INIT, List.of())) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);

            assertThat(producerPool.getProducers()).isEmpty();
            producer.beginTransaction();
            producerPool.recycleByTransactionId(TRANSACTIONAL_ID, false);
            assertThat(producerPool.getProducers()).doesNotContain(producer);
            // forcefully reset transaction state for split brain scenarios
            assertThat(producer.isClosed()).isTrue();

            FlinkKafkaInternalProducer<byte[], byte[]> newProducer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);
            assertThat(newProducer).isNotSameAs(producer);
        }
    }

    private static Properties getProducerConfig() {
        Properties kafkaProducerConfig = new Properties();
        kafkaProducerConfig.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        kafkaProducerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProducerConfig;
    }
}
