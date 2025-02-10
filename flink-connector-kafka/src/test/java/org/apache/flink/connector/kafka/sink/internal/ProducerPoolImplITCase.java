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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ProducerPoolImplITCase {

    public static final Consumer<FlinkKafkaInternalProducer<byte[], byte[]>> INIT = p -> {};
    public static final String TRANSACTIONAL_ID = "test-transactional-id";

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(ProducerPoolImplITCase.class).withEmbeddedZookeeper();

    @AfterEach
    void checkLeak() {
        checkProducerLeak();
    }

    @Test
    void testGetTransactionalProducer() throws Exception {
        try (ProducerPoolImpl producerPool = new ProducerPoolImpl(getProducerConfig(), INIT)) {

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

    /** Tests direct recycling as used during abortion of transactions. */
    @Test
    void testRecycleProducer() throws Exception {
        try (ProducerPoolImpl producerPool = new ProducerPoolImpl(getProducerConfig(), INIT)) {
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
        try (ProducerPoolImpl producerPool = new ProducerPoolImpl(getProducerConfig(), INIT)) {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);

            assertThat(producerPool.getProducers()).isEmpty();
            producer.beginTransaction();
            producerPool.recycleByTransactionId(TRANSACTIONAL_ID);
            assertThat(producerPool.getProducers()).contains(producer);
            // forcefully reset transaction state for split brain scenarios
            assertThat(producer.isInTransaction()).isFalse();

            FlinkKafkaInternalProducer<byte[], byte[]> newProducer =
                    producerPool.getTransactionalProducer(TRANSACTIONAL_ID, 1L);
            assertThat(newProducer).isSameAs(producer);
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
