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

package org.apache.flink.connector.kafka.share;

import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkKafkaShareAckTransactionalProducerTest {

    @Test
    void testStagesAndPreparesShareAckTransaction() throws Exception {
        RecordingProducer internalProducer = new RecordingProducer("share-ack-txn");
        FlinkKafkaShareAckTransactionalProducer producer =
                new FlinkKafkaShareAckTransactionalProducer(
                        internalProducer,
                        (kafkaProducer, payload) -> {
                            assertThat(kafkaProducer).isSameAs(internalProducer);
                            internalProducer.events.add("stage:" + payload.getId());
                        });

        producer.beginTransaction();
        producer.stage(payload(1L));
        Optional<String> preparedTransactionState = producer.prepareTransaction();
        producer.close();

        assertThat(preparedTransactionState).contains("prepared-state");
        assertThat(producer.getTransactionalId()).isEqualTo("share-ack-txn");
        assertThat(producer.getProducerId()).isEqualTo(10L);
        assertThat(producer.getProducerEpoch()).isEqualTo((short) 3);
        assertThat(internalProducer.events)
                .containsExactly(
                        "begin",
                        "stage:group|topic-id|orders|0|1",
                        "mark-share-acks",
                        "prepare",
                        "close");
    }

    @Test
    void testDoesNotMarkShareAcksWhenStagingFails() {
        RecordingProducer internalProducer = new RecordingProducer("share-ack-txn");
        FlinkKafkaShareAckTransactionalProducer producer =
                new FlinkKafkaShareAckTransactionalProducer(
                        internalProducer,
                        (kafkaProducer, payload) -> {
                            throw new IOException("stage failed");
                        });

        producer.beginTransaction();

        assertThatThrownBy(() -> producer.stage(payload(1L)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("stage failed");
        assertThat(internalProducer.events).containsExactly("begin");
        producer.close();
    }

    @Test
    void testRejectsNonTransactionalProducer() {
        RecordingProducer internalProducer = new RecordingProducer(null);
        FlinkKafkaShareAckTransactionalProducer producer =
                new FlinkKafkaShareAckTransactionalProducer(internalProducer, (p, payload) -> {});

        assertThatThrownBy(producer::getTransactionalId)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transactional Kafka producer");
        producer.close();
    }

    private static ShareAckPayload payload(long offset) {
        return new ShareAckRecord(
                        new ShareAckId("group", "topic-id", "orders", 0, offset),
                        "member",
                        7,
                        ShareAckDecision.ACCEPT)
                .toPayload();
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:1");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return properties;
    }

    private static final class RecordingProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        private final List<String> events = new ArrayList<>();
        private final String transactionalId;

        private RecordingProducer(String transactionalId) {
            super(properties(), "unused");
            this.transactionalId = transactionalId;
        }

        @Override
        public void beginTransaction() {
            events.add("begin");
        }

        @Override
        public void markShareAcksStaged() {
            events.add("mark-share-acks");
        }

        @Override
        public Optional<String> precommitTransaction() {
            events.add("prepare");
            return Optional.of("prepared-state");
        }

        @Override
        public String getTransactionalId() {
            return transactionalId;
        }

        @Override
        public long getProducerId() {
            return 10L;
        }

        @Override
        public short getEpoch() {
            return 3;
        }

        @Override
        public void close() {
            events.add("close");
        }
    }
}
