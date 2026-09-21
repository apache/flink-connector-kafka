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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkKafkaShareAckTransactionCommitterTest {

    @Test
    void testCommitsPreparedShareAckTransaction() throws Exception {
        AtomicReference<RecordingProducer> createdProducer = new AtomicReference<>();
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            createdProducer.set(producer);
                            return producer;
                        });

        assertThat(committer.commit(committable("share-ack-txn", "prepared-state")))
                .isEqualTo(TransactionCommitResult.COMMITTED);
        committer.close();

        assertThat(createdProducer.get().events)
                .containsExactly("complete-prepared:prepared-state", "close");
    }

    @Test
    void testCommitsResumedShareAckTransaction() throws Exception {
        AtomicReference<RecordingProducer> createdProducer = new AtomicReference<>();
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            createdProducer.set(producer);
                            return producer;
                        });

        assertThat(committer.commit(committable("share-ack-txn", null)))
                .isEqualTo(TransactionCommitResult.COMMITTED);
        committer.close();

        assertThat(createdProducer.get().events).containsExactly("resume:10:3", "commit", "close");
    }

    @Test
    void testReusesCommitterProducerAcrossTransactions() throws Exception {
        AtomicInteger createdProducers = new AtomicInteger();
        AtomicReference<RecordingProducer> createdProducer = new AtomicReference<>();
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            createdProducers.incrementAndGet();
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            createdProducer.set(producer);
                            return producer;
                        });

        committer.commit(committable("share-ack-txn-1", null));
        committer.commit(committable("share-ack-txn-2", null));
        committer.close();

        assertThat(createdProducers.get()).isEqualTo(1);
        assertThat(createdProducer.get().events)
                .containsExactly(
                        "resume:10:3",
                        "commit",
                        "set-transactional-id:share-ack-txn-2",
                        "resume:10:3",
                        "commit",
                        "close");
    }

    @Test
    void testKeepsCommitterProducerOnRetriableFailure() {
        AtomicReference<RecordingProducer> createdProducer = new AtomicReference<>();
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            producer.commitFailure = new TimeoutException("retry");
                            createdProducer.set(producer);
                            return producer;
                        });

        assertThatThrownBy(() -> committer.commit(committable("share-ack-txn", null)))
                .isInstanceOf(TimeoutException.class);

        assertThat(createdProducer.get().closed).isFalse();
        committer.close();
    }

    @Test
    void testClosesCommitterProducerOnFatalFailure() {
        AtomicReference<RecordingProducer> createdProducer = new AtomicReference<>();
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            producer.commitFailure = new ProducerFencedException("fenced");
                            createdProducer.set(producer);
                            return producer;
                        });

        assertThatThrownBy(() -> committer.commit(committable("share-ack-txn", null)))
                .isInstanceOf(ProducerFencedException.class);

        assertThat(createdProducer.get().closed).isTrue();
    }

    @Test
    void testConvertsKafkaInterruptException() {
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            producer.commitFailure = new InterruptException("interrupted");
                            return producer;
                        });

        assertThatThrownBy(() -> committer.commit(committable("share-ack-txn", null)))
                .isInstanceOf(InterruptedException.class)
                .hasMessageContaining("interrupted");
        assertThat(Thread.interrupted()).isFalse();
        committer.close();
    }

    @Test
    void testCloseClosesCommitterProducer() throws Exception {
        AtomicReference<RecordingProducer> createdProducer = new AtomicReference<>();
        FlinkKafkaShareAckTransactionCommitter committer =
                new FlinkKafkaShareAckTransactionCommitter(
                        properties(),
                        (producerProperties, transactionalId) -> {
                            RecordingProducer producer =
                                    new RecordingProducer(producerProperties, transactionalId);
                            createdProducer.set(producer);
                            return producer;
                        });
        committer.commit(committable("share-ack-txn", null));

        committer.close();

        assertThat(createdProducer.get().closed).isTrue();
    }

    private static ShareAckCommittable committable(
            String transactionalId, String preparedTransactionState) {
        return new ShareAckCommittable(
                "orders-pipeline-v1",
                42L,
                transactionalId,
                10L,
                (short) 3,
                preparedTransactionState,
                List.of(new ShareAckId("group", "topic-id", "orders", 0, 1L)));
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
        private boolean closed;
        private RuntimeException commitFailure;

        private RecordingProducer(Properties properties, String transactionalId) {
            super(properties, transactionalId);
        }

        @Override
        public void completePreparedTransaction(String preparedTransactionState) {
            events.add("complete-prepared:" + preparedTransactionState);
        }

        @Override
        public void resumeTransaction(long producerId, short epoch) {
            events.add("resume:" + producerId + ':' + epoch);
        }

        @Override
        public void commitTransaction() {
            events.add("commit");
            if (commitFailure != null) {
                throw commitFailure;
            }
        }

        @Override
        public void setTransactionId(String transactionalId) {
            events.add("set-transactional-id:" + transactionalId);
        }

        @Override
        public void close() {
            events.add("close");
            closed = true;
            super.close();
        }
    }
}
