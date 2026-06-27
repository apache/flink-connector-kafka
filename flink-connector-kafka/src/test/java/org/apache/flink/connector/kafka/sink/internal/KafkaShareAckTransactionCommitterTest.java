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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.connector.kafka.share.ShareAckCommittable;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaShareAckTransactionCommitterTest {

    private static final String TRANS_ID = "share-txn";

    @Test
    void testPreparedCommittableUsesPreparedRecovery() throws Exception {
        AtomicReference<RecordingProducer> producer = new AtomicReference<>();
        try (KafkaShareAckTransactionCommitter committer =
                new KafkaShareAckTransactionCommitter(getProperties(), recordingFactory(producer))) {

            committer.commit(List.of(preparedCommittable(TRANS_ID, "3:4")));

            assertThat(producer.get().completedPreparedTransactionState).isEqualTo("3:4");
            assertThat(producer.get().resumeCalls).isZero();
            assertThat(producer.get().commitCalls).isZero();
        }
    }

    @Test
    void testLegacyCommittableUsesOwnerFence() throws Exception {
        AtomicReference<RecordingProducer> producer = new AtomicReference<>();
        try (KafkaShareAckTransactionCommitter committer =
                new KafkaShareAckTransactionCommitter(getProperties(), recordingFactory(producer))) {

            committer.commit(List.of(legacyCommittable(TRANS_ID)));

            assertThat(producer.get().resumedProducerId).isEqualTo(3L);
            assertThat(producer.get().resumedEpoch).isEqualTo((short) 4);
            assertThat(producer.get().commitCalls).isOne();
            assertThat(producer.get().completedPreparedTransactionState).isNull();
        }
    }

    @Test
    void testReusesProducerForMultipleCommittables() throws Exception {
        AtomicReference<RecordingProducer> producer = new AtomicReference<>();
        try (KafkaShareAckTransactionCommitter committer =
                new KafkaShareAckTransactionCommitter(getProperties(), recordingFactory(producer))) {

            committer.commit(
                    List.of(
                            preparedCommittable("share-txn-0", "3:4"),
                            preparedCommittable("share-txn-1", "5:6")));

            assertThat(producer.get().createdTransactionalId).isEqualTo("share-txn-0");
            assertThat(producer.get().transactionalIds).containsExactly("share-txn-1");
            assertThat(producer.get().completedPreparedStates).containsExactly("3:4", "5:6");
        }
    }

    @Test
    void testRetriableFailureThrowsIOExceptionAndKeepsProducer() throws Exception {
        AtomicReference<RecordingProducer> producer = new AtomicReference<>();
        producer.updateAndGet(
                ignored -> {
                    RecordingProducer recordingProducer =
                            new RecordingProducer(getProperties(), TRANS_ID);
                    recordingProducer.completeException = new TimeoutException("retry");
                    return recordingProducer;
                });
        try (KafkaShareAckTransactionCommitter committer =
                new KafkaShareAckTransactionCommitter(getProperties(), recordingFactory(producer))) {

            assertThatThrownBy(
                            () -> committer.commit(List.of(preparedCommittable(TRANS_ID, "3:4"))))
                    .isInstanceOf(IOException.class)
                    .hasRootCauseInstanceOf(TimeoutException.class);
            assertThat(committer.getCommittingProducer()).isSameAs(producer.get());
            assertThat(producer.get().closed).isFalse();
        }
    }

    @Test
    void testFatalFailureClosesProducer() throws Exception {
        AtomicReference<RecordingProducer> producer = new AtomicReference<>();
        producer.updateAndGet(
                ignored -> {
                    RecordingProducer recordingProducer =
                            new RecordingProducer(getProperties(), TRANS_ID);
                    recordingProducer.completeException = new ProducerFencedException("fenced");
                    return recordingProducer;
                });
        try (KafkaShareAckTransactionCommitter committer =
                new KafkaShareAckTransactionCommitter(getProperties(), recordingFactory(producer))) {

            assertThatThrownBy(
                            () -> committer.commit(List.of(preparedCommittable(TRANS_ID, "3:4"))))
                    .isInstanceOf(ProducerFencedException.class);
            assertThat(committer.getCommittingProducer()).isNull();
            assertThat(producer.get().closed).isTrue();
        }
    }

    @AfterEach
    void check() {
        checkProducerLeak();
    }

    private static BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>>
            recordingFactory(AtomicReference<RecordingProducer> producer) {
        return (properties, transactionalId) -> {
            if (producer.get() == null) {
                producer.set(new RecordingProducer(properties, transactionalId));
            }
            return producer.get();
        };
    }

    private static ShareAckCommittable preparedCommittable(
            String transactionalId, String preparedState) {
        return new ShareAckCommittable(
                42L, transactionalId, 3L, (short) 4, preparedState, "share-group", 0);
    }

    private static ShareAckCommittable legacyCommittable(String transactionalId) {
        return new ShareAckCommittable(42L, transactionalId, 3L, (short) 4, "share-group", 0);
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:1");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private static class RecordingProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {

        private final String createdTransactionalId;
        private final List<String> transactionalIds = new ArrayList<>();
        private final List<String> completedPreparedStates = new ArrayList<>();
        private String completedPreparedTransactionState;
        private RuntimeException completeException;
        private long resumedProducerId;
        private short resumedEpoch;
        private int resumeCalls;
        private int commitCalls;
        private boolean closed;

        private RecordingProducer(Properties properties, String transactionalId) {
            super(properties, transactionalId);
            this.createdTransactionalId = transactionalId;
        }

        @Override
        public void setTransactionId(String transactionalId) {
            transactionalIds.add(transactionalId);
        }

        @Override
        public void completePreparedTransaction(String preparedTransactionState) {
            if (completeException != null) {
                throw completeException;
            }
            completedPreparedTransactionState = preparedTransactionState;
            completedPreparedStates.add(preparedTransactionState);
        }

        @Override
        public void resumeTransaction(long producerId, short epoch) {
            resumeCalls++;
            resumedProducerId = producerId;
            resumedEpoch = epoch;
        }

        @Override
        public void commitTransaction() {
            commitCalls++;
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }
    }
}
