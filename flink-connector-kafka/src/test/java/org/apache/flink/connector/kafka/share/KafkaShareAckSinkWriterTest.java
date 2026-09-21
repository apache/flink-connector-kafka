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

import org.apache.flink.api.connector.sink2.SinkWriter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaShareAckSinkWriterTest {

    private static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return Long.MIN_VALUE;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    @Test
    void testWritesAcksIntoCheckpointAlignedTransaction() throws Exception {
        RecordingProducerFactory producerFactory = new RecordingProducerFactory();
        KafkaShareAckSinkWriter writer =
                new KafkaShareAckSinkWriter("orders-pipeline-v1", 2, 5L, producerFactory);

        writer.write(record(1L), CONTEXT);
        writer.write(record(2L), CONTEXT);
        Collection<ShareAckCommittable> committables = writer.prepareCommit();

        assertThat(producerFactory.transactionalIds)
                .containsExactly("orders-pipeline-v1-share-ack-2-5");
        assertThat(committables).singleElement().satisfies(
                committable -> {
                    assertThat(committable.getAckScopeId()).isEqualTo("orders-pipeline-v1");
                    assertThat(committable.getCheckpointId()).isEqualTo(5L);
                    assertThat(committable.getTransactionalId())
                            .isEqualTo("orders-pipeline-v1-share-ack-2-5");
                    assertThat(committable.getShareAckIds())
                            .containsExactly(id(1L), id(2L));
                });
        assertThat(producerFactory.producers.get(0).events)
                .containsExactly(
                        "begin",
                        "stage:group|topic-id|orders|0|1",
                        "stage:group|topic-id|orders|0|2",
                        "prepare",
                        "close");
    }

    @Test
    void testEmptyCheckpointAdvancesTransactionId() throws Exception {
        RecordingProducerFactory producerFactory = new RecordingProducerFactory();
        KafkaShareAckSinkWriter writer =
                new KafkaShareAckSinkWriter("orders-pipeline-v1", 2, 5L, producerFactory);

        assertThat(writer.prepareCommit()).isEmpty();
        writer.write(record(1L), CONTEXT);
        Collection<ShareAckCommittable> committables = writer.prepareCommit();

        assertThat(producerFactory.transactionalIds)
                .containsExactly("orders-pipeline-v1-share-ack-2-6");
        assertThat(committables).singleElement().satisfies(
                committable -> assertThat(committable.getCheckpointId()).isEqualTo(6L));
    }

    @Test
    void testPrepareStartsNextTransactionOnlyWhenNextAckArrives() throws Exception {
        RecordingProducerFactory producerFactory = new RecordingProducerFactory();
        KafkaShareAckSinkWriter writer =
                new KafkaShareAckSinkWriter("orders-pipeline-v1", 2, 5L, producerFactory);

        writer.write(record(1L), CONTEXT);
        writer.prepareCommit();
        writer.write(record(2L), CONTEXT);
        writer.prepareCommit();

        assertThat(producerFactory.transactionalIds)
                .containsExactly(
                        "orders-pipeline-v1-share-ack-2-5",
                        "orders-pipeline-v1-share-ack-2-6");
    }

    @Test
    void testCloseAbortsOpenTransaction() throws Exception {
        RecordingProducerFactory producerFactory = new RecordingProducerFactory();
        KafkaShareAckSinkWriter writer =
                new KafkaShareAckSinkWriter("orders-pipeline-v1", 2, 5L, producerFactory);

        writer.write(record(1L), CONTEXT);
        writer.close();

        assertThat(producerFactory.producers.get(0).events)
                .containsExactly(
                        "begin", "stage:group|topic-id|orders|0|1", "abort", "close");
    }

    private static ShareAckRecord record(long offset) {
        return new ShareAckRecord(id(offset), "member", 7, ShareAckDecision.ACCEPT);
    }

    private static ShareAckId id(long offset) {
        return new ShareAckId("group", "topic-id", "orders", 0, offset);
    }

    private static final class RecordingProducerFactory
            implements KafkaShareAckSinkWriter.ProducerFactory {

        private final List<String> transactionalIds = new ArrayList<>();
        private final List<RecordingProducer> producers = new ArrayList<>();

        @Override
        public ShareAckTransactionalProducer create(String transactionalId) {
            transactionalIds.add(transactionalId);
            RecordingProducer producer = new RecordingProducer(transactionalId);
            producers.add(producer);
            return producer;
        }
    }

    private static final class RecordingProducer implements ShareAckTransactionalProducer {
        private final String transactionalId;
        private final List<String> events = new ArrayList<>();

        private RecordingProducer(String transactionalId) {
            this.transactionalId = transactionalId;
        }

        @Override
        public void beginTransaction() {
            events.add("begin");
        }

        @Override
        public void stage(ShareAckPayload payload) throws IOException {
            events.add("stage:" + payload.getId());
        }

        @Override
        public Optional<String> prepareTransaction() {
            events.add("prepare");
            return Optional.of(transactionalId + ":prepared");
        }

        @Override
        public void abortTransaction() {
            events.add("abort");
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
        public short getProducerEpoch() {
            return 3;
        }

        @Override
        public void close() {
            events.add("close");
        }
    }
}
