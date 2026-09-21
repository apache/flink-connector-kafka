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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.kafka.share.ShareAckPayload;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SameTransactionShareAckKafkaWriterTest {

    @Test
    void testStagesShareAcksBeforePreparingSinkTransaction() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingDelegate delegate = new RecordingDelegate(events);
        RecordingPayloadBuffer payloadBuffer = new RecordingPayloadBuffer(events);
        SameTransactionShareAckKafkaWriter<String> writer =
                new SameTransactionShareAckKafkaWriter<>(
                        delegate, ignored -> List.of(payload("ack-0")), payloadBuffer);

        writer.write("record", null);
        Collection<KafkaCommittable> committables = writer.prepareCommit();

        assertThat(committables).containsExactly(RecordingDelegate.COMMITTABLE);
        assertThat(events)
                .containsExactly(
                        "delegate-write:record",
                        "buffer-add:ack-0",
                        "buffer-stage:true:producer",
                        "delegate-prepare",
                        "buffer-clear");
    }

    @Test
    void testKeepsShareAcksBufferedWhenSinkPrepareFails() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingDelegate delegate = new RecordingDelegate(events);
        RecordingPayloadBuffer payloadBuffer = new RecordingPayloadBuffer(events);
        SameTransactionShareAckKafkaWriter<String> writer =
                new SameTransactionShareAckKafkaWriter<>(
                        delegate, ignored -> List.of(payload("ack-0")), payloadBuffer);
        writer.write("record", null);
        delegate.failPrepare = true;

        assertThatThrownBy(writer::prepareCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("prepare failed");

        assertThat(payloadBuffer.clearCount).isZero();
        assertThat(payloadBuffer.stageCount).isOne();

        delegate.failPrepare = false;
        writer.prepareCommit();

        assertThat(payloadBuffer.stageCount).isEqualTo(2);
        assertThat(payloadBuffer.clearCount).isOne();
    }

    @Test
    void testRejectsShareAcksWithoutSinkRecordsBeforePreparingSinkTransaction() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingDelegate delegate = new RecordingDelegate(events);
        delegate.transactionHasRecords = false;
        RecordingPayloadBuffer payloadBuffer = new RecordingPayloadBuffer(events);
        SameTransactionShareAckKafkaWriter<String> writer =
                new SameTransactionShareAckKafkaWriter<>(
                        delegate, ignored -> List.of(payload("ack-0")), payloadBuffer);
        writer.write("record", null);

        assertThatThrownBy(writer::prepareCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without sink records");

        assertThat(delegate.prepareCalls).isZero();
        assertThat(payloadBuffer.clearCount).isZero();
    }

    private static ShareAckPayload payload(String id) {
        return new ShareAckPayload(
                id,
                "group",
                "member",
                1,
                List.of(
                        new ShareAckPayload.TopicPartitionAcknowledgements(
                                "AAAAAAAAAAAAAAAAAAAAAA",
                                "input",
                                0,
                                List.of(
                                        new ShareAckPayload.AcknowledgementBatch(
                                                0L, 0L, List.of((byte) 1))))));
    }

    private static final class RecordingDelegate
            implements SameTransactionShareAckKafkaWriter.SameTransactionWriterDelegate<String> {

        private static final KafkaCommittable COMMITTABLE =
                new KafkaCommittable(1L, (short) 0, "txn", null);

        private final List<String> events;
        private boolean transactionHasRecords = true;
        private boolean failPrepare;
        private int prepareCalls;

        private RecordingDelegate(List<String> events) {
            this.events = events;
        }

        @Override
        public void initialize() {}

        @Override
        public Object currentProducer() {
            return "producer";
        }

        @Override
        public boolean currentTransactionHasRecords() {
            return transactionHasRecords;
        }

        @Override
        public void write(String element, SinkWriter.Context context) {
            events.add("delegate-write:" + element);
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public Collection<KafkaCommittable> prepareCommit() throws IOException {
            prepareCalls++;
            events.add("delegate-prepare");
            if (failPrepare) {
                throw new IOException("prepare failed");
            }
            return List.of(COMMITTABLE);
        }

        @Override
        public List<KafkaWriterState> snapshotState(long checkpointId) {
            return List.of();
        }

        @Override
        public void close() {}
    }

    private static final class RecordingPayloadBuffer extends ShareAckPayloadBuffer {

        private final List<String> events;
        private final List<ShareAckPayload> buffered = new ArrayList<>();
        private int stageCount;
        private int clearCount;

        private RecordingPayloadBuffer(List<String> events) {
            this.events = events;
        }

        @Override
        void addAll(Collection<ShareAckPayload> payloads) throws IOException {
            super.addAll(payloads);
            buffered.addAll(payloads);
            payloads.forEach(payload -> events.add("buffer-add:" + payload.getId()));
        }

        @Override
        void stage(
                Object producer,
                boolean transactionHasRecords,
                ShareAckPayloadStageFunction stager)
                throws IOException {
            if (buffered.isEmpty()) {
                return;
            }
            if (!transactionHasRecords) {
                throw new IOException(
                        "Cannot commit share acknowledgements without sink records in the same Kafka transaction.");
            }
            stageCount++;
            events.add("buffer-stage:" + transactionHasRecords + ":" + producer);
        }

        @Override
        void clear() {
            super.clear();
            buffered.clear();
            clearCount++;
            events.add("buffer-clear");
        }
    }
}
