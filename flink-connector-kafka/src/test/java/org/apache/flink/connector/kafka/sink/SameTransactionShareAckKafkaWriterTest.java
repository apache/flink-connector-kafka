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
    void testStagesShareAcksIntoTransactionAtWrite() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingDelegate delegate = new RecordingDelegate(events);
        RecordingStager stager = new RecordingStager(events);
        SameTransactionShareAckKafkaWriter<String> writer =
                new SameTransactionShareAckKafkaWriter<>(
                        delegate,
                        ignored -> List.of(payload("ack-0")),
                        new ShareAckPayloadBuffer(),
                        stager);

        writer.write("record", null);
        Collection<KafkaCommittable> committables = writer.prepareCommit();

        assertThat(committables).containsExactly(RecordingDelegate.COMMITTABLE);
        // Staging happens at write() (before prepareCommit), and marks the producer.
        assertThat(events)
                .containsExactly(
                        "delegate-write:record",
                        "stage:ack-0:producer",
                        "delegate-mark",
                        "delegate-prepare");
    }

    @Test
    void testStagesEachPayloadOncePerTransaction() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingDelegate delegate = new RecordingDelegate(events);
        RecordingStager stager = new RecordingStager(events);
        SameTransactionShareAckKafkaWriter<String> writer =
                new SameTransactionShareAckKafkaWriter<>(
                        delegate,
                        ignored -> List.of(payload("ack-0")),
                        new ShareAckPayloadBuffer(),
                        stager);

        // Two records carrying the same ack payload id stage it only once.
        writer.write("r1", null);
        writer.write("r2", null);

        assertThat(stager.stagedIds).containsExactly("ack-0");
    }

    @Test
    void testAbortsWholeTransactionWhenStageFails() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingDelegate delegate = new RecordingDelegate(events);
        ShareAckStagerFailure stager = new ShareAckStagerFailure();
        SameTransactionShareAckKafkaWriter<String> writer =
                new SameTransactionShareAckKafkaWriter<>(
                        delegate,
                        ignored -> List.of(payload("ack-0")),
                        new ShareAckPayloadBuffer(),
                        stager);

        // A failed stage must surface from write() so the checkpoint fails and the transaction is
        // aborted wholesale; no committable may be produced for this window.
        assertThatThrownBy(() -> writer.write("record", null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("stage boom");
        assertThat(delegate.prepareCalls).isZero();
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

    private static final class RecordingStager
            implements SameTransactionShareAckKafkaWriter.ShareAckStager {
        private final List<String> events;
        private final List<String> stagedIds = new ArrayList<>();

        private RecordingStager(List<String> events) {
            this.events = events;
        }

        @Override
        public void stage(Object producer, ShareAckPayload payload) {
            stagedIds.add(payload.getId());
            events.add("stage:" + payload.getId() + ":" + producer);
        }
    }

    private static final class ShareAckStagerFailure
            implements SameTransactionShareAckKafkaWriter.ShareAckStager {
        @Override
        public void stage(Object producer, ShareAckPayload payload) throws IOException {
            throw new IOException("stage boom");
        }
    }

    private static final class RecordingDelegate
            implements SameTransactionShareAckKafkaWriter.SameTransactionWriterDelegate<String> {

        private static final KafkaCommittable COMMITTABLE =
                new KafkaCommittable(1L, (short) 0, "txn", null);

        private final List<String> events;
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
        public void markShareAcksStaged() {
            events.add("delegate-mark");
        }

        @Override
        public void write(String element, SinkWriter.Context context) {
            events.add("delegate-write:" + element);
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public Collection<KafkaCommittable> prepareCommit() {
            prepareCalls++;
            events.add("delegate-prepare");
            return List.of(COMMITTABLE);
        }

        @Override
        public List<KafkaWriterState> snapshotState(long checkpointId) {
            return List.of();
        }

        @Override
        public void close() {}
    }
}
