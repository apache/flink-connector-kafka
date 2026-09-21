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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShareAckTransactionWriterTest {

    @Test
    void testPreparesCommittableForStagedAcks() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);

        writer.write(record(1L, ShareAckDecision.ACCEPT));
        writer.write(record(2L, ShareAckDecision.REJECT));
        Optional<ShareAckCommittable> committable = writer.prepareCommit(42L);

        assertThat(committable).isPresent();
        assertThat(committable.get().getAckScopeId()).isEqualTo("orders-pipeline-v1");
        assertThat(committable.get().getCheckpointId()).isEqualTo(42L);
        assertThat(committable.get().getTransactionalId()).isEqualTo("share-ack-txn");
        assertThat(committable.get().getProducerId()).isEqualTo(10L);
        assertThat(committable.get().getProducerEpoch()).isEqualTo((short) 3);
        assertThat(committable.get().getPreparedTransactionState()).contains("10:3");
        assertThat(committable.get().getShareAckIds())
                .containsExactly(id(1L), id(2L));
        assertThat(writer.hasStagedAcks()).isFalse();
        assertThat(producer.events)
                .containsExactly(
                        "begin",
                        "stage:group|topic-id|orders|0|1",
                        "stage:group|topic-id|orders|0|2",
                        "prepare");
    }

    @Test
    void testDeduplicatesSameAckRecord() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);

        ShareAckRecord record = record(1L, ShareAckDecision.ACCEPT);
        writer.write(record);
        writer.write(record);
        Optional<ShareAckCommittable> committable = writer.prepareCommit(42L);

        assertThat(committable).isPresent();
        assertThat(committable.get().getShareAckIds()).containsExactly(id(1L));
        assertThat(producer.events)
                .containsExactly("begin", "stage:group|topic-id|orders|0|1", "prepare");
    }

    @Test
    void testRejectsConflictingAckRecordForSameId() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);

        writer.write(record(1L, ShareAckDecision.ACCEPT));

        assertThatThrownBy(() -> writer.write(record(1L, ShareAckDecision.REJECT)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Conflicting share acknowledgement payload");
        assertThat(producer.events)
                .containsExactly("begin", "stage:group|topic-id|orders|0|1");
    }

    @Test
    void testPrepareWithoutAcksReturnsEmptyCommittable() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);

        assertThat(writer.prepareCommit(42L)).isEmpty();
        assertThat(producer.events).isEmpty();
    }

    @Test
    void testStageFailureDoesNotRememberPayloadAsStaged() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        producer.failStage = true;
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);
        ShareAckRecord record = record(1L, ShareAckDecision.ACCEPT);

        assertThatThrownBy(() -> writer.write(record))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("stage failed");

        producer.failStage = false;
        writer.write(record);
        assertThat(writer.prepareCommit(42L)).isPresent();
        assertThat(producer.events)
                .containsExactly(
                        "begin",
                        "stage:group|topic-id|orders|0|1",
                        "stage:group|topic-id|orders|0|1",
                        "prepare");
    }

    @Test
    void testCloseAbortsStartedTransactionBeforePrepare() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);

        writer.write(record(1L, ShareAckDecision.ACCEPT));
        writer.close();

        assertThat(producer.events)
                .containsExactly(
                        "begin", "stage:group|topic-id|orders|0|1", "abort", "close");
    }

    @Test
    void testCloseDoesNotAbortPreparedTransaction() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        ShareAckTransactionWriter writer =
                new ShareAckTransactionWriter("orders-pipeline-v1", producer);

        writer.write(record(1L, ShareAckDecision.ACCEPT));
        writer.prepareCommit(42L);
        writer.close();

        assertThat(producer.events)
                .containsExactly("begin", "stage:group|topic-id|orders|0|1", "prepare", "close");
    }

    private static ShareAckRecord record(long offset, ShareAckDecision decision) {
        return new ShareAckRecord(id(offset), "member", 7, decision);
    }

    private static ShareAckId id(long offset) {
        return new ShareAckId("group", "topic-id", "orders", 0, offset);
    }

    private static final class RecordingProducer implements ShareAckTransactionalProducer {
        private final List<String> events = new ArrayList<>();
        private boolean failStage;

        @Override
        public void beginTransaction() {
            events.add("begin");
        }

        @Override
        public void stage(ShareAckPayload payload) throws IOException {
            events.add("stage:" + payload.getId());
            if (failStage) {
                throw new IOException("stage failed");
            }
        }

        @Override
        public Optional<String> prepareTransaction() {
            events.add("prepare");
            return Optional.of("10:3");
        }

        @Override
        public void abortTransaction() {
            events.add("abort");
        }

        @Override
        public String getTransactionalId() {
            return "share-ack-txn";
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
