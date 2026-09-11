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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ShareAckCommitterPolicyTest {

    @Test
    void testAtLeastOnceCommitterCommitsShareAcksOnly() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingShareAckCommitter shareAckCommitter =
                new RecordingShareAckCommitter(events);
        AtLeastOnceShareAckCommitter committer =
                new AtLeastOnceShareAckCommitter(shareAckCommitter);
        TestingCommitRequest<ShareAckCommittable> request =
                new TestingCommitRequest<>(shareAckCommittable("ack-txn"));

        committer.commit(List.of(request));

        assertThat(events).containsExactly("ack:ack-txn");
        assertThat(request.retryLater).isFalse();
        assertThat(request.failedUnknown).isNull();
    }

    @Test
    void testAtLeastOnceCommitterRetriesRetriableShareAckFailure() throws Exception {
        RecordingShareAckCommitter shareAckCommitter =
                new RecordingShareAckCommitter(new ArrayList<>());
        shareAckCommitter.failure = new TimeoutException("retry");
        AtLeastOnceShareAckCommitter committer =
                new AtLeastOnceShareAckCommitter(shareAckCommitter);
        TestingCommitRequest<ShareAckCommittable> request =
                new TestingCommitRequest<>(shareAckCommittable("ack-txn"));

        committer.commit(List.of(request));

        assertThat(request.retryLater).isTrue();
        assertThat(request.failedUnknown).isNull();
    }

    @Test
    void testOrderedCommitterCommitsOutputsBeforeShareAcks() throws Exception {
        List<String> events = new ArrayList<>();
        OrderedShareEosCommitter committer =
                new OrderedShareEosCommitter(
                        new RecordingOutputCommitter(events),
                        new RecordingShareAckCommitter(events));
        TestingCommitRequest<ShareEosCheckpointLedger> request =
                new TestingCommitRequest<>(
                        ledger(
                                ShareAckCommitPhase.COLLECTED,
                                List.of(outputCommittable("out-a"), outputCommittable("out-b")),
                                List.of(shareAckCommittable("ack-txn"))));

        committer.commit(List.of(request));

        assertThat(events).containsExactly("output:out-a", "output:out-b", "ack:ack-txn");
        assertThat(request.retryLater).isFalse();
        assertThat(request.updatedCommittable).isNull();
        assertThat(request.failedUnknown).isNull();
    }

    @Test
    void testOrderedCommitterDoesNotCommitShareAcksWhenOutputCommitFails() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingOutputCommitter outputCommitter = new RecordingOutputCommitter(events);
        outputCommitter.failureTransactionalId = "out-b";
        OrderedShareEosCommitter committer =
                new OrderedShareEosCommitter(
                        outputCommitter, new RecordingShareAckCommitter(events));
        TestingCommitRequest<ShareEosCheckpointLedger> request =
                new TestingCommitRequest<>(
                        ledger(
                                ShareAckCommitPhase.COLLECTED,
                                List.of(outputCommittable("out-a"), outputCommittable("out-b")),
                                List.of(shareAckCommittable("ack-txn"))));

        committer.commit(List.of(request));

        assertThat(events).containsExactly("output:out-a", "output:out-b");
        assertThat(request.updatedCommittable.getPhase())
                .isEqualTo(ShareAckCommitPhase.COMMITTING_OUTPUTS);
        assertThat(request.retryLater).isTrue();
    }

    @Test
    void testOrderedCommitterRetriesShareAcksAfterOutputsAreCommitted() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingShareAckCommitter shareAckCommitter =
                new RecordingShareAckCommitter(events);
        shareAckCommitter.failure = new TimeoutException("retry ack");
        OrderedShareEosCommitter committer =
                new OrderedShareEosCommitter(
                        new RecordingOutputCommitter(events), shareAckCommitter);
        TestingCommitRequest<ShareEosCheckpointLedger> request =
                new TestingCommitRequest<>(
                        ledger(
                                ShareAckCommitPhase.COLLECTED,
                                List.of(outputCommittable("out-a")),
                                List.of(shareAckCommittable("ack-txn"))));

        committer.commit(List.of(request));

        assertThat(events).containsExactly("output:out-a", "ack:ack-txn");
        assertThat(request.updatedCommittable.getPhase())
                .isEqualTo(ShareAckCommitPhase.COMMITTING_SHARE_ACKS);
        assertThat(request.retryLater).isTrue();

        events.clear();
        shareAckCommitter.failure = null;
        TestingCommitRequest<ShareEosCheckpointLedger> retryRequest =
                new TestingCommitRequest<>(request.updatedCommittable);

        committer.commit(List.of(retryRequest));

        assertThat(events).containsExactly("ack:ack-txn");
        assertThat(retryRequest.retryLater).isFalse();
    }

    private static ShareEosCheckpointLedger ledger(
            ShareAckCommitPhase phase,
            Collection<KafkaCommittable> outputs,
            Collection<ShareAckCommittable> acks) {
        return new ShareEosCheckpointLedger(
                "orders-pipeline-v1", 42L, phase, outputs, acks);
    }

    private static KafkaCommittable outputCommittable(String transactionalId) {
        return new KafkaCommittable(1L, (short) 2, transactionalId, "1:2", null);
    }

    private static ShareAckCommittable shareAckCommittable(String transactionalId) {
        return new ShareAckCommittable(
                "orders-pipeline-v1",
                42L,
                transactionalId,
                3L,
                (short) 4,
                "3:4",
                List.of(new ShareAckId("group", "topic-id", "orders", 0, 1L)));
    }

    private static final class RecordingOutputCommitter
            implements KafkaOutputTransactionCommitter {
        private final List<String> events;
        private String failureTransactionalId;

        private RecordingOutputCommitter(List<String> events) {
            this.events = events;
        }

        @Override
        public TransactionCommitResult commit(KafkaCommittable committable) {
            events.add("output:" + committable.getTransactionalId());
            if (committable.getTransactionalId().equals(failureTransactionalId)) {
                throw new TimeoutException("retry output");
            }
            return TransactionCommitResult.COMMITTED;
        }

        @Override
        public void close() {}
    }

    private static final class RecordingShareAckCommitter
            implements ShareAckTransactionCommitter {
        private final List<String> events;
        private RuntimeException failure;

        private RecordingShareAckCommitter(List<String> events) {
            this.events = events;
        }

        @Override
        public TransactionCommitResult commit(ShareAckCommittable committable)
                throws IOException {
            events.add("ack:" + committable.getTransactionalId());
            if (failure != null) {
                throw failure;
            }
            return TransactionCommitResult.COMMITTED;
        }

        @Override
        public void close() {}
    }

    private static final class TestingCommitRequest<CommT>
            implements Committer.CommitRequest<CommT> {
        private final CommT committable;
        private boolean retryLater;
        private boolean alreadyCommitted;
        private CommT updatedCommittable;
        private Throwable failedKnown;
        private Throwable failedUnknown;

        private TestingCommitRequest(CommT committable) {
            this.committable = committable;
        }

        @Override
        public CommT getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            failedKnown = t;
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            failedUnknown = t;
        }

        @Override
        public void retryLater() {
            retryLater = true;
        }

        @Override
        public void updateAndRetryLater(CommT committable) {
            updatedCommittable = committable;
            retryLater = true;
        }

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted = true;
        }
    }
}
