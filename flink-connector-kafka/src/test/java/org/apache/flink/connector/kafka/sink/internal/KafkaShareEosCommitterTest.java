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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaShareEosCommittable;
import org.apache.flink.connector.kafka.sink.ShareAckCommittable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaShareEosCommitterTest {

    @Test
    void testCommitsKafkaSinkBeforeShareAcks() throws Exception {
        List<String> commits = new ArrayList<>();
        KafkaShareEosCommitter committer =
                new KafkaShareEosCommitter(
                        committables -> recordKafkaCommit(commits, committables.iterator().next()),
                        committables ->
                                recordShareAckCommit(commits, committables.iterator().next()));

        RecordingCommitRequest request =
                new RecordingCommitRequest(
                        KafkaShareEosCommittable.ready(
                                42L, List.of(kafkaCommittable()), List.of(shareAckCommittable())));

        committer.commit(List.of(request));

        assertThat(commits).containsExactly("sink:sink-txn", "share:share-txn");
        assertThat(request.retryCount).isZero();
        assertThat(request.updatedCommittable).isNull();
    }

    @Test
    void testDoesNotCommitShareAcksWhenSinkCommitRetries() throws Exception {
        List<String> commits = new ArrayList<>();
        KafkaShareEosCommitter committer =
                new KafkaShareEosCommitter(
                        committables -> {
                            commits.add("sink");
                            throw new IOException("sink unavailable");
                        },
                        committables -> commits.add("share"));

        RecordingCommitRequest request =
                new RecordingCommitRequest(
                        KafkaShareEosCommittable.ready(
                                42L, List.of(kafkaCommittable()), List.of(shareAckCommittable())));

        committer.commit(List.of(request));

        assertThat(commits).containsExactly("sink");
        assertThat(request.retryCount).isOne();
        assertThat(request.updatedCommittable).isNull();
    }

    @Test
    void testShareAckRetryRemembersSinkWasCommitted() throws Exception {
        List<String> commits = new ArrayList<>();
        KafkaShareEosCommitter firstAttempt =
                new KafkaShareEosCommitter(
                        committables -> commits.add("sink"),
                        committables -> {
                            commits.add("share");
                            throw new IOException("share ack unavailable");
                        });
        RecordingCommitRequest firstRequest =
                new RecordingCommitRequest(
                        KafkaShareEosCommittable.ready(
                                42L, List.of(kafkaCommittable()), List.of(shareAckCommittable())));

        firstAttempt.commit(List.of(firstRequest));

        assertThat(firstRequest.retryCount).isOne();
        assertThat(firstRequest.updatedCommittable.getCommitPhase())
                .isEqualTo(KafkaShareEosCommittable.CommitPhase.SINK_COMMITTED);

        KafkaShareEosCommitter secondAttempt =
                new KafkaShareEosCommitter(
                        committables -> commits.add("sink-retry"),
                        committables -> commits.add("share-retry"));
        RecordingCommitRequest secondRequest =
                new RecordingCommitRequest(firstRequest.updatedCommittable);

        secondAttempt.commit(List.of(secondRequest));

        assertThat(commits).containsExactly("sink", "share", "share-retry");
        assertThat(secondRequest.retryCount).isZero();
    }

    private static KafkaCommittable kafkaCommittable() {
        return new KafkaCommittable(1L, (short) 2, "sink-txn", null);
    }

    private static ShareAckCommittable shareAckCommittable() {
        return new ShareAckCommittable(42L, "share-txn", 3L, (short) 4, "share-group", 5);
    }

    private static void recordKafkaCommit(List<String> commits, KafkaCommittable committable) {
        commits.add("sink:" + committable.getTransactionalId());
    }

    private static void recordShareAckCommit(
            List<String> commits, ShareAckCommittable committable) {
        commits.add("share:" + committable.getTransactionalId());
    }

    private static class RecordingCommitRequest
            implements Committer.CommitRequest<KafkaShareEosCommittable> {

        private final KafkaShareEosCommittable committable;
        private int retryCount;
        private KafkaShareEosCommittable updatedCommittable;

        private RecordingCommitRequest(KafkaShareEosCommittable committable) {
            this.committable = committable;
        }

        @Override
        public KafkaShareEosCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return retryCount;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {}

        @Override
        public void retryLater() {
            retryCount++;
        }

        @Override
        public void updateAndRetryLater(KafkaShareEosCommittable committable) {
            retryCount++;
            updatedCommittable = committable;
        }

        @Override
        public void signalAlreadyCommitted() {}
    }
}
