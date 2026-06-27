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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.connector.kafka.sink.internal.KafkaShareEosCommitter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaShareEosRecoveredCommittableTest {

    private static final KafkaShareEosCommittableSerializer SERIALIZER =
            new KafkaShareEosCommittableSerializer();

    @Test
    void testRecoveredPreparedCommittableCommitsSinkBeforeShareAcks() throws Exception {
        KafkaShareEosCommittable checkpointed =
                KafkaShareEosCommittable.ready(
                        42L,
                        List.of(
                                new KafkaCommittable(
                                        1L, (short) 2, "sink-txn", "sink-prepared", null)),
                        List.of(
                                new ShareAckCommittable(
                                        42L,
                                        "share-txn",
                                        3L,
                                        (short) 4,
                                        "share-prepared",
                                        "share-group",
                                        5)));

        KafkaShareEosCommittable recovered =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), SERIALIZER.serialize(checkpointed));

        assertThat(recovered).isEqualTo(checkpointed);
        assertThat(recovered.getKafkaCommittables().get(0).getProducer()).isEmpty();
        assertThat(recovered.getKafkaCommittables().get(0).getPreparedTransactionState())
                .contains("sink-prepared");
        assertThat(recovered.getShareAckCommittables().get(0).getPreparedTransactionState())
                .contains("share-prepared");

        List<String> commits = new ArrayList<>();
        KafkaShareEosCommitter committer =
                new KafkaShareEosCommitter(
                        committables -> {
                            KafkaCommittable committable = committables.iterator().next();
                            commits.add(
                                    "sink:"
                                            + committable.getTransactionalId()
                                            + ":"
                                            + committable
                                                    .getPreparedTransactionState()
                                                    .orElse("missing"));
                        },
                        committables -> {
                            ShareAckCommittable committable = committables.iterator().next();
                            commits.add(
                                    "share:"
                                            + committable.getTransactionalId()
                                            + ":"
                                            + committable
                                                    .getPreparedTransactionState()
                                                    .orElse("missing"));
                        });
        RecordingCommitRequest request = new RecordingCommitRequest(recovered);

        committer.commit(List.of(request));

        assertThat(commits)
                .containsExactly("sink:sink-txn:sink-prepared", "share:share-txn:share-prepared");
        assertThat(request.retryCount).isZero();
        assertThat(request.updatedCommittable).isNull();
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
