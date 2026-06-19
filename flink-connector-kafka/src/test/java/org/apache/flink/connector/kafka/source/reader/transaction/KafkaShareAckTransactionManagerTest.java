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

package org.apache.flink.connector.kafka.source.reader.transaction;

import org.apache.flink.connector.kafka.share.ShareAckCommittable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaShareAckTransactionManagerTest {

    @Test
    void testStagesMultiplePollsIntoOneCheckpointTransaction() throws Exception {
        RecordingShareAckTransactionClient client = new RecordingShareAckTransactionClient();
        KafkaShareAckTransactionManager manager =
                new KafkaShareAckTransactionManager(client, "share-group", 7, List.of());

        manager.stageAcknowledgements();
        manager.stageAcknowledgements();

        assertThat(manager.snapshotState(42L))
                .containsExactly(
                        new ShareAckCommittable(
                                42L, "share-txn-0", 100L, (short) 0, "share-group", 7));
        assertThat(client.events)
                .containsExactly(
                        "begin:share-txn-0",
                        "stage:share-txn-0",
                        "stage:share-txn-0",
                        "preCommit:share-txn-0");

        manager.stageAcknowledgements();

        assertThat(client.events)
                .containsExactly(
                        "begin:share-txn-0",
                        "stage:share-txn-0",
                        "stage:share-txn-0",
                        "preCommit:share-txn-0",
                        "begin:share-txn-1",
                        "stage:share-txn-1");
    }

    @Test
    void testSnapshotWithoutAcknowledgementsDoesNotCreateTransaction() throws Exception {
        RecordingShareAckTransactionClient client = new RecordingShareAckTransactionClient();
        KafkaShareAckTransactionManager manager =
                new KafkaShareAckTransactionManager(client, "share-group", 7, List.of());

        assertThat(manager.snapshotState(42L)).isEmpty();
        assertThat(client.events).isEmpty();
    }

    @Test
    void testRestoredPendingCommittablesStayInSnapshotUntilCommitted() throws Exception {
        ShareAckCommittable restored =
                new ShareAckCommittable(41L, "share-txn-restored", 100L, (short) 1, "group", 3);
        KafkaShareAckTransactionManager manager =
                new KafkaShareAckTransactionManager(
                        new RecordingShareAckTransactionClient(), "group", 3, List.of(restored));

        assertThat(manager.snapshotState(42L)).containsExactly(restored);

        manager.markCommittedUpTo(41L);

        assertThat(manager.snapshotState(43L)).isEmpty();
    }

    @Test
    void testPreCommitFailureKeepsActiveTransactionForRetry() throws Exception {
        RecordingShareAckTransactionClient client = new RecordingShareAckTransactionClient();
        client.failNextPreCommit = true;
        KafkaShareAckTransactionManager manager =
                new KafkaShareAckTransactionManager(client, "share-group", 7, List.of());

        manager.stageAcknowledgements();

        assertThatThrownBy(() -> manager.snapshotState(42L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("preCommit failed");

        assertThat(manager.snapshotState(43L))
                .containsExactly(
                        new ShareAckCommittable(
                                43L, "share-txn-0", 100L, (short) 0, "share-group", 7));
        assertThat(client.events)
                .containsExactly(
                        "begin:share-txn-0",
                        "stage:share-txn-0",
                        "preCommit:share-txn-0",
                        "preCommit:share-txn-0");
    }

    private static class RecordingShareAckTransactionClient implements ShareAckTransactionClient {

        private final List<String> events = new ArrayList<>();
        private int nextTransactionIndex;
        private boolean failNextPreCommit;

        @Override
        public ShareAckTransactionHandle beginTransaction() {
            int transactionIndex = nextTransactionIndex++;
            ShareAckTransactionHandle transaction =
                    new ShareAckTransactionHandle(
                            "share-txn-" + transactionIndex,
                            100L,
                            (short) transactionIndex);
            events.add("begin:" + transaction.getTransactionalId());
            return transaction;
        }

        @Override
        public void stageAcknowledgements(ShareAckTransactionHandle transaction) {
            events.add("stage:" + transaction.getTransactionalId());
        }

        @Override
        public void preCommit(ShareAckTransactionHandle transaction) throws IOException {
            events.add("preCommit:" + transaction.getTransactionalId());
            if (failNextPreCommit) {
                failNextPreCommit = false;
                throw new IOException("preCommit failed");
            }
        }
    }
}
