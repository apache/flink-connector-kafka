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

import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShareAckModelTest {

    @Test
    void testShareAckRecordBuildsSingleRecordPayload() {
        ShareAckId id = new ShareAckId("group", "topic-id", "orders", 2, 42L);
        ShareAckRecord record =
                new ShareAckRecord(id, "member", 7, ShareAckDecision.REJECT);

        ShareAckPayload payload = record.toPayload();

        assertThat(payload.getId()).isEqualTo("group|topic-id|orders|2|42");
        assertThat(payload.getGroupId()).isEqualTo("group");
        assertThat(payload.getMemberId()).isEqualTo("member");
        assertThat(payload.getMemberEpoch()).isEqualTo(7);
        assertThat(payload.getAcknowledgements()).hasSize(1);
        ShareAckPayload.TopicPartitionAcknowledgements partition =
                payload.getAcknowledgements().get(0);
        assertThat(partition.getTopicId()).isEqualTo("topic-id");
        assertThat(partition.getTopic()).isEqualTo("orders");
        assertThat(partition.getPartition()).isEqualTo(2);
        assertThat(partition.getBatches()).hasSize(1);
        ShareAckPayload.AcknowledgementBatch batch = partition.getBatches().get(0);
        assertThat(batch.getFirstOffset()).isEqualTo(42L);
        assertThat(batch.getLastOffset()).isEqualTo(42L);
        assertThat(batch.getAcknowledgeTypes())
                .containsExactly(ShareAckDecision.REJECT.kafkaTypeId());
    }

    @Test
    void testShareAckDecisionRejectsUnsupportedTransactionalTypes() {
        assertThat(ShareAckDecision.fromKafkaTypeId((byte) 1)).isEqualTo(ShareAckDecision.ACCEPT);
        assertThat(ShareAckDecision.fromKafkaTypeId((byte) 3)).isEqualTo(ShareAckDecision.REJECT);

        assertThatThrownBy(() -> ShareAckDecision.fromKafkaTypeId((byte) 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported transactional share acknowledgement type id");
    }

    @Test
    void testShareAckCommittableSerializerRoundTrips() throws Exception {
        ShareAckCommittable committable =
                new ShareAckCommittable(
                        "orders-pipeline-v1",
                        42L,
                        "orders-pipeline-v1-share-ack-0-42",
                        10L,
                        (short) 3,
                        "10:3",
                        List.of(
                                new ShareAckId("group", "topic-id", "orders", 0, 1L),
                                new ShareAckId("group", "topic-id", "orders", 0, 2L)));
        ShareAckCommittableSerializer serializer = new ShareAckCommittableSerializer();

        ShareAckCommittable copy =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(committable));

        assertThat(copy).isEqualTo(committable);
    }

    @Test
    void testShareEosCheckpointLedgerAdvancesPhaseImmutably() {
        KafkaCommittable output =
                new KafkaCommittable(1L, (short) 2, "output-txn", "1:2", null);
        ShareAckCommittable ack =
                new ShareAckCommittable(
                        "orders-pipeline-v1",
                        42L,
                        "share-ack-txn",
                        3L,
                        (short) 4,
                        "3:4",
                        List.of(new ShareAckId("group", "topic-id", "orders", 0, 1L)));
        ShareEosCheckpointLedger ledger =
                new ShareEosCheckpointLedger(
                        "orders-pipeline-v1",
                        42L,
                        ShareAckCommitPhase.COMMITTING_OUTPUTS,
                        List.of(output),
                        List.of(ack));

        ShareEosCheckpointLedger outputsCommitted =
                ledger.withPhase(ShareAckCommitPhase.OUTPUTS_COMMITTED);

        assertThat(ledger.outputsCommitted()).isFalse();
        assertThat(outputsCommitted.outputsCommitted()).isTrue();
        assertThat(outputsCommitted.getOutputCommittables()).containsExactly(output);
        assertThat(outputsCommitted.getShareAckCommittables()).containsExactly(ack);
    }
}
