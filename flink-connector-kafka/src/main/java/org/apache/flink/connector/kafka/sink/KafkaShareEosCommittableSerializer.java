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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class KafkaShareEosCommittableSerializer
        implements SimpleVersionedSerializer<KafkaShareEosCommittable> {

    private static final int VERSION_WITH_PREPARED_TRANSACTION_STATE = 2;

    private static final KafkaCommittableSerializer KAFKA_COMMITTABLE_SERIALIZER =
            new KafkaCommittableSerializer();

    @Override
    public int getVersion() {
        return VERSION_WITH_PREPARED_TRANSACTION_STATE;
    }

    @Override
    public byte[] serialize(KafkaShareEosCommittable committable) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(committable.getCheckpointId());
            out.writeInt(committable.getCommitPhase().ordinal());
            out.writeInt(committable.getKafkaCommittables().size());
            for (KafkaCommittable kafkaCommittable : committable.getKafkaCommittables()) {
                byte[] bytes = KAFKA_COMMITTABLE_SERIALIZER.serialize(kafkaCommittable);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            out.writeInt(committable.getShareAckCommittables().size());
            for (ShareAckCommittable shareAckCommittable :
                    committable.getShareAckCommittables()) {
                out.writeLong(shareAckCommittable.getCheckpointId());
                out.writeUTF(shareAckCommittable.getTransactionalId());
                out.writeLong(shareAckCommittable.getTransactionOwnerId());
                out.writeShort(shareAckCommittable.getTransactionOwnerEpoch());
                out.writeBoolean(shareAckCommittable.getPreparedTransactionState().isPresent());
                if (shareAckCommittable.getPreparedTransactionState().isPresent()) {
                    out.writeUTF(shareAckCommittable.getPreparedTransactionState().get());
                }
                out.writeUTF(shareAckCommittable.getGroupId());
                out.writeInt(shareAckCommittable.getSourceSubtaskId());
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaShareEosCommittable deserialize(int version, byte[] serialized)
            throws IOException {
        if (version < 1 || version > getVersion()) {
            throw new IOException("Unknown version: " + version);
        }

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            long checkpointId = in.readLong();
            KafkaShareEosCommittable.CommitPhase phase =
                    KafkaShareEosCommittable.CommitPhase.values()[in.readInt()];
            List<KafkaCommittable> kafkaCommittables = new ArrayList<>();
            int kafkaCommittablesSize = in.readInt();
            for (int i = 0; i < kafkaCommittablesSize; i++) {
                byte[] bytes = new byte[in.readInt()];
                in.readFully(bytes);
                kafkaCommittables.add(
                        KAFKA_COMMITTABLE_SERIALIZER.deserialize(
                                version >= VERSION_WITH_PREPARED_TRANSACTION_STATE
                                        ? KAFKA_COMMITTABLE_SERIALIZER.getVersion()
                                        : 1,
                                bytes));
            }
            List<ShareAckCommittable> shareAckCommittables = new ArrayList<>();
            int shareAckCommittablesSize = in.readInt();
            for (int i = 0; i < shareAckCommittablesSize; i++) {
                long shareAckCheckpointId = in.readLong();
                String transactionalId = in.readUTF();
                long transactionOwnerId = in.readLong();
                short transactionOwnerEpoch = in.readShort();
                String preparedTransactionState =
                        version >= VERSION_WITH_PREPARED_TRANSACTION_STATE && in.readBoolean()
                                ? in.readUTF()
                                : null;
                String groupId = in.readUTF();
                int sourceSubtaskId = in.readInt();
                shareAckCommittables.add(
                        new ShareAckCommittable(
                                shareAckCheckpointId,
                                transactionalId,
                                transactionOwnerId,
                                transactionOwnerEpoch,
                                preparedTransactionState,
                                groupId,
                                sourceSubtaskId));
            }
            return new KafkaShareEosCommittable(
                    checkpointId, kafkaCommittables, shareAckCommittables, phase);
        }
    }
}
