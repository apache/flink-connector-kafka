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

import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.connector.kafka.sink.KafkaWriterState.UNKNOWN;

/** A serializer used to serialize {@link KafkaWriterState}. */
class KafkaWriterStateSerializer implements SimpleVersionedSerializer<KafkaWriterState> {
    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(KafkaWriterState state) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(state.getTransactionalIdPrefix());
            out.writeInt(state.getOwnedSubtaskId());
            out.writeInt(state.getTotalNumberOfOwnedSubtasks());
            out.writeInt(state.getTransactionOwnership().ordinal());
            out.writeInt(state.getPrecommittedTransactionalIds().size());
            for (CheckpointTransaction transaction : state.getPrecommittedTransactionalIds()) {
                out.writeUTF(transaction.getTransactionalId());
                out.writeLong(transaction.getCheckpointId());
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version > 2) {
            throw new IOException("Unknown version: " + version);
        }

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final String transactionalIdPrefix = in.readUTF();
            int ownedSubtaskId = UNKNOWN;
            int totalNumberOfOwnedSubtasks = UNKNOWN;
            TransactionOwnership transactionOwnership = TransactionOwnership.IMPLICIT_BY_SUBTASK_ID;
            final Collection<CheckpointTransaction> precommitted = new ArrayList<>();
            if (version == 2) {
                ownedSubtaskId = in.readInt();
                totalNumberOfOwnedSubtasks = in.readInt();
                transactionOwnership = TransactionOwnership.values()[in.readInt()];

                final int usedTransactionIdsSize = in.readInt();
                for (int i = 0; i < usedTransactionIdsSize; i++) {
                    precommitted.add(new CheckpointTransaction(in.readUTF(), in.readLong()));
                }
            }
            return new KafkaWriterState(
                    transactionalIdPrefix,
                    ownedSubtaskId,
                    totalNumberOfOwnedSubtasks,
                    transactionOwnership,
                    precommitted);
        }
    }
}
