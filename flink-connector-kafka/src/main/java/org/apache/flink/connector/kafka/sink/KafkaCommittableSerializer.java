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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class KafkaCommittableSerializer implements SimpleVersionedSerializer<KafkaCommittable> {

    private static final int VERSION_WITH_PREPARED_TRANSACTION_STATE = 2;

    @Override
    public int getVersion() {
        return VERSION_WITH_PREPARED_TRANSACTION_STATE;
    }

    @Override
    public byte[] serialize(KafkaCommittable state) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeShort(state.getEpoch());
            out.writeLong(state.getProducerId());
            out.writeUTF(state.getTransactionalId());
            out.writeBoolean(state.getPreparedTransactionState().isPresent());
            if (state.getPreparedTransactionState().isPresent()) {
                out.writeUTF(state.getPreparedTransactionState().get());
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version < 1 || version > getVersion()) {
            throw new IOException("Unknown version: " + version);
        }

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final short epoch = in.readShort();
            final long producerId = in.readLong();
            final String transactionalId = in.readUTF();
            final String preparedTransactionState =
                    version >= VERSION_WITH_PREPARED_TRANSACTION_STATE && in.readBoolean()
                            ? in.readUTF()
                            : null;
            return new KafkaCommittable(
                    producerId, epoch, transactionalId, preparedTransactionState, null);
        }
    }
}
