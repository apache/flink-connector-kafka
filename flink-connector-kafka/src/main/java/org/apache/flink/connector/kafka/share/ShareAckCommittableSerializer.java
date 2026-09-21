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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Internal
public class ShareAckCommittableSerializer
        implements SimpleVersionedSerializer<ShareAckCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ShareAckCommittable committable) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(committable.getAckScopeId());
            out.writeLong(committable.getCheckpointId());
            out.writeUTF(committable.getTransactionalId());
            out.writeLong(committable.getProducerId());
            out.writeShort(committable.getProducerEpoch());
            out.writeBoolean(committable.getPreparedTransactionState().isPresent());
            if (committable.getPreparedTransactionState().isPresent()) {
                out.writeUTF(committable.getPreparedTransactionState().get());
            }
            out.writeInt(committable.getShareAckIds().size());
            for (ShareAckId shareAckId : committable.getShareAckIds()) {
                writeShareAckId(out, shareAckId);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public ShareAckCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String ackScopeId = in.readUTF();
            long checkpointId = in.readLong();
            String transactionalId = in.readUTF();
            long producerId = in.readLong();
            short producerEpoch = in.readShort();
            String preparedTransactionState = in.readBoolean() ? in.readUTF() : null;
            int shareAckIdCount = in.readInt();
            List<ShareAckId> shareAckIds = new ArrayList<>(shareAckIdCount);
            for (int i = 0; i < shareAckIdCount; i++) {
                shareAckIds.add(readShareAckId(in));
            }
            return new ShareAckCommittable(
                    ackScopeId,
                    checkpointId,
                    transactionalId,
                    producerId,
                    producerEpoch,
                    preparedTransactionState,
                    shareAckIds);
        }
    }

    private static void writeShareAckId(DataOutputStream out, ShareAckId shareAckId)
            throws IOException {
        out.writeUTF(shareAckId.getShareGroupId());
        out.writeUTF(shareAckId.getTopicId());
        out.writeUTF(shareAckId.getTopic());
        out.writeInt(shareAckId.getPartition());
        out.writeLong(shareAckId.getOffset());
    }

    private static ShareAckId readShareAckId(DataInputStream in) throws IOException {
        return new ShareAckId(
                in.readUTF(),
                in.readUTF(),
                in.readUTF(),
                in.readInt(),
                in.readLong());
    }
}
