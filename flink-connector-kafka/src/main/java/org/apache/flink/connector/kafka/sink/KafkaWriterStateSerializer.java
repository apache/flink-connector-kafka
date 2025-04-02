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

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * KafkaWriterState}.
 *
 * <p>The serializer has a version (returned by {@link #getVersion()}) which can be attached to the
 * serialized data. When the serializer evolves, the version can be used to identify with which
 * prior version the data was serialized. Currently, this serializer supported versions are:
 *
 * <ol>
 *   <li>1
 * </ol>
 *
 * <p>Other versions of the serialized data will fail to deserialize and throw an exception.
 */
class KafkaWriterStateSerializer implements SimpleVersionedSerializer<KafkaWriterState> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(KafkaWriterState state) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(state.getTransactionalIdPrefix());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaWriterState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private KafkaWriterState deserializeV1(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final String transactionalIdPrefx = in.readUTF();
            return new KafkaWriterState(transactionalIdPrefx);
        }
    }
}
