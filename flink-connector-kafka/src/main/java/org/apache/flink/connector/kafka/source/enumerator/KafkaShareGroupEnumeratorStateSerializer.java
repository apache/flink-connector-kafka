/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Serializer for KafkaShareGroupEnumeratorState.
 *
 * <p>This serializer handles the serialization and deserialization of share group enumerator state
 * for checkpointing and recovery purposes.
 */
public class KafkaShareGroupEnumeratorStateSerializer
        implements SimpleVersionedSerializer<KafkaShareGroupEnumeratorState> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KafkaShareGroupEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            // Serialize share group ID
            out.writeUTF(state.getShareGroupId());

            // Serialize topics
            Set<String> topics = state.getTopics();
            out.writeInt(topics.size());
            for (String topic : topics) {
                out.writeUTF(topic);
            }

            return baos.toByteArray();
        }
    }

    @Override
    public KafkaShareGroupEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            // Deserialize share group ID
            String shareGroupId = in.readUTF();

            // Deserialize topics
            int topicCount = in.readInt();
            Set<String> topics = new HashSet<>();
            for (int i = 0; i < topicCount; i++) {
                topics.add(in.readUTF());
            }

            return new KafkaShareGroupEnumeratorState(topics, shareGroupId);
        }
    }
}
