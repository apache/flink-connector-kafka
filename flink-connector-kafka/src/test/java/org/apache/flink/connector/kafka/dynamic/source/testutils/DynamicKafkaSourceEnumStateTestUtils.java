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

package org.apache.flink.connector.kafka.dynamic.source.testutils;

import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

/** Test utilities for DynamicKafkaSource enum state serialization. */
public final class DynamicKafkaSourceEnumStateTestUtils {
    private DynamicKafkaSourceEnumStateTestUtils() {}

    public static byte[] serializeV1State(
            String streamId, String clusterId, Set<String> topics, String bootstrapServers)
            throws IOException {
        KafkaSourceEnumStateSerializer kafkaSourceEnumStateSerializer =
                new KafkaSourceEnumStateSerializer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(1);
            out.writeUTF(streamId);
            out.writeInt(1);
            out.writeUTF(clusterId);
            out.writeInt(topics.size());
            for (String topic : topics) {
                out.writeUTF(topic);
            }
            out.writeUTF(bootstrapServers);
            out.writeInt(kafkaSourceEnumStateSerializer.getVersion());
            out.writeInt(0);
            return baos.toByteArray();
        }
    }

    public static byte[] serializeV2State(
            String streamId,
            String clusterId,
            Set<String> topics,
            String bootstrapServers,
            KafkaSourceEnumState clusterState)
            throws IOException {
        KafkaSourceEnumStateSerializer kafkaSourceEnumStateSerializer =
                new KafkaSourceEnumStateSerializer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            // kafka streams in the V2 layout
            out.writeInt(1);
            out.writeUTF(streamId);
            out.writeInt(1);
            out.writeUTF(clusterId);
            out.writeInt(topics.size());
            for (String topic : topics) {
                out.writeUTF(topic);
            }
            out.writeUTF(bootstrapServers);
            out.writeBoolean(false); // no starting offsets initializer
            out.writeBoolean(false); // no stopping offsets initializer
            out.writeInt(kafkaSourceEnumStateSerializer.getVersion());
            // cluster enumerator states
            out.writeInt(1);
            out.writeUTF(clusterId);
            writeLengthPrefixed(kafkaSourceEnumStateSerializer.serialize(clusterState), out);
            return baos.toByteArray();
        }
    }

    public static byte[] serializeV3State(
            String streamId,
            String clusterId,
            Set<String> topics,
            String bootstrapServers,
            KafkaSourceEnumState clusterState,
            String retainedClusterId,
            long retainedUntilMs,
            KafkaSourceEnumState retainedClusterState)
            throws IOException {
        KafkaSourceEnumStateSerializer kafkaSourceEnumStateSerializer =
                new KafkaSourceEnumStateSerializer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.write(
                    serializeV2State(streamId, clusterId, topics, bootstrapServers, clusterState));
            // retained cluster enumerator states (new in V3)
            out.writeInt(1);
            out.writeUTF(retainedClusterId);
            out.writeLong(retainedUntilMs);
            writeLengthPrefixed(
                    kafkaSourceEnumStateSerializer.serialize(retainedClusterState), out);
            return baos.toByteArray();
        }
    }

    private static void writeLengthPrefixed(byte[] bytes, DataOutputStream out) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }
}
