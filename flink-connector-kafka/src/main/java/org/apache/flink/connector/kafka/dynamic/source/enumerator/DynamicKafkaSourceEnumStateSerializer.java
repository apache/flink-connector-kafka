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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.CommonClientConfigs;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** (De)serializer for {@link DynamicKafkaSourceEnumState}. */
@Internal
public class DynamicKafkaSourceEnumStateSerializer
        implements SimpleVersionedSerializer<DynamicKafkaSourceEnumState> {

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;

    private final KafkaSourceEnumStateSerializer kafkaSourceEnumStateSerializer;

    public DynamicKafkaSourceEnumStateSerializer() {
        this.kafkaSourceEnumStateSerializer = new KafkaSourceEnumStateSerializer();
    }

    @Override
    public int getVersion() {
        return VERSION_2;
    }

    @Override
    public byte[] serialize(DynamicKafkaSourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            Set<KafkaStream> kafkaStreams = state.getKafkaStreams();
            serializeV2(kafkaStreams, out);

            Map<String, KafkaSourceEnumState> clusterEnumeratorStates =
                    state.getClusterEnumeratorStates();
            out.writeInt(kafkaSourceEnumStateSerializer.getVersion());

            // write sub enumerator states
            out.writeInt(clusterEnumeratorStates.size());
            for (Map.Entry<String, KafkaSourceEnumState> clusterEnumeratorState :
                    clusterEnumeratorStates.entrySet()) {
                String kafkaClusterId = clusterEnumeratorState.getKey();
                out.writeUTF(kafkaClusterId);
                byte[] bytes =
                        kafkaSourceEnumStateSerializer.serialize(clusterEnumeratorState.getValue());
                // we need to know the exact size of the byte array since
                // KafkaSourceEnumStateSerializer
                // will throw exception if there are leftover unread bytes in deserialization.
                out.writeInt(bytes.length);
                out.write(bytes);
            }

            return baos.toByteArray();
        }
    }

    @Override
    public DynamicKafkaSourceEnumState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION_1 && version != VERSION_2) {
            throw new IOException(
                    String.format(
                            "The bytes are serialized with version %d, "
                                    + "while this deserializer only supports version up to %d",
                            version, getVersion()));
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            Set<KafkaStream> kafkaStreams =
                    version == VERSION_1 ? deserializeV1(in) : deserializeV2(in);

            Map<String, KafkaSourceEnumState> clusterEnumeratorStates = new HashMap<>();
            int kafkaSourceEnumStateSerializerVersion = in.readInt();

            int clusterEnumeratorStateMapSize = in.readInt();
            for (int i = 0; i < clusterEnumeratorStateMapSize; i++) {
                String kafkaClusterId = in.readUTF();
                int byteArraySize = in.readInt();
                KafkaSourceEnumState kafkaSourceEnumState =
                        kafkaSourceEnumStateSerializer.deserialize(
                                kafkaSourceEnumStateSerializerVersion,
                                readNBytes(in, byteArraySize));
                clusterEnumeratorStates.put(kafkaClusterId, kafkaSourceEnumState);
            }

            return new DynamicKafkaSourceEnumState(kafkaStreams, clusterEnumeratorStates);
        }
    }

    private void serializeV2(Set<KafkaStream> kafkaStreams, DataOutputStream out)
            throws IOException {
        out.writeInt(kafkaStreams.size());
        for (KafkaStream kafkaStream : kafkaStreams) {
            out.writeUTF(kafkaStream.getStreamId());
            Map<String, ClusterMetadata> clusterMetadataMap = kafkaStream.getClusterMetadataMap();
            out.writeInt(clusterMetadataMap.size());
            for (Map.Entry<String, ClusterMetadata> entry : clusterMetadataMap.entrySet()) {
                String kafkaClusterId = entry.getKey();
                ClusterMetadata clusterMetadata = entry.getValue();
                out.writeUTF(kafkaClusterId);
                out.writeInt(clusterMetadata.getTopics().size());
                for (String topic : clusterMetadata.getTopics()) {
                    out.writeUTF(topic);
                }

                // only write bootstrap server for now, can extend later to serialize the complete
                // properties
                out.writeUTF(
                        Preconditions.checkNotNull(
                                clusterMetadata
                                        .getProperties()
                                        .getProperty(
                                                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                                "Bootstrap servers must be specified in properties")));
                writeOffsetsInitializer(clusterMetadata.getStartingOffsetsInitializer(), out);
                writeOffsetsInitializer(clusterMetadata.getStoppingOffsetsInitializer(), out);
            }
        }
    }

    private Set<KafkaStream> deserializeV1(DataInputStream in) throws IOException {

        Set<KafkaStream> kafkaStreams = new HashSet<>();
        int numStreams = in.readInt();
        for (int i = 0; i < numStreams; i++) {
            String streamId = in.readUTF();
            Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();
            int clusterMetadataMapSize = in.readInt();
            for (int j = 0; j < clusterMetadataMapSize; j++) {
                String kafkaClusterId = in.readUTF();
                int topicsSize = in.readInt();
                Set<String> topics = new HashSet<>();
                for (int k = 0; k < topicsSize; k++) {
                    topics.add(in.readUTF());
                }

                String bootstrapServers = in.readUTF();
                Properties properties = new Properties();
                properties.setProperty(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

                clusterMetadataMap.put(kafkaClusterId, new ClusterMetadata(topics, properties));
            }

            kafkaStreams.add(new KafkaStream(streamId, clusterMetadataMap));
        }

        return kafkaStreams;
    }

    private Set<KafkaStream> deserializeV2(DataInputStream in) throws IOException {
        Set<KafkaStream> kafkaStreams = new HashSet<>();
        int numStreams = in.readInt();
        for (int i = 0; i < numStreams; i++) {
            String streamId = in.readUTF();
            Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();
            int clusterMetadataMapSize = in.readInt();
            for (int j = 0; j < clusterMetadataMapSize; j++) {
                String kafkaClusterId = in.readUTF();
                int topicsSize = in.readInt();
                Set<String> topics = new HashSet<>();
                for (int k = 0; k < topicsSize; k++) {
                    topics.add(in.readUTF());
                }

                String bootstrapServers = in.readUTF();
                Properties properties = new Properties();
                properties.setProperty(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

                OffsetsInitializer startingOffsetsInitializer = readOffsetsInitializer(in);
                OffsetsInitializer stoppingOffsetsInitializer = readOffsetsInitializer(in);

                clusterMetadataMap.put(
                        kafkaClusterId,
                        new ClusterMetadata(
                                topics,
                                properties,
                                startingOffsetsInitializer,
                                stoppingOffsetsInitializer));
            }

            kafkaStreams.add(new KafkaStream(streamId, clusterMetadataMap));
        }

        return kafkaStreams;
    }

    private static void writeOffsetsInitializer(
            @Nullable OffsetsInitializer offsetsInitializer, DataOutputStream out)
            throws IOException {
        if (offsetsInitializer == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        byte[] serializedOffsets = InstantiationUtil.serializeObject(offsetsInitializer);
        out.writeInt(serializedOffsets.length);
        out.write(serializedOffsets);
    }

    @Nullable
    private static OffsetsInitializer readOffsetsInitializer(DataInputStream in) throws IOException {
        boolean hasOffsetsInitializer = in.readBoolean();
        if (!hasOffsetsInitializer) {
            return null;
        }

        int serializedSize = in.readInt();
        byte[] serializedOffsets = readNBytes(in, serializedSize);
        try {
            return InstantiationUtil.deserializeObject(
                    serializedOffsets, getClassLoaderForOffsets());
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize OffsetsInitializer", e);
        }
    }

    private static ClassLoader getClassLoaderForOffsets() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader != null
                ? classLoader
                : DynamicKafkaSourceEnumStateSerializer.class.getClassLoader();
    }

    private static byte[] readNBytes(DataInputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }
}
