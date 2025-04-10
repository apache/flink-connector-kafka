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

package org.apache.flink.connector.kafka.dynamic.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** (De)serializes the {@link DynamicKafkaSourceSplit}. */
@Internal
public class DynamicKafkaSourceSplitSerializer
        implements SimpleVersionedSerializer<DynamicKafkaSourceSplit> {

    private static final int VERSION_1 = 1;

    private final KafkaPartitionSplitSerializer kafkaPartitionSplitSerializer;

    public DynamicKafkaSourceSplitSerializer() {
        this.kafkaPartitionSplitSerializer = new KafkaPartitionSplitSerializer();
    }

    @Override
    public int getVersion() {
        return VERSION_1;
    }

    @Override
    public byte[] serialize(DynamicKafkaSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getKafkaClusterId());
            out.writeInt(kafkaPartitionSplitSerializer.getVersion());
            out.write(kafkaPartitionSplitSerializer.serialize(split.getKafkaPartitionSplit()));
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DynamicKafkaSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String kafkaClusterId = in.readUTF();
            int kafkaPartitionSplitSerializerVersion = in.readInt();
            KafkaPartitionSplit kafkaPartitionSplit =
                    kafkaPartitionSplitSerializer.deserialize(
                            kafkaPartitionSplitSerializerVersion, in.readAllBytes());
            return new DynamicKafkaSourceSplit(kafkaClusterId, kafkaPartitionSplit);
        }
    }
}
