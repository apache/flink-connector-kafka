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

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test for {@link
 * org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplitSerializer}.
 */
public class DynamicKafkaSourceSplitSerializerTest {

    @Test
    public void testSerde() throws IOException {
        DynamicKafkaSourceSplitSerializer serializer = new DynamicKafkaSourceSplitSerializer();
        DynamicKafkaSourceSplit dynamicKafkaSourceSplit =
                new DynamicKafkaSourceSplit(
                        "test-cluster",
                        new KafkaPartitionSplit(new TopicPartition("test-topic", 3), 1));
        DynamicKafkaSourceSplit dynamicKafkaSourceSplitAfterSerde =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(dynamicKafkaSourceSplit));
        assertEquals(dynamicKafkaSourceSplit, dynamicKafkaSourceSplitAfterSerde);
    }

    @Test
    public void testSerdeRetainedSplit() throws IOException {
        DynamicKafkaSourceSplitSerializer serializer = new DynamicKafkaSourceSplitSerializer();
        DynamicKafkaSourceSplit retainedSplit =
                new DynamicKafkaSourceSplit(
                        "test-cluster",
                        new KafkaPartitionSplit(new TopicPartition("test-topic", 3), 1),
                        123L);

        DynamicKafkaSourceSplit retainedSplitAfterSerde =
                serializer.deserialize(
                        serializer.getVersion(), serializer.serialize(retainedSplit));

        assertEquals(retainedSplit, retainedSplitAfterSerde);
    }

    @Test
    public void testDeserializeV1State() throws IOException {
        DynamicKafkaSourceSplitSerializer serializer = new DynamicKafkaSourceSplitSerializer();
        DynamicKafkaSourceSplit dynamicKafkaSourceSplit =
                serializer.deserialize(1, serializeV1State());

        assertEquals(
                new DynamicKafkaSourceSplit(
                        "test-cluster",
                        new KafkaPartitionSplit(new TopicPartition("test-topic", 3), 1)),
                dynamicKafkaSourceSplit);
    }

    private static byte[] serializeV1State() throws IOException {
        KafkaPartitionSplitSerializer kafkaPartitionSplitSerializer =
                new KafkaPartitionSplitSerializer();
        KafkaPartitionSplit kafkaPartitionSplit =
                new KafkaPartitionSplit(new TopicPartition("test-topic", 3), 1);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF("test-cluster");
            out.writeInt(kafkaPartitionSplitSerializer.getVersion());
            out.write(kafkaPartitionSplitSerializer.serialize(kafkaPartitionSplit));
            return baos.toByteArray();
        }
    }
}
