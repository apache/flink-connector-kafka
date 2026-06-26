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

import org.apache.flink.connector.kafka.share.ShareAckCommittable;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaShareEosCommittableSerializerTest {

    private static final KafkaShareEosCommittableSerializer SERIALIZER =
            new KafkaShareEosCommittableSerializer();

    @Test
    void testCommittableSerDe() throws IOException {
        KafkaShareEosCommittable committable =
                new KafkaShareEosCommittable(
                        42L,
                        List.of(new KafkaCommittable(1L, (short) 2, "sink-txn", null)),
                        List.of(
                                new ShareAckCommittable(
                                        42L,
                                        "share-txn",
                                        3L,
                                        (short) 4,
                                        "3:4",
                                        "share-group",
                                        5)),
                        KafkaShareEosCommittable.CommitPhase.SINK_COMMITTED);

        byte[] serialized = SERIALIZER.serialize(committable);

        assertThat(SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized))
                .isEqualTo(committable);
    }

    @Test
    void testDeserializeVersionOneCommittable() throws IOException {
        byte[] serialized = versionOneCommittableBytes();

        KafkaShareEosCommittable restored = SERIALIZER.deserialize(1, serialized);

        assertThat(restored.getCheckpointId()).isEqualTo(42L);
        assertThat(restored.getKafkaCommittables())
                .containsExactly(new KafkaCommittable(1L, (short) 2, "sink-txn", null));
        assertThat(restored.getShareAckCommittables())
                .containsExactly(
                        new ShareAckCommittable(
                                42L, "share-txn", 3L, (short) 4, "share-group", 5));
        assertThat(restored.getShareAckCommittables().get(0).getPreparedTransactionState())
                .isEmpty();
    }

    private static byte[] versionOneCommittableBytes() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(42L);
            out.writeInt(KafkaShareEosCommittable.CommitPhase.SINK_COMMITTED.ordinal());
            out.writeInt(1);
            byte[] kafkaCommittableBytes = versionOneKafkaCommittableBytes();
            out.writeInt(kafkaCommittableBytes.length);
            out.write(kafkaCommittableBytes);
            out.writeInt(1);
            out.writeLong(42L);
            out.writeUTF("share-txn");
            out.writeLong(3L);
            out.writeShort(4);
            out.writeUTF("share-group");
            out.writeInt(5);
            out.flush();
            return baos.toByteArray();
        }
    }

    private static byte[] versionOneKafkaCommittableBytes() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeShort(2);
            out.writeLong(1L);
            out.writeUTF("sink-txn");
            out.flush();
            return baos.toByteArray();
        }
    }
}
