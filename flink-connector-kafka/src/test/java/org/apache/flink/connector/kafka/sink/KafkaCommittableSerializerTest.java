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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for serializing and deserialzing {@link KafkaCommittable} with {@link
 * KafkaCommittableSerializer}.
 */
class KafkaCommittableSerializerTest {

    private static final KafkaCommittableSerializer SERIALIZER = new KafkaCommittableSerializer();

    @Test
    void testCommittableSerDe() throws IOException {
        final String transactionalId = "test-id";
        final short epoch = 5;
        final KafkaCommittable committable = new KafkaCommittable(1L, epoch, transactionalId, null);
        final byte[] serialized = SERIALIZER.serialize(committable);
        assertThat(SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized))
                .isEqualTo(committable);
    }

    @Test
    void testPreparedTransactionStateSerDe() throws IOException {
        final KafkaCommittable committable =
                new KafkaCommittable(1L, (short) 5, "test-id", "1:5", null);

        final KafkaCommittable restored =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), SERIALIZER.serialize(committable));

        assertThat(restored).isEqualTo(committable);
        assertThat(restored.getPreparedTransactionState()).contains("1:5");
    }

    @Test
    void testDeserializeVersionOneCommittable() throws IOException {
        final byte[] versionOneBytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeShort(5);
            out.writeLong(1L);
            out.writeUTF("test-id");
            out.flush();
            versionOneBytes = baos.toByteArray();
        }

        final KafkaCommittable restored = SERIALIZER.deserialize(1, versionOneBytes);

        assertThat(restored).isEqualTo(new KafkaCommittable(1L, (short) 5, "test-id", null));
        assertThat(restored.getPreparedTransactionState()).isEmpty();
    }
}
