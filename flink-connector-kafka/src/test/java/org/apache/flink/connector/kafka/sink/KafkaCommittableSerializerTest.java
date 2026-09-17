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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for serializing and deserialzing {@link KafkaCommittable} with {@link
 * KafkaCommittableSerializer}.
 */
class KafkaCommittableSerializerTest {

    private static final KafkaCommittableSerializer SERIALIZER = new KafkaCommittableSerializer();

    @ParameterizedTest
    @NullSource
    @ValueSource(booleans = {false, true})
    void testCommittableSerDe(Boolean transactionV2Enabled) throws IOException {
        final String transactionalId = "test-id";
        final short epoch = 5;
        final KafkaCommittable committable =
                new KafkaCommittable(1L, epoch, transactionalId, transactionV2Enabled, null);
        final byte[] serialized = SERIALIZER.serialize(committable);
        assertThat(SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized))
                .isEqualTo(committable);
    }

    @Test
    void testReadVersionOne() throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeShort(32766);
            out.writeLong(123L);
            out.writeUTF("legacy-transaction");
            KafkaCommittable legacy = SERIALIZER.deserialize(1, bytes.toByteArray());
            assertThat(legacy)
                    .isEqualTo(
                            new KafkaCommittable(
                                    123L, (short) 32766, "legacy-transaction", null, null));
            assertThat(legacy.getTransactionV2Enabled()).isNull();
            // Recheckpointing legacy state must preserve unknown instead of turning it into V1.
            assertThat(
                            SERIALIZER.deserialize(
                                    SERIALIZER.getVersion(), SERIALIZER.serialize(legacy)))
                    .isEqualTo(legacy);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-2, 2})
    void testRejectInvalidTransactionProtocol(int protocol) throws IOException {
        byte[] serialized =
                SERIALIZER.serialize(new KafkaCommittable(1L, (short) 0, "test-id", false, null));
        serialized[serialized.length - 1] = (byte) protocol;
        assertThatThrownBy(() -> SERIALIZER.deserialize(2, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("transaction protocol");
    }

    @Test
    void testRejectUnknownVersion() {
        assertThatThrownBy(() -> SERIALIZER.deserialize(3, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Kafka committable version");
    }
}
