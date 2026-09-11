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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for serializing and deserialzing {@link KafkaWriterState} with {@link
 * KafkaWriterStateSerializer}.
 */
class KafkaWriterStateSerializerTest {

    private static final KafkaWriterStateSerializer SERIALIZER = new KafkaWriterStateSerializer();

    @Test
    void testStateSerDe() throws IOException {
        final KafkaWriterState state =
                new KafkaWriterState(
                        "idPrefix",
                        0,
                        1,
                        TransactionOwnership.IMPLICIT_BY_SUBTASK_ID,
                        Arrays.asList(
                                new CheckpointTransaction("id1", 5L, 1000L, (short) 7),
                                new CheckpointTransaction("id2", 6L, 1001L, (short) 8)));
        final byte[] serialized = SERIALIZER.serialize(state);
        assertThat(SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized)).isEqualTo(state);
    }

    @Test
    void testUnknownEpochSurvivesSerDe() throws IOException {
        final KafkaWriterState state =
                new KafkaWriterState(
                        "idPrefix",
                        0,
                        1,
                        TransactionOwnership.EXPLICIT_BY_WRITER_STATE,
                        Arrays.asList(new CheckpointTransaction("id1", 5L)));
        final byte[] serialized = SERIALIZER.serialize(state);
        final KafkaWriterState deserialized =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized);
        assertThat(deserialized).isEqualTo(state);
        assertThat(deserialized.getPrecommittedTransactionalIds())
                .allMatch(transaction -> !transaction.hasKnownEpoch());
    }

    /** State written by version 2 has no producer id and epoch; both read back as unknown. */
    @Test
    void testDeserializeVersion2() throws IOException {
        final byte[] serializedV2;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF("idPrefix");
            out.writeInt(3);
            out.writeInt(4);
            out.writeInt(TransactionOwnership.EXPLICIT_BY_WRITER_STATE.ordinal());
            out.writeInt(2);
            out.writeUTF("id1");
            out.writeLong(5L);
            out.writeUTF("id2");
            out.writeLong(6L);
            out.flush();
            serializedV2 = baos.toByteArray();
        }

        assertThat(SERIALIZER.deserialize(2, serializedV2))
                .isEqualTo(
                        new KafkaWriterState(
                                "idPrefix",
                                3,
                                4,
                                TransactionOwnership.EXPLICIT_BY_WRITER_STATE,
                                Arrays.asList(
                                        new CheckpointTransaction("id1", 5L),
                                        new CheckpointTransaction("id2", 6L))));
    }
}
