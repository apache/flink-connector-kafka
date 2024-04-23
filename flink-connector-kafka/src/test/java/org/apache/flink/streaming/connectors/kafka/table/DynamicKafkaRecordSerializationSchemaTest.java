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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for DynamicKafkaRecordSerializationSchema. */
public class DynamicKafkaRecordSerializationSchemaTest {

    @Test
    public void testserializewithoutMethod() {

        DynamicKafkaRecordSerializationSchema.nullifySerializeWithAdditionalPropertiesMethod();
        try {
            FlinkKafkaPartitioner<RowData> partitioner = null;
            MockSerializationSchema<RowData> keySerialization = null;

            RowData.FieldGetter[] keyFieldGetters = null;

            keySerialization = new MockSerializationSchema<>(true);
            RowData.FieldGetter keyFieldGetter = RowData.createFieldGetter(new IntType(), 0);
            keyFieldGetters = new RowData.FieldGetter[] {keyFieldGetter};

            MockSerializationSchema<RowData> valueSerialization =
                    new MockSerializationSchema<>(false);
            RowData.FieldGetter valueFieldGetter = RowData.createFieldGetter(new IntType(), 0);
            RowData.FieldGetter[] valueFieldGetters = {valueFieldGetter};

            DynamicKafkaRecordSerializationSchema dynamicKafkaRecordSerializationSchema =
                    new DynamicKafkaRecordSerializationSchema(
                            "test",
                            partitioner,
                            keySerialization,
                            valueSerialization,
                            keyFieldGetters,
                            valueFieldGetters,
                            false,
                            new int[] {},
                            false);

            RowData consumedRow = new GenericRowData(1);
            KafkaRecordSerializationSchema.KafkaSinkContext context =
                    new KafkaRecordSerializationSchema.KafkaSinkContext() {
                        @Override
                        public int getParallelInstanceId() {
                            return 0;
                        }

                        @Override
                        public int getNumberOfParallelInstances() {
                            return 0;
                        }

                        @Override
                        public int[] getPartitionsForTopic(String topic) {
                            return new int[0];
                        }
                    };

            Long timestamp = null;
            final ProducerRecord<byte[], byte[]> producerRecord =
                    dynamicKafkaRecordSerializationSchema.serialize(
                            consumedRow, context, timestamp);
            Headers headers = producerRecord.headers();
            assertThat(headers).isEmpty();
        } finally {
            // ensure the method is present after this test.
            DynamicKafkaRecordSerializationSchema.initializeMethod();
        }
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    public void testserialize(TestSpec testSpec) {
        // ensure we have the method.
        DynamicKafkaRecordSerializationSchema.initializeMethod();
        FlinkKafkaPartitioner<RowData> partitioner = null;
        MockSerializationSchema<RowData> keySerialization = null;

        RowData.FieldGetter[] keyFieldGetters = null;

        if (!testSpec.valueOnly) {
            keySerialization = new MockSerializationSchema<>(true);
            keySerialization.setProduceHeaders(testSpec.providesHeaders);
            RowData.FieldGetter keyFieldGetter = RowData.createFieldGetter(new IntType(), 0);
            keyFieldGetters = new RowData.FieldGetter[] {keyFieldGetter};
        }
        MockSerializationSchema<RowData> valueSerialization = new MockSerializationSchema<>(false);
        RowData.FieldGetter valueFieldGetter = RowData.createFieldGetter(new IntType(), 0);
        RowData.FieldGetter[] valueFieldGetters = {valueFieldGetter};

        valueSerialization.setProduceHeaders(testSpec.providesHeaders);
        DynamicKafkaRecordSerializationSchema dynamicKafkaRecordSerializationSchema =
                new DynamicKafkaRecordSerializationSchema(
                        "test",
                        partitioner,
                        keySerialization,
                        valueSerialization,
                        keyFieldGetters,
                        valueFieldGetters,
                        false,
                        new int[] {},
                        false);

        RowData consumedRow = new GenericRowData(1);
        KafkaRecordSerializationSchema.KafkaSinkContext context =
                new KafkaRecordSerializationSchema.KafkaSinkContext() {
                    @Override
                    public int getParallelInstanceId() {
                        return 0;
                    }

                    @Override
                    public int getNumberOfParallelInstances() {
                        return 0;
                    }

                    @Override
                    public int[] getPartitionsForTopic(String topic) {
                        return new int[0];
                    }
                };

        Long timestamp = null;
        final ProducerRecord<byte[], byte[]> producerRecord =
                dynamicKafkaRecordSerializationSchema.serialize(consumedRow, context, timestamp);
        Headers headers = producerRecord.headers();
        if (!valueSerialization.produceHeaders
                && (keySerialization == null || !keySerialization.produceHeaders)) {
            assertThat(headers).isEmpty();
        } else {
            assertThat(headers.toArray().length == 2);

            Map<String, byte[]> headerMap = new HashMap<>();
            Arrays.asList(headers.toArray()).stream()
                    .forEach(header -> headerMap.put(header.key(), header.value()));
            if (keySerialization != null && keySerialization.isKey) {
                assertThat(headerMap.get("K-key1")).isEqualTo(longToBytes(1));
                assertThat(headerMap.get("K-key2")).isEqualTo(longToBytes(2));
            }
            if (!valueSerialization.isKey) {
                assertThat(headerMap.get("V-key1")).isEqualTo(longToBytes(3));
                assertThat(headerMap.get("V-key2")).isEqualTo(longToBytes(4));
            }
        }
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private static class TestSpec {

        final boolean valueOnly;

        final boolean providesHeaders;

        private TestSpec(boolean valueOnly, boolean providesHeaders) {
            this.providesHeaders = providesHeaders;
            this.valueOnly = valueOnly;
        }

        @Override
        public String toString() {

            return "TestSpec{"
                    + "valueOnly="
                    + valueOnly
                    + ", providesHeaders="
                    + providesHeaders
                    + '}';
        }
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder().addAll(getValidTestSpecs()).build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getValidTestSpecs() {
        return ImmutableList.of(
                // value and headers
                new TestSpec(true, true),
                // key and headers
                new TestSpec(false, true),
                // value and no headers
                new TestSpec(true, false),
                // key and headers
                new TestSpec(false, false));
    }
}
