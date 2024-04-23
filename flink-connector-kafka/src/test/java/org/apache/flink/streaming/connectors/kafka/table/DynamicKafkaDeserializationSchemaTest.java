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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Optional;

/** test. */
public class DynamicKafkaDeserializationSchemaTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    public void testdeserialize(TestSpec testSpec) {
        // ensure we have the method.
        DynamicKafkaRecordSerializationSchema.initializeMethod();
        MockDeserializationSchema<RowData> keyDeserialization = null;
        if (!testSpec.valueOnly) {
            keyDeserialization = new MockDeserializationSchema<>(true, testSpec.providesHeaders);
        }
        MockDeserializationSchema<RowData> valueDeserialization =
                new MockDeserializationSchema<>(false, testSpec.providesHeaders);

        int[] keyProjection = {};
        if (!testSpec.valueOnly) {
            keyProjection = new int[] {0, 1};
        }
        int[] valueProjection = {2, 3};
        TypeInformation producedTypeInfo = null;
        DynamicKafkaDeserializationSchema dynamicKafkaRecordDeserializationSchema =
                new DynamicKafkaDeserializationSchema(
                        4,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        valueProjection,
                        false,
                        null,
                        producedTypeInfo,
                        false);
        byte[] key = {};
        if (!testSpec.valueOnly) {
            key = new byte[] {0};
        }
        byte[] value = {0};
        ConsumerRecord<byte[], byte[]> message =
                new ConsumerRecord<>(
                        "",
                        0,
                        1L,
                        1L,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        testSpec.valueOnly ? 0 : 1,
                        1,
                        key,
                        value,
                        new RecordHeaders(),
                        Optional.empty());
        try {
            dynamicKafkaRecordDeserializationSchema.deserialize(message, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder().addAll(getValidTestSpecs()).build();
    }

    @Test
    public void testdeserializeWithoutMethod() {

        // ensure we have the method.
        DynamicKafkaRecordSerializationSchema.nullifySerializeWithAdditionalPropertiesMethod();
        try {
            FlinkKafkaPartitioner<RowData> partitioner = null;
            MockDeserializationSchema<RowData> keyDeserialization =
                    new MockDeserializationSchema<>(true, false);
            MockDeserializationSchema<RowData> valueDeserialization =
                    new MockDeserializationSchema<>(false, false);

            RowData.FieldGetter[] keyFieldGetters = null;

            int[] keyProjection = {0, 1};
            int[] valueProjection = {2, 3};
            TypeInformation producedTypeInfo = null;
            DynamicKafkaDeserializationSchema dynamicKafkaRecordDeserializationSchema =
                    new DynamicKafkaDeserializationSchema(
                            4,
                            keyDeserialization,
                            keyProjection,
                            valueDeserialization,
                            valueProjection,
                            false,
                            null,
                            producedTypeInfo,
                            false);
            byte[] key = {0};
            byte[] value = {0};
            ConsumerRecord<byte[], byte[]> message =
                    new ConsumerRecord<>(
                            "",
                            0,
                            1L,
                            1L,
                            TimestampType.NO_TIMESTAMP_TYPE,
                            1,
                            1,
                            key,
                            value,
                            new RecordHeaders(),
                            Optional.empty());
            try {
                dynamicKafkaRecordDeserializationSchema.deserialize(message, null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } finally {
            DynamicKafkaRecordSerializationSchema.initializeMethod();
        }
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

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
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

    @NotNull
    private static ImmutableList<TestSpec> getInvalidTestSpecs() {
        return ImmutableList.of();
    }
}
