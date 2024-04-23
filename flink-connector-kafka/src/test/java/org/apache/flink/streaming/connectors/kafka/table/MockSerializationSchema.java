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

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Serialization of schema mock.
 *
 * @param <T> type
 */
public class MockSerializationSchema<T> implements SerializationSchema<T> {
    boolean isKey = false;
    boolean produceHeaders = false;

    public MockSerializationSchema(boolean isKey) {
        this.isKey = isKey;
    }

    public void setProduceHeaders(boolean produceHeaders) {
        this.produceHeaders = produceHeaders;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(T t) {
        return new byte[0];
    }

    @Override
    public byte[] serializeWithAdditionalProperties(
            T element,
            Map<String, Object> inputAdditionalProperties,
            Map<String, Object> outputAdditionalProperties) {

        if (produceHeaders) {
            Map<String, Object> headersMap = new HashMap<>();
            if (isKey) {
                headersMap.put("K-key1", longToBytes(1));
                headersMap.put("K-key2", longToBytes(2));
            } else {
                headersMap.put("V-key1", longToBytes(3));
                headersMap.put("V-key2", longToBytes(4));
            }
            Map<String, Object> childMap = new HashMap<>();
            childMap.putAll(headersMap);
            outputAdditionalProperties.put(DynamicKafkaRecordSerializationSchema.HEADERS, childMap);
        }
        return new byte[0];
    }

    private static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
