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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Map;

/**
 * Deserialization of schema mock.
 *
 * @param <T> type
 */
public class MockDeserializationSchema<T> implements DeserializationSchema<T> {

    private final boolean isKey;
    private final boolean providesHeaders;

    public MockDeserializationSchema(boolean isKey, boolean providerHeaders) {
        this.isKey = isKey;
        this.providesHeaders = providerHeaders;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return null;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public void deserialize(byte[] message, Collector<T> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public T deserializeWithAdditionalProperties(
            byte[] message, Map<String, Object> inputAdditionalProperties) throws IOException {
        return DeserializationSchema.super.deserializeWithAdditionalProperties(
                message, inputAdditionalProperties);
    }

    @Override
    public void deserializeWithAdditionalProperties(
            byte[] message, Map<String, Object> inputAdditionalProperties, Collector<T> out)
            throws IOException {
        DeserializationSchema.super.deserializeWithAdditionalProperties(
                message, inputAdditionalProperties, out);
    }
}