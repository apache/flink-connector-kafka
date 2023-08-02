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

package org.apache.flink.connector.kafka.source.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** Extract payload from kafka connect SourceRecord,filter out the schema. */
public class ExtractPayloadSourceRecordUtil {

    private static final String RECORD_PAYLOAD_FIELD = "payload";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static byte[] extractPayloadIfIncludeConnectSchema(byte[] message, boolean includeSchema)
            throws IOException {
        if (includeSchema) {
            JsonNode jsonNode = deserializeToJsonNode(message);
            return objectMapper.writeValueAsBytes(jsonNode.get(RECORD_PAYLOAD_FIELD));
        }
        return message;
    }

    private static JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }
}
