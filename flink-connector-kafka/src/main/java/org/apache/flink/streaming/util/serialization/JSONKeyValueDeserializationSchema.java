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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.util.JacksonMapperFactory;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * DeserializationSchema that deserializes a JSON String into an ObjectNode.
 *
 * <p>Key fields can be accessed by calling objectNode.get("key").get(&lt;name>).as(&lt;type>)
 *
 * <p>Value fields can be accessed by calling objectNode.get("value").get(&lt;name>).as(&lt;type>)
 *
 * <p>Metadata fields can be accessed by calling
 * objectNode.get("metadata").get(&lt;name>).as(&lt;type>) and include the "offset" (long), "topic"
 * (String) and "partition" (int).
 */
@PublicEvolving
public class JSONKeyValueDeserializationSchema
        implements KafkaRecordDeserializationSchema<ObjectNode> {

    private static final long serialVersionUID = 1509391548173891955L;

    private final boolean includeMetadata;
    private ObjectMapper mapper;

    public JSONKeyValueDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ObjectNode> out)
            throws IOException {
        ObjectNode node = mapper.createObjectNode();
        if (record.key() != null) {
            node.set("key", mapper.readValue(record.key(), JsonNode.class));
        }
        if (record.value() != null) {
            node.set("value", mapper.readValue(record.value(), JsonNode.class));
        }
        if (includeMetadata) {
            node.putObject("metadata")
                    .put("offset", record.offset())
                    .put("topic", record.topic())
                    .put("partition", record.partition());
        }
        out.collect(node);
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }
}
