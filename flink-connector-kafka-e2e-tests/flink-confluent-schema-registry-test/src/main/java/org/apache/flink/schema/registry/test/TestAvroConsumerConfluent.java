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

package org.apache.flink.schema.registry.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import example.avro.User;

/**
 * Reads Avro records from Kafka with the Confluent Schema Registry deserialization schema and
 * writes them back as strings and with the registry serialization schema, which registers the
 * schema under {@code --output-subject}. The source is bounded, so the job finishes once it has
 * read the records that were in the input topic when it started.
 */
public class TestAvroConsumerConfluent {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String bootstrapServers = params.getRequired("bootstrap.servers");
        final String registryUrl = params.getRequired("schema-registry-url");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaSource<User> source =
                KafkaSource.<User>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setGroupId(params.getRequired("group.id"))
                        .setTopics(params.getRequired("input-topic"))
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                                User.class, registryUrl)))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();
        final DataStream<User> users =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        users.map(User::toString)
                .returns(Types.STRING)
                .sinkTo(
                        KafkaSink.<String>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setRecordSerializer(
                                        KafkaRecordSerializationSchema.builder()
                                                .setTopic(params.getRequired("output-string-topic"))
                                                .setValueSerializationSchema(
                                                        new SimpleStringSchema())
                                                .build())
                                .build());

        users.sinkTo(
                KafkaSink.<User>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(params.getRequired("output-avro-topic"))
                                        .setValueSerializationSchema(
                                                ConfluentRegistryAvroSerializationSchema
                                                        .forSpecific(
                                                                User.class,
                                                                params.getRequired(
                                                                        "output-subject"),
                                                                registryUrl))
                                        .build())
                        .build());

        env.execute("Kafka Confluent Schema Registry Avro example");
    }
}
