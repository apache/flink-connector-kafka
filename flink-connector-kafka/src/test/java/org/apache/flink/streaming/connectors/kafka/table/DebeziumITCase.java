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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.avro.registry.confluent.AvroConfluentFormatOptions;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroFormatFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.streaming.connectors.kafka.table.Utils.RANDOM;
import static org.apache.flink.streaming.connectors.kafka.table.Utils.generateRandomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic IT cases for the Kafka table source based by a Debezium-Confluent-Avro topic. */
public class DebeziumITCase extends KafkaTableTestBase {

    @BeforeEach
    void before() {
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    private static final org.apache.avro.Schema keySchema =
            org.apache.avro.SchemaBuilder.record("key")
                    .namespace("com.example")
                    .doc("key doc")
                    .fields()
                    .name("id")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))
                    .noDefault()
                    .endRecord();

    private static final org.apache.avro.Schema valueSchema1 =
            org.apache.avro.SchemaBuilder.record("Value")
                    .namespace("com.example")
                    .fields()
                    .name("id")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))
                    .noDefault()
                    .name("a")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                    .noDefault()
                    .name("b")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))
                    .noDefault()
                    .name("c")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                    .noDefault()
                    .name("d")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))
                    .noDefault()
                    .endRecord();

    private static final org.apache.avro.Schema valueSchema2 =
            org.apache.avro.SchemaBuilder.record("Value")
                    .namespace("com.example")
                    .fields()
                    .name("id")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))
                    .noDefault()
                    .name("a")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                    .noDefault()
                    .name("b")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                    .noDefault()
                    .name("c")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                    .noDefault()
                    .name("d")
                    .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                    .noDefault()
                    .endRecord();

    private static org.apache.avro.Schema envelopeSchema(org.apache.avro.Schema valueSchema) {
        org.apache.avro.Schema envelopeSchema =
                org.apache.avro.SchemaBuilder.record("Envelope")
                        .namespace("com.example")
                        .fields()
                        .name("before")
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .type(valueSchema)
                        .endUnion()
                        .nullDefault()
                        .name("after")
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .type(valueSchema)
                        .endUnion()
                        .nullDefault()
                        .name("op")
                        .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING))
                        .noDefault()
                        .name("ts_ms")
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG))
                        .endUnion()
                        .nullDefault()
                        .endRecord();

        envelopeSchema.addProp("connect.name", "com.example.Envelope");

        return envelopeSchema;
    }

    @Test
    public void testWithBreakingSchemaChangeUsingGenericRecord() throws Exception {
        final String topic = UUID.randomUUID().toString();
        createTestTopic(topic, 1, 1);

        // common key record

        final int id = 1000;
        GenericRecordBuilder builder = new GenericRecordBuilder(keySchema);
        builder.set("id", id);
        final GenericRecord key = builder.build();

        // first message envelope record

        final String a0 = generateRandomString();
        final int b0 = RANDOM.nextInt(Integer.MAX_VALUE);
        final String c0 = generateRandomString();
        final int d0 = RANDOM.nextInt(Integer.MAX_VALUE);
        builder = new GenericRecordBuilder(valueSchema1);
        builder.set("id", id);
        builder.set("a", a0);
        builder.set("b", b0);
        builder.set("c", c0);
        builder.set("d", d0);
        final GenericRecord after1 = builder.build();

        builder = new GenericRecordBuilder(envelopeSchema(valueSchema1));
        builder.set("after", after1);
        builder.set("op", "c");
        final GenericRecord envelope1 = builder.build();

        // second message envelope record

        builder = new GenericRecordBuilder(valueSchema2);
        builder.set("id", id);
        builder.set("a", a0);
        builder.set("b", String.valueOf(b0));
        builder.set("c", c0);
        builder.set("d", String.valueOf(d0));
        final GenericRecord before2 = builder.build();

        final String a1 = generateRandomString();
        final String b1 = generateRandomString();
        final String c1 = generateRandomString();
        final String d1 = generateRandomString();
        builder = new GenericRecordBuilder(valueSchema2);
        builder.set("id", id);
        builder.set("a", a1);
        builder.set("b", b1);
        builder.set("c", c1);
        builder.set("d", d1);
        final GenericRecord after2 = builder.build();

        builder = new GenericRecordBuilder(envelopeSchema(valueSchema2));
        builder.set("before", before2);
        builder.set("after", after2);
        builder.set("op", "u");
        final GenericRecord envelope2 = builder.build();

        // send data

        final Properties producerProps = getStandardProps();
        producerProps.put(
                "key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProps.put(
                "value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", getSchemaRegistryUrl());
        try (KafkaProducer<GenericRecord, GenericRecord> producer =
                new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, key, envelope1)).get();
            producer.send(new ProducerRecord<>(topic, key, envelope2)).get();
        }

        // Read data

        final String tableName = "kafka";

        final Schema readSchema =
                Schema.newBuilder()
                        // Using original schema
                        .fromSchema(toFlinkSchema(keySchema, envelopeSchema(valueSchema1)))
                        // Added Kafka metadata
                        .columnByMetadata(
                                "kafka_topic",
                                KafkaDynamicSource.ReadableMetadata.TOPIC.dataType,
                                KafkaDynamicSource.ReadableMetadata.TOPIC.key)
                        .columnByMetadata(
                                "kafka_partition",
                                KafkaDynamicSource.ReadableMetadata.PARTITION.dataType,
                                KafkaDynamicSource.ReadableMetadata.PARTITION.key)
                        .columnByMetadata(
                                "kafka_offset",
                                KafkaDynamicSource.ReadableMetadata.OFFSET.dataType,
                                KafkaDynamicSource.ReadableMetadata.OFFSET.key)
                        .build();

        final Tuple2<String, Map<String, String>> formatOptions =
                Tuple2.of(
                        DebeziumAvroFormatFactory.IDENTIFIER,
                        Map.of(
                                AvroConfluentFormatOptions.URL.key(), getSchemaRegistryUrl(),
                                AvroConfluentFormatOptions.SUBJECT.key(),
                                        UUID.randomUUID().toString()));

        createKafkaTable(topic, formatOptions, tableName, readSchema);

        check(
                String.format("SELECT a FROM %s", tableName),
                "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[a], metadata=[]]], fields=[a])\n",
                3,
                Arrays.asList(
                        String.format("+I[%s]", a0),
                        String.format("-U[%s]", a0),
                        String.format("+U[%s]", a1)));

        check(
                String.format("SELECT c FROM %s", tableName),
                "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[c], metadata=[]]], fields=[c])\n",
                3,
                Arrays.asList(
                        String.format("+I[%s]", c0),
                        String.format("-U[%s]", c0),
                        String.format("+U[%s]", c1)));

        check(
                String.format("SELECT a, c FROM %s", tableName),
                "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[a, c], metadata=[]]], fields=[a, c])\n",
                3,
                Arrays.asList(
                        String.format("+I[%s, %s]", a0, c0),
                        String.format("-U[%s, %s]", a0, c0),
                        String.format("+U[%s, %s]", a1, c1)));

        check(
                String.format("SELECT c, a FROM %s", tableName),
                "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[c, a], metadata=[]]], fields=[c, a])\n",
                3,
                Arrays.asList(
                        String.format("+I[%s, %s]", c0, a0),
                        String.format("-U[%s, %s]", c0, a0),
                        String.format("+U[%s, %s]", c1, a1)));

        check(
                String.format("SELECT c, kafka_offset, a FROM %s", tableName),
                "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[c, a], metadata=[offset]]], fields=[c, a, kafka_offset])\n",
                3,
                Arrays.asList(
                        String.format("+I[%s, %d, %s]", c0, 0, a0),
                        String.format("-U[%s, %d, %s]", c0, 1, a0),
                        String.format("+U[%s, %d, %s]", c1, 1, a1)));

        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT * FROM " + tableName).execute().await())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(RuntimeException.class)
                .cause()
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(JobExecutionException.class)
                .cause()
                .isInstanceOf(JobException.class)
                .cause()
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(IOException.class)
                .hasMessage("Can't deserialize Debezium Avro message.");

        cleanupTopic(topic);
    }

    private static Schema.Builder schemaBuilder(final List<org.apache.avro.Schema.Field> fields) {
        final Schema.Builder builder = Schema.newBuilder();
        fields.forEach(
                field -> {
                    final String fieldName = field.name();
                    final DataType dataType =
                            AvroSchemaConverter.convertToDataType(field.schema().toString());
                    builder.column(fieldName, dataType).withComment(field.doc());
                });
        return builder;
    }

    private static Schema toFlinkSchema(
            final org.apache.avro.Schema keySchema, final org.apache.avro.Schema valueSchema) {
        assert keySchema.getType() == org.apache.avro.Schema.Type.RECORD;
        assert valueSchema.getType() == org.apache.avro.Schema.Type.RECORD;

        // Debezium format contains an "after" nullable RECORD field
        // Note: we could also use the "before" nullable RECORD field as the schemas are always
        // identical
        final org.apache.avro.Schema afterUnion = valueSchema.getField("after").schema();
        assert afterUnion.isUnion();
        final Set<org.apache.avro.Schema.Type> typesSet =
                afterUnion.getTypes().stream()
                        .map(org.apache.avro.Schema::getType)
                        .collect(Collectors.toSet());
        assert typesSet.equals(
                Set.of(org.apache.avro.Schema.Type.RECORD, org.apache.avro.Schema.Type.NULL));
        final org.apache.avro.Schema afterRecord =
                afterUnion.getTypes().stream()
                        .filter(x -> x.getType() == org.apache.avro.Schema.Type.RECORD)
                        .findFirst()
                        .get();

        return schemaBuilder(afterRecord.getFields())
                .primaryKey(
                        keySchema.getFields().stream()
                                .map(org.apache.avro.Schema.Field::name)
                                .collect(Collectors.toList()))
                .build();
    }

    private void check(
            final String sqlQuery,
            final String expectedPlanEndsWith,
            final int expectedNumRows,
            final List<String> expectedRows)
            throws Exception {
        final Table table = tEnv.sqlQuery(sqlQuery);

        assertThat(table.explain()).endsWith(expectedPlanEndsWith);

        final List<Row> actualRows = KafkaTableTestUtils.collectRows(table, expectedNumRows);
        assertThat(actualRows.stream().map(Row::toString)).isEqualTo(expectedRows);
    }

    private void createKafkaTable(
            final String topic,
            final Tuple2<String, Map<String, String>> formatOptions,
            final String tableName,
            final Schema schema) {
        final String format = formatOptions.f0;
        final TableDescriptor.Builder builder =
                TableDescriptor.forConnector("kafka")
                        .schema(schema)
                        .option(
                                KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS,
                                getBootstrapServers())
                        .option(KafkaConnectorOptions.TOPIC.key(), topic)
                        .option(
                                KafkaConnectorOptions.PROPS_GROUP_ID.key(),
                                getStandardProps().getProperty("group.id"))
                        .option(KafkaConnectorOptions.SCAN_STARTUP_MODE.key(), "earliest-offset")
                        .option("format", format);

        formatOptions.f1.forEach(
                (key, value) -> builder.option(String.format("%s.%s", format, key), value));

        final TableDescriptor tableDescriptor = builder.build();
        tEnv.createTable(tableName, tableDescriptor);
    }

    private void cleanupTopic(String topic) {
        ignoreExceptions(
                () -> deleteTestTopic(topic),
                anyCauseMatches(UnknownTopicOrPartitionException.class));
    }

    @SafeVarargs
    private static void ignoreExceptions(
            RunnableWithException runnable, ThrowingConsumer<? super Throwable>... ignoreIf) {
        try {
            runnable.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            // check if the exception is one of the ignored ones
            assertThat(ex).satisfiesAnyOf(ignoreIf);
        }
    }
}
