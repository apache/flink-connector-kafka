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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.formats.json.JsonParseException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.config.FormatProjectionPushdownLevel;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectAllRows;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectRows;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.readLines;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;
import static org.apache.flink.table.utils.TableTestMatchers.deepEqualTo;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Basic IT cases for the Kafka table source and sink. */
class KafkaTableITCase extends KafkaTableTestBase {

    private static Collection<String> formats() {
        return Arrays.asList("avro", "csv", "json");
    }

    @BeforeEach
    void before() {
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSink(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "tstopic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "create table kafka (\n"
                                + "  `computed-price` as price + 1.0,\n"
                                + "  price decimal(38, 18),\n"
                                + "  currency string,\n"
                                + "  log_date date,\n"
                                + "  log_time time(3),\n"
                                + "  log_ts timestamp(3),\n"
                                + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                                + "  watermark for ts as ts\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic,
                        bootstraps,
                        groupId,
                        formatOptions(format));

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "SELECT CAST(price AS DECIMAL(10, 2)), currency, "
                        + " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n"
                        + "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n"
                        + "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n"
                        + "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n"
                        + "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n"
                        + "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n"
                        + "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n"
                        + "  AS orders (price, currency, d, t, ts)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        String query =
                "SELECT\n"
                        + "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n"
                        + "  CAST(MAX(log_date) AS VARCHAR),\n"
                        + "  CAST(MAX(log_time) AS VARCHAR),\n"
                        + "  CAST(MAX(ts) AS VARCHAR),\n"
                        + "  COUNT(*),\n"
                        + "  CAST(MAX(price) AS DECIMAL(10, 2))\n"
                        + "FROM kafka\n"
                        + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("Job_2");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

        List<String> expected =
                Arrays.asList(
                        "+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
                        "+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

        assertThat(TestingSinkFunction.getStringRows()).isEqualTo(expected);

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithTopicList(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic1 = "topics1_" + format + "_" + UUID.randomUUID();
        final String topic2 = "topics2_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic2, 1, 1);
        createTestTopic(topic1, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();
        final String createTableTemplate =
                "CREATE TABLE %s (\n"
                        + "  `topic` STRING METADATA,\n"
                        + "  `user_id` INT,\n"
                        + "  `item_id` INT,\n"
                        + "  `behavior` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = '%s',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'scan.bounded.mode' = 'latest-offset',\n"
                        + "  %s\n"
                        + ")\n";
        final String createTopicListTable =
                String.format(
                        createTableTemplate,
                        "kafka",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        String.join(";", Arrays.asList(topic1, topic2)),
                        bootstraps,
                        groupId,
                        formatOptions(format));
        final String createTopic1Table =
                String.format(
                        createTableTemplate,
                        "topic1",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic1,
                        bootstraps,
                        groupId,
                        formatOptions(format));
        final String createTopic2Table =
                String.format(
                        createTableTemplate,
                        "topic2",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic2,
                        bootstraps,
                        groupId,
                        formatOptions(format));

        tEnv.executeSql(createTopicListTable);
        tEnv.executeSql(createTopic1Table);
        tEnv.executeSql(createTopic2Table);

        List<Row> values =
                Arrays.asList(
                        Row.of(topic1, 1, 1102, "behavior 1"),
                        Row.of(topic2, 2, 1103, "behavior 2"));
        tEnv.fromValues(values).insertInto("kafka").execute().await();

        // ---------- Consume stream from Kafka -------------------
        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from kafka"));
        List<Row> topic1Results = collectAllRows(tEnv.sqlQuery("SELECT * from topic1"));
        List<Row> topic2Results = collectAllRows(tEnv.sqlQuery("SELECT * from topic2"));
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.of(topic1, 1, 1102, "behavior 1"),
                        Row.of(topic2, 2, 1103, "behavior 2"));
        assertThat(topic1Results).containsExactly(Row.of(topic1, 1, 1102, "behavior 1"));
        assertThat(topic2Results).containsExactly(Row.of(topic2, 2, 1103, "behavior 2"));

        // ------------- cleanup -------------------
        cleanupTopic(topic1);
        cleanupTopic(topic2);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithTopicPattern(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic1 = "topics1_" + format + "_" + UUID.randomUUID();
        final String topic2 = "topics2_" + format + "_" + UUID.randomUUID();
        final String topicPattern = "topics.*";
        createTestTopic(topic2, 1, 1);
        createTestTopic(topic1, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();
        final String createTableTemplate =
                "CREATE TABLE %s (\n"
                        + "  `topic` STRING METADATA,\n"
                        + "  `user_id` INT,\n"
                        + "  `item_id` INT,\n"
                        + "  `behavior` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = '%s',\n"
                        + "  'topic-pattern' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'scan.bounded.mode' = 'latest-offset',\n"
                        + "  %s\n"
                        + ")\n";
        final String createTopicPatternTable =
                String.format(
                        createTableTemplate,
                        "kafka",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topicPattern,
                        bootstraps,
                        groupId,
                        formatOptions(format));
        final String createTopic1Table =
                String.format(
                        createTableTemplate,
                        "topic1",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic1,
                        bootstraps,
                        groupId,
                        formatOptions(format));
        final String createTopic2Table =
                String.format(
                        createTableTemplate,
                        "topic2",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic2,
                        bootstraps,
                        groupId,
                        formatOptions(format));

        tEnv.executeSql(createTopicPatternTable);
        tEnv.executeSql(createTopic1Table);
        tEnv.executeSql(createTopic2Table);

        List<Row> values =
                Arrays.asList(
                        Row.of(topic1, 1, 1102, "behavior 1"),
                        Row.of(topic2, 2, 1103, "behavior 2"));
        tEnv.fromValues(values).insertInto("kafka").execute().await();

        // ---------- Consume stream from Kafka -------------------
        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from kafka"));
        List<Row> topic1Results = collectAllRows(tEnv.sqlQuery("SELECT * from topic1"));
        List<Row> topic2Results = collectAllRows(tEnv.sqlQuery("SELECT * from topic2"));
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.of(topic1, 1, 1102, "behavior 1"),
                        Row.of(topic2, 2, 1103, "behavior 2"));
        assertThat(topic1Results).containsExactly(Row.of(topic1, 1, 1102, "behavior 1"));
        assertThat(topic2Results).containsExactly(Row.of(topic2, 2, 1103, "behavior 2"));

        // ------------- cleanup -------------------

        cleanupTopic(topic1);
        cleanupTopic(topic2);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testExactlyOnceSink(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "topics_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String bootstraps = getBootstrapServers();
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE sink (\n"
                                + "  `user_id` INT,\n"
                                + "  `item_id` INT,\n"
                                + "  `behavior` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'sink.delivery-guarantee' = '%s',\n"
                                + "  'sink.transactional-id-prefix' = '%s',\n"
                                + "  'sink.transaction-naming-strategy' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'scan.bounded.mode' = 'latest-offset',\n"
                                + "  %s\n"
                                + ")\n",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        DeliveryGuarantee.EXACTLY_ONCE,
                        topic, // use topic as transactional id prefix - it's unique
                        TransactionNamingStrategy.POOLING,
                        topic,
                        bootstraps,
                        formatOptions(format)));

        List<Row> values =
                Arrays.asList(Row.of(1, 1102, "behavior 1"), Row.of(2, 1103, "behavior 2"));
        tEnv.fromValues(values).insertInto("sink").execute().await();

        // ---------- Consume stream from Kafka -------------------
        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from sink"));
        assertThat(results)
                .containsExactlyInAnyOrder(
                        Row.of(1, 1102, "behavior 1"), Row.of(2, 1103, "behavior 2"));

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceEmptyResultOnDeletedOffsets(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "bounded_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);
        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` INT,\n"
                                + "  `item_id` INT,\n"
                                + "  `behavior` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'specific-offsets',\n"
                                + "  'scan.bounded.mode' = 'specific-offsets',\n"
                                + "  'scan.startup.specific-offsets' = 'partition:0,offset:1',\n"
                                + "  'scan.bounded.specific-offsets' = 'partition:0,offset:3',\n"
                                + "  %s\n"
                                + ")\n",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic,
                        bootstraps,
                        groupId,
                        formatOptions(format));
        tEnv.executeSql(createTable);
        List<Row> values =
                Arrays.asList(
                        Row.of(1, 1102, "behavior 1"),
                        Row.of(2, 1103, "behavior 2"),
                        Row.of(3, 1104, "behavior 3"));
        tEnv.fromValues(values).insertInto("kafka").execute().await();
        // ---------- Delete events from Kafka -------------------
        Map<Integer, Long> partitionOffsetsToDelete = new HashMap<>();
        partitionOffsetsToDelete.put(0, 3L);
        deleteRecords(topic, partitionOffsetsToDelete);
        // ---------- Consume stream from Kafka -------------------
        List<Row> results = new ArrayList<>();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(createTable);
        results.addAll(collectAllRows(tEnv.sqlQuery("SELECT * FROM kafka")));
        assertThat(results).isEmpty();

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithBoundedSpecificOffsets(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "bounded_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` INT,\n"
                                + "  `item_id` INT,\n"
                                + "  `behavior` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'scan.bounded.mode' = 'specific-offsets',\n"
                                + "  'scan.bounded.specific-offsets' = 'partition:0,offset:2',\n"
                                + "  %s\n"
                                + ")\n",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic,
                        bootstraps,
                        groupId,
                        formatOptions(format));

        tEnv.executeSql(createTable);

        List<Row> values =
                Arrays.asList(
                        Row.of(1, 1102, "behavior 1"),
                        Row.of(2, 1103, "behavior 2"),
                        Row.of(3, 1104, "behavior 3"));
        tEnv.fromValues(values).insertInto("kafka").execute().await();

        // ---------- Consume stream from Kafka -------------------

        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from kafka"));

        assertThat(results)
                .containsExactly(Row.of(1, 1102, "behavior 1"), Row.of(2, 1103, "behavior 2"));

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithBoundedTimestamp(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "bounded_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` INT,\n"
                                + "  `item_id` INT,\n"
                                + "  `behavior` STRING,\n"
                                + "  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'scan.bounded.mode' = 'timestamp',\n"
                                + "  'scan.bounded.timestamp-millis' = '5',\n"
                                + "  %s\n"
                                + ")\n",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        topic,
                        bootstraps,
                        groupId,
                        formatOptions(format));

        tEnv.executeSql(createTable);

        List<Row> values =
                Arrays.asList(
                        Row.of(1, 1102, "behavior 1", Instant.ofEpochMilli(0L)),
                        Row.of(2, 1103, "behavior 2", Instant.ofEpochMilli(3L)),
                        Row.of(3, 1104, "behavior 3", Instant.ofEpochMilli(7L)));
        tEnv.fromValues(values).insertInto("kafka").execute().await();

        // ---------- Consume stream from Kafka -------------------

        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from kafka"));

        assertThat(results)
                .containsExactly(
                        Row.of(1, 1102, "behavior 1", Instant.ofEpochMilli(0L)),
                        Row.of(2, 1103, "behavior 2", Instant.ofEpochMilli(3L)));

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaTableWithMultipleTopics(final String format) throws Exception {
        // ---------- create source and sink tables -------------------
        String tableTemp =
                "create table %s (\n"
                        + "  currency string\n"
                        + ") with (\n"
                        + "  'connector' = '%s',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  %s\n"
                        + ")";
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();
        List<String> currencies = Arrays.asList("Euro", "Dollar", "Yen", "Dummy");
        List<String> topics =
                currencies.stream()
                        .map(
                                currency ->
                                        String.format(
                                                "%s_%s_%s", currency, format, UUID.randomUUID()))
                        .collect(Collectors.toList());
        // Because kafka connector currently doesn't support write data into multiple topic
        // together,
        // we have to create multiple sink tables.
        IntStream.range(0, 4)
                .forEach(
                        index -> {
                            createTestTopic(topics.get(index), 1, 1);
                            tEnv.executeSql(
                                    String.format(
                                            tableTemp,
                                            currencies.get(index).toLowerCase(),
                                            KafkaDynamicTableFactory.IDENTIFIER,
                                            topics.get(index),
                                            bootstraps,
                                            groupId,
                                            formatOptions(format)));
                        });
        // create source table
        tEnv.executeSql(
                String.format(
                        tableTemp,
                        "currencies_topic_list",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        String.join(";", topics),
                        bootstraps,
                        groupId,
                        formatOptions(format)));

        // ---------- Prepare data in Kafka topics -------------------
        String insertTemp =
                "INSERT INTO %s\n"
                        + "SELECT currency\n"
                        + " FROM (VALUES ('%s'))\n"
                        + " AS orders (currency)";
        currencies.forEach(
                currency -> {
                    try {
                        tEnv.executeSql(String.format(insertTemp, currency.toLowerCase(), currency))
                                .await();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                });

        // ------------- test the topic-list kafka source -------------------
        DataStream<RowData> result =
                tEnv.toAppendStream(
                        tEnv.sqlQuery("SELECT currency FROM currencies_topic_list"), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(4); // expect to receive 4 records
        result.addSink(sink);

        try {
            env.execute("Job_3");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }
        List<String> expected = Arrays.asList("+I(Dollar)", "+I(Dummy)", "+I(Euro)", "+I(Yen)");
        List<String> rows = TestingSinkFunction.getStringRows();
        rows.sort(Comparator.naturalOrder());
        assertThat(rows).isEqualTo(expected);

        // ------------- cleanup -------------------
        topics.forEach(super::deleteTestTopic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithMetadata(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "metadata_topic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `physical_1` STRING,\n"
                                + "  `physical_2` INT,\n"
                                // metadata fields are out of order on purpose
                                // offset is ignored because it might not be deterministic
                                + "  `timestamp-type` STRING METADATA VIRTUAL,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `leader-epoch` INT METADATA VIRTUAL,\n"
                                + "  `headers` MAP<STRING, BYTES> METADATA,\n"
                                + "  `partition` INT METADATA VIRTUAL,\n"
                                + "  `topic` STRING METADATA VIRTUAL,\n"
                                + "  `physical_3` BOOLEAN\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        topic, bootstraps, groupId, formatOptions(format));
        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " ('data 1', 1, TIMESTAMP '2020-03-08 13:12:11.123', MAP['k1', X'C0FFEE', 'k2', X'BABE01'], TRUE),\n"
                        + " ('data 2', 2, TIMESTAMP '2020-03-09 13:12:11.123', CAST(NULL AS MAP<STRING, BYTES>), FALSE),\n"
                        + " ('data 3', 3, TIMESTAMP '2020-03-10 13:12:11.123', MAP['k1', X'102030', 'k2', X'203040'], TRUE)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                "data 1",
                                1,
                                "CreateTime",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                0,
                                map(
                                        entry("k1", EncodingUtils.decodeHex("C0FFEE")),
                                        entry("k2", EncodingUtils.decodeHex("BABE01"))),
                                0,
                                topic,
                                true),
                        Row.of(
                                "data 2",
                                2,
                                "CreateTime",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                0,
                                Collections.emptyMap(),
                                0,
                                topic,
                                false),
                        Row.of(
                                "data 3",
                                3,
                                "CreateTime",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                0,
                                map(
                                        entry("k1", EncodingUtils.decodeHex("102030")),
                                        entry("k2", EncodingUtils.decodeHex("203040"))),
                                0,
                                topic,
                                true));

        assertThat(result).satisfies(matching(deepEqualTo(expected, true)));

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithKeyAndPartialValue(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_partial_value_topic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        // k_user_id and user_id have different data types to verify the correct mapping,
        // fields are reordered on purpose
        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `k_user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `k_event_id` BIGINT,\n"
                                + "  `user_id` INT,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s,\n"
                                + "  'key.fields' = 'k_event_id; k_user_id',\n"
                                + "  'key.fields-prefix' = 'k_',\n"
                                + "  %s,\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        keyFormatOptions(format),
                        valueFormatOptions(format));

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 41, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 42, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 43, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                41,
                                "payload 1"),
                        Row.of(
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                42,
                                "payload 2"),
                        Row.of(
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                43,
                                "payload 3"));

        assertThat(result).satisfies(matching(deepEqualTo(expected, true)));

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaSourceSinkWithKeyAndFullValue(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_full_value_topic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        // compared to the partial value test we cannot support both k_user_id and user_id in a full
        // value due to duplicate names after key prefix stripping,
        // fields are reordered on purpose,
        // fields for keys and values are overlapping
        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `event_id` BIGINT,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s,\n"
                                + "  'key.fields' = 'event_id; user_id',\n"
                                + "  %s,\n"
                                + "  'value.fields-include' = 'ALL'\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        keyFormatOptions(format),
                        valueFormatOptions(format));

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                "payload 1"),
                        Row.of(
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                "payload 2"),
                        Row.of(
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                "payload 3"));

        assertThat(result).satisfies(matching(deepEqualTo(expected, true)));

        // ------------- cleanup -------------------

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testKafkaTemporalJoinChangelog(final String format) throws Exception {
        // Set the session time zone to UTC, because the next `METADATA FROM
        // 'value.source.timestamp'` DDL
        // will use the session time zone when convert the changelog time from milliseconds to
        // timestamp
        tEnv.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");

        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String orderTopic = "temporal_join_topic_order_" + format + "_" + UUID.randomUUID();
        createTestTopic(orderTopic, 1, 1);

        final String productTopic =
                "temporal_join_topic_product_" + format + "_" + UUID.randomUUID();
        createTestTopic(productTopic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        // create order table and set initial values
        final String orderTableDDL =
                String.format(
                        "CREATE TABLE ordersTable (\n"
                                + "  order_id STRING,\n"
                                + "  product_id STRING,\n"
                                + "  order_time TIMESTAMP(3),\n"
                                + "  quantity INT,\n"
                                + "  purchaser STRING,\n"
                                + "  WATERMARK FOR order_time AS order_time - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  %s\n"
                                + ")",
                        orderTopic, bootstraps, groupId, formatOptions(format));
        tEnv.executeSql(orderTableDDL);
        String orderInitialValues =
                "INSERT INTO ordersTable\n"
                        + "VALUES\n"
                        + "('o_001', 'p_001', TIMESTAMP '2020-10-01 00:01:00', 1, 'Alice'),"
                        + "('o_002', 'p_002', TIMESTAMP '2020-10-01 00:02:00', 1, 'Bob'),"
                        + "('o_003', 'p_001', TIMESTAMP '2020-10-01 12:00:00', 2, 'Tom'),"
                        + "('o_004', 'p_002', TIMESTAMP '2020-10-01 12:00:00', 2, 'King'),"
                        + "('o_005', 'p_001', TIMESTAMP '2020-10-01 18:00:00', 10, 'Leonard'),"
                        + "('o_006', 'p_002', TIMESTAMP '2020-10-01 18:00:00', 10, 'Leonard'),"
                        + "('o_007', 'p_002', TIMESTAMP '2020-10-01 18:00:01', 10, 'Robinson')"; // used to advance watermark
        tEnv.executeSql(orderInitialValues).await();

        // create product table and set initial values
        final String productTableDDL =
                String.format(
                        "CREATE TABLE productChangelogTable (\n"
                                + "  product_id STRING,\n"
                                + "  product_name STRING,\n"
                                + "  product_price DECIMAL(10, 4),\n"
                                + "  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,\n"
                                + "  PRIMARY KEY(product_id) NOT ENFORCED,\n"
                                + "  WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  %s\n"
                                + ")",
                        productTopic, bootstraps, groupId, valueFormatOptions("debezium-json"));
        tEnv.executeSql(productTableDDL);

        // use raw format to initial the changelog data
        initialProductChangelog(productTopic, bootstraps);

        // ---------- query temporal join result from Kafka -------------------
        final List<String> result =
                collectRows(
                                tEnv.sqlQuery(
                                        "SELECT"
                                                + "  order_id,"
                                                + "  order_time,"
                                                + "  P.product_id,"
                                                + "  P.update_time as product_update_time,"
                                                + "  product_price,"
                                                + "  purchaser,"
                                                + "  product_name,"
                                                + "  quantity,"
                                                + "  quantity * product_price AS order_amount "
                                                + "FROM ordersTable AS O "
                                                + "LEFT JOIN productChangelogTable FOR SYSTEM_TIME AS OF O.order_time AS P "
                                                + "ON O.product_id = P.product_id"),
                                6)
                        .stream()
                        .map(row -> row.toString())
                        .sorted()
                        .collect(Collectors.toList());

        final List<String> expected =
                Arrays.asList(
                        "+I[o_001, 2020-10-01T00:01, p_001, 1970-01-01T00:00, 11.1100, Alice, scooter, 1, 11.1100]",
                        "+I[o_002, 2020-10-01T00:02, p_002, 1970-01-01T00:00, 23.1100, Bob, basketball, 1, 23.1100]",
                        "+I[o_003, 2020-10-01T12:00, p_001, 2020-10-01T12:00, 12.9900, Tom, scooter, 2, 25.9800]",
                        "+I[o_004, 2020-10-01T12:00, p_002, 2020-10-01T12:00, 19.9900, King, basketball, 2, 39.9800]",
                        "+I[o_005, 2020-10-01T18:00, p_001, 2020-10-01T18:00, 11.9900, Leonard, scooter, 10, 119.9000]",
                        "+I[o_006, 2020-10-01T18:00, null, null, null, Leonard, null, 10, null]");

        assertThat(result).isEqualTo(expected);

        // ------------- cleanup -------------------

        cleanupTopic(orderTopic);
        cleanupTopic(productTopic);
    }

    private void initialProductChangelog(String topic, String bootstraps) throws Exception {
        String productChangelogDDL =
                String.format(
                        "CREATE TABLE productChangelog (\n"
                                + "  changelog STRING"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'format' = 'raw'\n"
                                + ")",
                        topic, bootstraps);
        tEnv.executeSql(productChangelogDDL);
        String[] allChangelog = readLines("product_changelog.txt").toArray(new String[0]);

        StringBuilder insertSqlSb = new StringBuilder();
        insertSqlSb.append("INSERT INTO productChangelog VALUES ");
        for (String log : allChangelog) {
            insertSqlSb.append("('" + log + "'),");
        }
        // trim the last comma
        String insertSql = insertSqlSb.substring(0, insertSqlSb.toString().length() - 1);
        tEnv.executeSql(insertSql).await();
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testPerPartitionWatermarkKafka(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "per_partition_watermark_topic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3),\n"
                                + "  WATERMARK FOR `timestamp` AS `timestamp`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  %s\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        TestPartitioner.class.getName(),
                        formatOptions(format));

        tEnv.executeSql(createTable);

        // make every partition have more than one record
        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (0, 'partition-0-name-0', TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (0, 'partition-0-name-1', TIMESTAMP '2020-03-08 14:12:12.223'),\n"
                        + " (0, 'partition-0-name-2', TIMESTAMP '2020-03-08 15:12:13.323'),\n"
                        + " (1, 'partition-1-name-0', TIMESTAMP '2020-03-09 13:13:11.123'),\n"
                        + " (1, 'partition-1-name-1', TIMESTAMP '2020-03-09 15:13:11.133'),\n"
                        + " (1, 'partition-1-name-2', TIMESTAMP '2020-03-09 16:13:11.143'),\n"
                        + " (2, 'partition-2-name-0', TIMESTAMP '2020-03-10 13:12:14.123'),\n"
                        + " (2, 'partition-2-name-1', TIMESTAMP '2020-03-10 14:12:14.123'),\n"
                        + " (2, 'partition-2-name-2', TIMESTAMP '2020-03-10 14:13:14.123'),\n"
                        + " (2, 'partition-2-name-3', TIMESTAMP '2020-03-10 14:14:14.123'),\n"
                        + " (2, 'partition-2-name-4', TIMESTAMP '2020-03-10 14:15:14.123'),\n"
                        + " (2, 'partition-2-name-5', TIMESTAMP '2020-03-10 14:16:14.123'),\n"
                        + " (3, 'partition-3-name-0', TIMESTAMP '2020-03-11 17:12:11.123'),\n"
                        + " (3, 'partition-3-name-1', TIMESTAMP '2020-03-11 18:12:11.123')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts as ts\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink.drop-late-event' = 'true'\n"
                        + ")";
        tEnv.executeSql(createSink);
        TableResult tableResult = tEnv.executeSql("INSERT INTO MySink SELECT * FROM kafka");
        final List<String> expected =
                Arrays.asList(
                        "+I[0, partition-0-name-0, 2020-03-08T13:12:11.123]",
                        "+I[0, partition-0-name-1, 2020-03-08T14:12:12.223]",
                        "+I[0, partition-0-name-2, 2020-03-08T15:12:13.323]",
                        "+I[1, partition-1-name-0, 2020-03-09T13:13:11.123]",
                        "+I[1, partition-1-name-1, 2020-03-09T15:13:11.133]",
                        "+I[1, partition-1-name-2, 2020-03-09T16:13:11.143]",
                        "+I[2, partition-2-name-0, 2020-03-10T13:12:14.123]",
                        "+I[2, partition-2-name-1, 2020-03-10T14:12:14.123]",
                        "+I[2, partition-2-name-2, 2020-03-10T14:13:14.123]",
                        "+I[2, partition-2-name-3, 2020-03-10T14:14:14.123]",
                        "+I[2, partition-2-name-4, 2020-03-10T14:15:14.123]",
                        "+I[2, partition-2-name-5, 2020-03-10T14:16:14.123]",
                        "+I[3, partition-3-name-0, 2020-03-11T17:12:11.123]",
                        "+I[3, partition-3-name-1, 2020-03-11T18:12:11.123]");
        KafkaTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        cancelJob(tableResult);
        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testPerPartitionWatermarkWithIdleSource(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "idle_partition_watermark_topic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();
        tEnv.getConfig().set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofMillis(100));

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `value` INT,\n"
                                + "  `timestamp` TIMESTAMP(3),\n"
                                + "  WATERMARK FOR `timestamp` AS `timestamp`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  %s\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        TestPartitioner.class.getName(),
                        formatOptions(format));

        tEnv.executeSql(createTable);

        // Only two partitions have elements and others are idle.
        // When idle timer triggers, the WatermarkOutputMultiplexer will use the minimum watermark
        // among active partitions as the output watermark.
        // Therefore, we need to make sure the watermark in the each partition is large enough to
        // trigger the window.
        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (0, 0, TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (0, 1, TIMESTAMP '2020-03-08 13:15:12.223'),\n"
                        + " (0, 2, TIMESTAMP '2020-03-08 16:12:13.323'),\n"
                        + " (1, 3, TIMESTAMP '2020-03-08 13:13:11.123'),\n"
                        + " (1, 4, TIMESTAMP '2020-03-08 13:19:11.133'),\n"
                        + " (1, 5, TIMESTAMP '2020-03-08 16:13:11.143')\n";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  `id` INT,\n"
                        + "  `cnt` BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")";
        tEnv.executeSql(createSink);
        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO MySink\n"
                                + "SELECT `partition_id` as `id`, COUNT(`value`) as `cnt`\n"
                                + "FROM kafka\n"
                                + "GROUP BY `partition_id`, TUMBLE(`timestamp`, INTERVAL '1' HOUR) ");

        final List<String> expected = Arrays.asList("+I[0, 2]", "+I[1, 2]");
        KafkaTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        cancelJob(tableResult);
        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testLatestOffsetStrategyResume(final String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "latest_offset_resume_topic_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 6, 1);
        env.setParallelism(1);

        // ---------- Produce data into Kafka's partition 0-6 -------------------

        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `value` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'latest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  %s\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        TestPartitioner.class.getName(),
                        formatOptions(format));

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka VALUES (0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  `id` INT,\n"
                        + "  `value` INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")";
        tEnv.executeSql(createSink);

        String executeInsert = "INSERT INTO MySink SELECT `partition_id`, `value` FROM kafka";
        TableResult tableResult = tEnv.executeSql(executeInsert);

        // ---------- Produce data into Kafka's partition 0-2 -------------------

        String moreValues = "INSERT INTO kafka VALUES (0, 1), (1, 1), (2, 1)";
        tEnv.executeSql(moreValues).await();

        final List<String> expected = Arrays.asList("+I[0, 1]", "+I[1, 1]", "+I[2, 1]");
        KafkaTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ---------- Stop the consume job with savepoint  -------------------

        String savepointBasePath = getTempDirPath(topic + "-savepoint");
        assert tableResult.getJobClient().isPresent();
        JobClient client = tableResult.getJobClient().get();
        String savepointPath =
                client.stopWithSavepoint(false, savepointBasePath, SavepointFormatType.DEFAULT)
                        .get();

        // ---------- Produce data into Kafka's partition 0-5 -------------------

        String produceValuesBeforeResume =
                "INSERT INTO kafka VALUES (0, 2), (1, 2), (2, 2), (3, 1), (4, 1), (5, 1)";
        tEnv.executeSql(produceValuesBeforeResume).await();

        // ---------- Resume the consume job from savepoint  -------------------

        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createTable);
        tEnv.executeSql(createSink);
        tableResult = tEnv.executeSql(executeInsert);

        final List<String> afterResumeExpected =
                Arrays.asList(
                        "+I[0, 1]",
                        "+I[1, 1]",
                        "+I[2, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 2]",
                        "+I[3, 1]",
                        "+I[4, 1]",
                        "+I[5, 1]");
        KafkaTableTestUtils.waitingExpectedResults(
                "MySink", afterResumeExpected, Duration.ofSeconds(5));

        // ---------- Produce data into Kafka's partition 0-5 -------------------

        String produceValuesAfterResume =
                "INSERT INTO kafka VALUES (0, 3), (1, 3), (2, 3), (3, 2), (4, 2), (5, 2)";
        this.tEnv.executeSql(produceValuesAfterResume).await();

        final List<String> afterProduceExpected =
                Arrays.asList(
                        "+I[0, 1]",
                        "+I[1, 1]",
                        "+I[2, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 2]",
                        "+I[3, 1]",
                        "+I[4, 1]",
                        "+I[5, 1]",
                        "+I[0, 3]",
                        "+I[1, 3]",
                        "+I[2, 3]",
                        "+I[3, 2]",
                        "+I[4, 2]",
                        "+I[5, 2]");
        KafkaTableTestUtils.waitingExpectedResults(
                "MySink", afterProduceExpected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        cancelJob(tableResult);
        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testStartFromGroupOffsetsLatest(final String format) throws Exception {
        testStartFromGroupOffsets("latest", format);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testStartFromGroupOffsetsEarliest(final String format) throws Exception {
        testStartFromGroupOffsets("earliest", format);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    void testStartFromGroupOffsetsNone(final String format) {
        Assertions.assertThatThrownBy(() -> testStartFromGroupOffsetsWithNoneResetStrategy(format))
                .satisfies(anyCauseMatches(NoOffsetForPartitionException.class));
    }

    private List<String> appendNewData(
            String topic, String tableName, String groupId, int targetNum) throws Exception {
        waitUtil(
                () -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets = getConsumerOffset(groupId);
                    long sum =
                            offsets.entrySet().stream()
                                    .filter(e -> e.getKey().topic().contains(topic))
                                    .mapToLong(e -> e.getValue().offset())
                                    .sum();
                    return sum == targetNum;
                },
                Duration.ofMillis(20000),
                "Can not reach the expected offset before adding new data.");
        String appendValues =
                "INSERT INTO "
                        + tableName
                        + "\n"
                        + "VALUES\n"
                        + " (2, 6),\n"
                        + " (2, 7),\n"
                        + " (2, 8)\n";
        tEnv.executeSql(appendValues).await();
        return Arrays.asList("+I[2, 6]", "+I[2, 7]", "+I[2, 8]");
    }

    private TableResult startFromGroupOffset(
            String tableName,
            String topic,
            String groupId,
            String resetStrategy,
            String sinkName,
            String format)
            throws ExecutionException, InterruptedException {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String bootstraps = getBootstrapServers();
        tEnv.getConfig().set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofMillis(100));

        final String createTableSql =
                "CREATE TABLE %s (\n"
                        + "  `partition_id` INT,\n"
                        + "  `value` INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'group-offsets',\n"
                        + "  'properties.auto.offset.reset' = '%s',\n"
                        + "  'properties.enable.auto.commit' = 'true',\n"
                        + "  'properties.auto.commit.interval.ms' = '1000',\n"
                        + "  %s\n"
                        + ")";
        tEnv.executeSql(
                String.format(
                        createTableSql,
                        tableName,
                        topic,
                        bootstraps,
                        groupId,
                        resetStrategy,
                        formatOptions(format)));

        String initialValues =
                "INSERT INTO "
                        + tableName
                        + "\n"
                        + "VALUES\n"
                        + " (0, 0),\n"
                        + " (0, 1),\n"
                        + " (0, 2),\n"
                        + " (1, 3),\n"
                        + " (1, 4),\n"
                        + " (1, 5)\n";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE "
                        + sinkName
                        + "(\n"
                        + "  `partition_id` INT,\n"
                        + "  `value` INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")";
        tEnv.executeSql(createSink);

        return tEnv.executeSql("INSERT INTO " + sinkName + " SELECT * FROM " + tableName);
    }

    private void testStartFromGroupOffsets(String resetStrategy, String format) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String tableName = "Table" + format + resetStrategy;
        final String topic =
                "groupOffset_" + format + resetStrategy + ThreadLocalRandom.current().nextLong();
        String groupId = format + resetStrategy;
        String sinkName = "mySink" + format + resetStrategy;
        List<String> expected =
                Arrays.asList(
                        "+I[0, 0]", "+I[0, 1]", "+I[0, 2]", "+I[1, 3]", "+I[1, 4]", "+I[1, 5]");

        TableResult tableResult = null;
        try {
            tableResult =
                    startFromGroupOffset(
                            tableName, topic, groupId, resetStrategy, sinkName, format);
            if ("latest".equals(resetStrategy)) {
                expected = appendNewData(topic, tableName, groupId, expected.size());
            }
            KafkaTableTestUtils.waitingExpectedResults(sinkName, expected, Duration.ofSeconds(15));
        } finally {
            // ------------- cleanup -------------------
            cancelJob(tableResult);
            cleanupTopic(topic);
        }
    }

    private void testStartFromGroupOffsetsWithNoneResetStrategy(final String format)
            throws ExecutionException, InterruptedException {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String resetStrategy = "none";
        final String tableName = resetStrategy + "Table";
        final String topic = "groupOffset_" + format + "_" + UUID.randomUUID();
        String groupId = resetStrategy + (new Random()).nextInt();

        TableResult tableResult = null;
        try {
            tableResult =
                    startFromGroupOffset(
                            tableName, topic, groupId, resetStrategy, "MySink", format);
            tableResult.await();
        } finally {
            // ------------- cleanup -------------------
            cancelJob(tableResult);
            cleanupTopic(topic);
        }
    }

    private void projectionPushdownSetupData(final String format, final String topic)
            throws Exception {
        createTestTopic(topic, 1, 1);

        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `a` STRING,\n"
                                + "  `b` STRING,\n"
                                + "  `topic` STRING NOT NULL METADATA VIRTUAL,\n"
                                + "  `c` STRING,\n"
                                + "  `partition` INT NOT NULL METADATA VIRTUAL,\n"
                                + "  `d` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s,\n"
                                + "  'key.fields' = 'a; b',\n"
                                + "  %s,\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        keyFormatOptions(format),
                        valueFormatOptions(format));
        tEnv.executeSql(createTable);

        final String initialValues = "INSERT INTO kafka (a, b, c, d) SELECT 'a', 'b', 'c', 'd'";
        tEnv.executeSql(initialValues).await();
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void testProjectionPushdownSelectAllFields(final String format) throws Exception {
        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        projectionPushdownSetupData(format, topic);

        assertQueryResult(
                "SELECT * FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[a, b, topic, c, partition, d])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, metadata=[topic, partition]]], fields=[a, b, c, d, topic, partition])\n",
                Collections.singletonList(String.format("+I(a,b,%s,c,%d,d)", topic, 0)));

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void testProjectionPushdownSelectSpecificPhysicalFields(final String format)
            throws Exception {
        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        projectionPushdownSetupData(format, topic);

        assertQueryResult(
                "SELECT a, c FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[a, c], metadata=[]]], fields=[a, c])\n",
                Collections.singletonList("+I(a,c)"));

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void
            testProjectionPushdownSelectNonContiguousPhysicalFieldsInDifferentOrderFromTableSchema(
                    final String format) throws Exception {
        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        projectionPushdownSetupData(format, topic);

        assertQueryResult(
                "SELECT c, a FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[c, a], metadata=[]]], fields=[c, a])\n",
                Collections.singletonList("+I(c,a)"));

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void
            testProjectionPushdownSelectSpecificPhysicalAndMetadataFieldsInDifferentOrderFromTableSchema(
                    final String format) throws Exception {
        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        projectionPushdownSetupData(format, topic);

        assertQueryResult(
                "SELECT c, `partition`, a, topic FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[c, partition, a, topic])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, project=[c, a], metadata=[topic, partition]]], fields=[c, a, topic, partition])\n",
                Collections.singletonList(String.format("+I(c,%s,a,%s)", 0, topic)));

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void testProjectionPushdownSelectSameColumnTwice(final String format) throws Exception {
        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        projectionPushdownSetupData(format, topic);

        assertQueryResult(
                "SELECT b AS b1, b AS b2 FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[b AS b1, b AS b2])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, project=[b], metadata=[]]], fields=[b])\n",
                Collections.singletonList("+I(b,b)"));

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void testProjectionPushdownNestColumns(final String format) throws Exception {
        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        projectionPushdownSetupData(format, topic);

        final String query = "SELECT ROW(b, a) AS nested FROM kafka";

        final String expectedPlanEndsWith =
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[ROW(b, a) AS nested])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, project=[b, a], metadata=[]]], fields=[b, a])\n";

        final BinaryRowData nested = new BinaryRowData(2);
        final BinaryWriter binaryWriter = new BinaryRowWriter(nested);
        binaryWriter.writeString(0, StringData.fromString("b"));
        binaryWriter.writeString(1, StringData.fromString("a"));
        binaryWriter.complete();
        final GenericRowData row = new GenericRowData(1);
        row.setField(0, nested);
        final List<RowData> expected = Collections.singletonList(row);

        final Table table = tEnv.sqlQuery(query);

        assertThat(table.explain()).endsWith(expectedPlanEndsWith);

        final DataStream<RowData> result =
                tEnv.toRetractStream(table, RowData.class).map(tuple -> tuple.f1);
        final TestingSinkFunction sink = new TestingSinkFunction(expected.size());
        result.addSink(sink).setParallelism(1);
        try {
            env.execute("Job_2");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }
        assertThat(TestingSinkFunction.getRows()).isEqualTo(expected);

        cleanupTopic(topic);
    }

    private void nestedProjectionPushdownSetupData(final String topic) throws Exception {
        // Only JSON format supports nested projection pushdown currently
        final String format = "json";

        createTestTopic(topic, 1, 1);

        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `a` ROW(a1 STRING, a2 STRING),\n"
                                + "  `b` ROW(b1 STRING, b2 STRING),\n"
                                + "  `topic` STRING NOT NULL METADATA VIRTUAL,\n"
                                + "  `c` ROW(c1 STRING, c2 STRING),\n"
                                + "  `partition` INT NOT NULL METADATA VIRTUAL,\n"
                                + "  `d` ROW(d1 ROW(d2 STRING, d3 STRING))\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s,\n"
                                + "  'key.fields' = 'a; b',\n"
                                + "  %s,\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        keyFormatOptions(format),
                        valueFormatOptions(format));
        tEnv.executeSql(createTable);

        tEnv.executeSql(
                        "INSERT INTO kafka \n"
                                + "SELECT \n"
                                + "    ROW('a1', 'a2'), \n"
                                + "    ROW('b1', 'b2'), \n"
                                + "    ROW('c1', 'c2'), \n"
                                + "    ROW(ROW('d2', 'd3'))")
                .await();
    }

    @Test
    public void testNestedProjectionPushdownSelectAllFields() throws Exception {
        final String topic = "testNestedProjectionPushdown_" + "_" + UUID.randomUUID();
        nestedProjectionPushdownSetupData(topic);

        assertQueryResult(
                "SELECT * FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[a, b, topic, c, partition, d])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, metadata=[topic, partition]]], fields=[a, b, c, d, topic, partition])\n",
                Collections.singletonList(
                        String.format(
                                "+I(+I(a1,a2),+I(b1,b2),%s,+I(c1,c2),0,+I(+I(d2,d3)))", topic)));

        cleanupTopic(topic);
    }

    @Test
    public void testNestedProjectionPushdownSelectSpecificNestedPhysicalFields() throws Exception {
        final String topic = "testNestedProjectionPushdown_" + "_" + UUID.randomUUID();
        nestedProjectionPushdownSetupData(topic);

        assertQueryResult(
                "SELECT a.a1, d.d1.d3 FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[a_a1, d_d1_d3], metadata=[]]], fields=[a_a1, d_d1_d3])\n",
                Collections.singletonList("+I(a1,d3)"));

        cleanupTopic(topic);
    }

    @Test
    public void
            testNestedProjectionPushdownSelectSpecificNestedPhysicalFieldsInDifferentOrderFromTableSchema()
                    throws Exception {
        final String topic = "testNestedProjectionPushdown_" + "_" + UUID.randomUUID();
        nestedProjectionPushdownSetupData(topic);

        assertQueryResult(
                "SELECT d.d1.d3, a.a1 FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[d_d1_d3, a_a1], metadata=[]]], fields=[d_d1_d3, a_a1])\n",
                Collections.singletonList("+I(d3,a1)"));

        cleanupTopic(topic);
    }

    @Test
    public void
            testNestedProjectionPushdownSelectSpecificNestedPhysicalAndMetadataFieldsInDifferentOrderFromTableSchema()
                    throws Exception {
        final String topic = "testNestedProjectionPushdown_" + "_" + UUID.randomUUID();
        nestedProjectionPushdownSetupData(topic);

        assertQueryResult(
                "SELECT d.d1.d3, `partition`, a.a1, topic FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[d_d1_d3 AS d3, partition, a_a1 AS a1, topic])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, project=[d_d1_d3, a_a1], metadata=[topic, partition]]], fields=[d_d1_d3, a_a1, topic, partition])\n",
                Collections.singletonList(String.format("+I(d3,%s,a1,%s)", 0, topic)));

        cleanupTopic(topic);
    }

    @Test
    public void
            testNestedProjectionPushdownSelectSpecificNestedAndNonNestedPhysicalAndMetadataFieldsInDifferentOrderFromTableSchema()
                    throws Exception {
        final String topic = "testNestedProjectionPushdown_" + "_" + UUID.randomUUID();
        nestedProjectionPushdownSetupData(topic);

        assertQueryResult(
                "SELECT d.d1.d3, d, `partition`, a.a1, topic FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "Calc(select=[d.d1.d3 AS d3, d, partition, a_a1 AS a1, topic])\n"
                        + "+- TableSourceScan(table=[[default_catalog, default_database, kafka, project=[d, a_a1], metadata=[topic, partition]]], fields=[d, a_a1, topic, partition])\n",
                Collections.singletonList(
                        String.format("+I(d3,+I(+I(d2,d3)),%s,a1,%s)", 0, topic)));

        cleanupTopic(topic);
    }

    @Test
    public void testProjectionPushdownWithJsonFormatAndBreakingSchemaChange() throws Exception {
        // Only self-contained formats like JSON can use projection pushdown to
        // avoid deserializing fields that aren't needed for a query and thus avoid
        // errors from breaking schema changes.
        final String format = "json";

        // Setup data

        final String topic = "testProjectionPushdown_" + format + "_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String originalCreateTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `a` INT,\n" // `a` is originally an INT  field
                                + "  `b` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s,\n"
                                + "  'key.fields' = 'a; b',\n"
                                + "  %s,\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        keyFormatOptions(format),
                        valueFormatOptions(format));
        tEnv.executeSql(originalCreateTable);
        final int a1 = 1;
        final String b1 = "b1";
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO kafka SELECT * FROM (VALUES (%d, '%s'))", a1, b1))
                .await();
        tEnv.executeSql("DROP TABLE kafka").await();

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `a` STRING,\n" // `a` is now a STRING field
                                + "  `b` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s,\n"
                                + "  'key.fields' = 'a; b',\n"
                                + "  %s,\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic,
                        bootstraps,
                        groupId,
                        keyFormatOptions(format),
                        valueFormatOptions(format)));
        final String a2 = "a2";
        final String b2 = "b2";
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO kafka SELECT * FROM (VALUES ('%s', '%s'))", a2, b2))
                .await();
        tEnv.executeSql("DROP TABLE kafka").await();

        // Read data using original create table statement where `a` is still typed as an INT
        tEnv.executeSql(originalCreateTable);

        // If you try to read all the fields, you naturally you get a deserialization exception
        // because of the breaking schema change to field `a`
        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM kafka").await())
                .hasRootCauseInstanceOf(JsonParseException.class)
                .hasRootCauseMessage("Fail to deserialize at field: a.");

        // If you try to read just the fields that didn't have any breaking schema changes
        // the query will work because projection pushdown ensures that fields that aren't needed
        // aren't deserialized
        assertQueryResult(
                "SELECT b FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[b], metadata=[]]], fields=[b])\n",
                Arrays.asList(String.format("+I(%s)", b1), String.format("+I(%s)", b2)));

        // clean up
        cleanupTopic(topic);
    }

    private void setupValueOnlyData(final String format, final String topic)
            throws ExecutionException, InterruptedException {
        createTestTopic(topic, 1, 1);

        final String groupId = getStandardProps().getProperty("group.id");
        final String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `a` STRING,\n"
                                + "  `b` STRING,\n"
                                + "  `c` STRING \n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        topic, bootstraps, groupId, valueFormatOptions(format));
        tEnv.executeSql(createTable);

        final String initialValues = "INSERT INTO kafka (a, b, c) SELECT 'a', 'b', 'c'";
        tEnv.executeSql(initialValues).await();
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void testShortcutWhenNoKeyAndNoConnectorMetadataAndSelectingAllFields(
            final String format) throws Exception {
        final String topic = "testSelectAllFields_" + format + "_" + UUID.randomUUID();
        setupValueOnlyData(format, topic);

        assertQueryResult(
                "SELECT * FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka]], fields=[a, b, c])\n",
                Collections.singletonList("+I(a,b,c)"));

        cleanupTopic(topic);
    }

    @ParameterizedTest(name = "format: {0}")
    @MethodSource("formats")
    public void testShortcutWhenNoKeyAndNoConnectorMetadataAndSelectingOnlyFirstField(
            final String format) throws Exception {
        final String topic = "testSelectFirstField_" + format + "_" + UUID.randomUUID();
        setupValueOnlyData(format, topic);

        assertQueryResult(
                "SELECT a FROM kafka",
                "== Optimized Execution Plan ==\n"
                        + "TableSourceScan(table=[[default_catalog, default_database, kafka, project=[a], metadata=[]]], fields=[a])\n",
                Collections.singletonList("+I(a)"));

        cleanupTopic(topic);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /** Extract the partition id from the row and set it on the record. */
    public static class TestPartitioner implements KafkaPartitioner<RowData> {

        private static final long serialVersionUID = 1L;
        private static final int PARTITION_ID_FIELD_IN_SCHEMA = 0;

        @Override
        public int partition(
                RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return partitions[record.getInt(PARTITION_ID_FIELD_IN_SCHEMA) % partitions.length];
        }
    }

    private static FormatProjectionPushdownLevel projectionPushdownLevel(final String format) {
        if (Objects.equals(format, "avro")) {
            return FormatProjectionPushdownLevel.NONE;
        } else if (Objects.equals(format, "csv")) {
            return FormatProjectionPushdownLevel.TOP_LEVEL;
        } else if (Objects.equals(format, "json")) {
            return FormatProjectionPushdownLevel.ALL;
        } else if (Objects.equals(format, "debezium-json")) {
            return FormatProjectionPushdownLevel.TOP_LEVEL;
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "The level of support the %s format has for projection pushdown is unknown",
                            format));
        }
    }

    private String formatOptions(final String format) {
        final FormatProjectionPushdownLevel formatProjectionPushdownLevel =
                projectionPushdownLevel(format);

        return String.format(
                "'format' = '%s',\n" + "'%s' = '%s',\n" + "'%s' = '%s'",
                format,
                KafkaConnectorOptions.KEY_PROJECTION_PUSHDOWN_LEVEL.key(),
                formatProjectionPushdownLevel,
                KafkaConnectorOptions.VALUE_PROJECTION_PUSHDOWN_LEVEL.key(),
                formatProjectionPushdownLevel);
    }

    private static String keyFormatOptions(final String format) {
        final FormatProjectionPushdownLevel formatProjectionPushdownLevel =
                projectionPushdownLevel(format);

        return String.format(
                "'key.format' = '%s',\n" + "'%s' = '%s'",
                format,
                KafkaConnectorOptions.KEY_PROJECTION_PUSHDOWN_LEVEL.key(),
                formatProjectionPushdownLevel);
    }

    private static String valueFormatOptions(final String format) {
        final FormatProjectionPushdownLevel formatProjectionPushdownLevel =
                projectionPushdownLevel(format);

        return String.format(
                "'value.format' = '%s',\n" + "'%s' = '%s'",
                format,
                KafkaConnectorOptions.VALUE_PROJECTION_PUSHDOWN_LEVEL.key(),
                formatProjectionPushdownLevel);
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static final List<RowData> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value);
            if (rows.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }

        private static List<RowData> getRows() {
            return rows;
        }

        private static List<String> getStringRows() {
            return getRows().stream().map(RowData::toString).collect(Collectors.toList());
        }
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }

    private void cleanupTopic(String topic) {
        ignoreExceptions(
                () -> deleteTestTopic(topic),
                anyCauseMatches(UnknownTopicOrPartitionException.class));
    }

    private static void cancelJob(TableResult tableResult) {
        if (tableResult != null && tableResult.getJobClient().isPresent()) {
            ignoreExceptions(
                    () -> tableResult.getJobClient().get().cancel().get(),
                    anyCauseMatches(FlinkJobTerminatedWithoutCancellationException.class),
                    anyCauseMatches(
                            "MiniCluster is not yet running or has already been shut down."));
        }
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

    private void assertQueryResult(
            final String query, final String expectedPlanEndsWith, final List<String> expected)
            throws Exception {
        final Table table = tEnv.sqlQuery(query);

        assertThat(table.explain()).endsWith(expectedPlanEndsWith);

        final DataStream<RowData> result =
                tEnv.toRetractStream(table, RowData.class).map(tuple -> tuple.f1);
        final TestingSinkFunction sink = new TestingSinkFunction(expected.size());
        result.addSink(sink).setParallelism(1);
        try {
            env.execute("Job_2");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }
        assertThat(TestingSinkFunction.getStringRows()).isEqualTo(expected);
    }
}
