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

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectRows;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for the Dynamic Kafka table source. */
class DynamicKafkaTableITCase extends KafkaTableTestBase {

    @BeforeEach
    void before() {
        env.setParallelism(1);
    }

    @Test
    void testDynamicKafkaSource() throws Exception {
        final String topic = "dynamic_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        Properties standardProps = getStandardProps();
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createSinkTable =
                String.format(
                        "create table kafka_sink (\n"
                                + "  id int,\n"
                                + "  payload string\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'format' = 'csv'\n"
                                + ")",
                        KafkaDynamicTableFactory.IDENTIFIER, topic, bootstraps, groupId);

        final String createSourceTable =
                String.format(
                        "create table dynamic_kafka (\n"
                                + "  id int,\n"
                                + "  payload string\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'stream-ids' = '%s',\n"
                                + "  'metadata-service' = 'single-cluster',\n"
                                + "  'metadata-service.cluster-id' = 'cluster-0',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'format' = 'csv'\n"
                                + ")",
                        DynamicKafkaTableFactory.IDENTIFIER, topic, bootstraps, groupId);

        tEnv.executeSql(createSinkTable);
        tEnv.executeSql(createSourceTable);

        tEnv.executeSql("INSERT INTO kafka_sink VALUES " + "(1, 'a'), " + "(2, 'b'), " + "(3, 'c')")
                .await();

        Table result = tEnv.sqlQuery("SELECT id, payload FROM dynamic_kafka");
        List<Row> rows = collectRows(result, 3);

        assertThat(rows)
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(Row.of(1, "a"), Row.of(2, "b"), Row.of(3, "c")));

        cleanupTopic(topic);
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
            assertThat(ex).satisfiesAnyOf(ignoreIf);
        }
    }
}
