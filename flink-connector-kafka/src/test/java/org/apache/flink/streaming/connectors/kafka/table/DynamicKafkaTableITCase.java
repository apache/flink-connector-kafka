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

import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.utils.TableTestMatchers;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectRows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** IT cases for the Dynamic Kafka table source. */
class DynamicKafkaTableITCase extends KafkaTableTestBase {

    @BeforeEach
    void before() {
        env.setParallelism(1);
    }

    @Test
    void testDynamicKafkaSourceWithClusterMetadata() throws Exception {
        final String topic = "dynamic_table_cluster_metadata_" + UUID.randomUUID();
        createTestTopic(topic, 1, 1);

        final String bootstrapServers = getBootstrapServers();
        final String groupId = getStandardProps().getProperty("group.id");

        final String createSinkTable =
                String.format(
                        "CREATE TABLE kafka_sink (\n"
                                + "  name STRING,\n"
                                + "  cnt INT\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  'json.fail-on-missing-field' = 'false',\n"
                                + "  'json.ignore-parse-errors' = 'true'\n"
                                + ")",
                        KafkaDynamicTableFactory.IDENTIFIER, topic, bootstrapServers);
        tEnv.executeSql(createSinkTable);

        final String insertData =
                "INSERT INTO kafka_sink\n"
                        + "SELECT * FROM (VALUES\n"
                        + "  ('Alice', 1),\n"
                        + "  ('Bob', 2)\n"
                        + ") AS t(name, cnt)";
        tEnv.executeSql(insertData).await();

        final String createSourceTable =
                String.format(
                        "CREATE TABLE kafka_source (\n"
                                + "  name STRING,\n"
                                + "  cnt INT,\n"
                                + "  kafka_cluster STRING METADATA FROM 'kafka_cluster',\n"
                                + "  topic STRING METADATA FROM 'topic'\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'stream-ids' = '%s',\n"
                                + "  'metadata-service' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'format' = 'json',\n"
                                + "  'json.fail-on-missing-field' = 'false',\n"
                                + "  'json.ignore-parse-errors' = 'true'\n"
                                + ")",
                        DynamicKafkaTableFactory.IDENTIFIER,
                        topic,
                        MultiClusterMetadataService.class.getName(),
                        bootstrapServers,
                        groupId);
        tEnv.executeSql(createSourceTable);

        Table result = tEnv.sqlQuery("SELECT name, cnt, kafka_cluster, topic FROM kafka_source");
        List<Row> actual = collectRows(result, 4);
        actual.sort(
                Comparator.comparing((Row row) -> row.getField(0).toString())
                        .thenComparing(row -> row.getField(2).toString()));

        List<Row> expected =
                Arrays.asList(
                        Row.of("Alice", 1, MultiClusterMetadataService.CLUSTER_0, topic),
                        Row.of("Alice", 1, MultiClusterMetadataService.CLUSTER_1, topic),
                        Row.of("Bob", 2, MultiClusterMetadataService.CLUSTER_0, topic),
                        Row.of("Bob", 2, MultiClusterMetadataService.CLUSTER_1, topic));

        assertThat(actual).satisfies(matching(TableTestMatchers.deepEqualTo(expected, false)));

        cleanupTopic(topic);
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

    /**
     * Test metadata service that maps a single stream id to two logical clusters.
     *
     * <p>Each logical cluster points to a distinct topic, allowing the test to validate that the
     * dynamic source injects the correct cluster id for each record.
     */
    public static final class MultiClusterMetadataService implements KafkaMetadataService {

        static final String CLUSTER_0 = "cluster-0";
        static final String CLUSTER_1 = "cluster-1";

        private final Properties cluster0Props;
        private final Properties cluster1Props;

        public MultiClusterMetadataService(Properties properties) {
            Properties baseProps = new Properties();
            baseProps.putAll(properties);

            cluster0Props = new Properties();
            cluster0Props.putAll(baseProps);
            cluster0Props.setProperty(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    baseProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-tests")
                            + "-"
                            + CLUSTER_0);

            cluster1Props = new Properties();
            cluster1Props.putAll(baseProps);
            cluster1Props.setProperty(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    baseProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-tests")
                            + "-"
                            + CLUSTER_1);
        }

        @Override
        public Set<KafkaStream> getAllStreams() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
            Map<String, KafkaStream> streams = new LinkedHashMap<>();
            for (String streamId : streamIds) {
                streams.put(streamId, createKafkaStream(streamId));
            }
            return streams;
        }

        @Override
        public boolean isClusterActive(String kafkaClusterId) {
            return CLUSTER_0.equals(kafkaClusterId) || CLUSTER_1.equals(kafkaClusterId);
        }

        @Override
        public void close() {
            // nothing to close
        }

        private KafkaStream createKafkaStream(String streamId) {
            Map<String, ClusterMetadata> clusterMetadata = new LinkedHashMap<>();
            clusterMetadata.put(
                    CLUSTER_0,
                    new ClusterMetadata(
                            Collections.singleton(streamId), cluster0Props, null, null));
            clusterMetadata.put(
                    CLUSTER_1,
                    new ClusterMetadata(
                            Collections.singleton(streamId), cluster1Props, null, null));
            return new KafkaStream(streamId, clusterMetadata);
        }
    }
}
