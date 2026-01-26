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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.flink.configuration.StateRecoveryOptions.SAVEPOINT_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** The test for creation savepoint for migration tests for the Kafka Sink. */
@Testcontainers
@ResourceLock("KafkaTestBase")
class KafkaSourceMigrationITCase {
    public static final String KAFKA_SOURCE_UID = "kafka-source-operator-uid";
    // Directory to store the savepoints in src/test/resources
    private static final Path KAFKA_SOURCE_SAVEPOINT_PATH =
            Path.of("src/test/resources/kafka-source-savepoint").toAbsolutePath();

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    public static final int NUM_RECORDS =
            KafkaSourceTestEnv.NUM_PARTITIONS * KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION;
    private static final String TOPIC = "topic";

    @RegisterExtension
    private static final SharedObjectsExtension SHARED_OBJECTS = SharedObjectsExtension.create();

    @BeforeEach
    void setupEnv() throws Throwable {
        // restarting Kafka with each migration test because we use the same topic underneath
        KafkaSourceTestEnv.setup();
    }

    @AfterEach
    void removeEnv() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    static Stream<Arguments> getKafkaSourceSavepoint() throws IOException {
        return Files.walk(KAFKA_SOURCE_SAVEPOINT_PATH)
                .filter(
                        f ->
                                Files.isDirectory(f)
                                        && f.getFileName().toString().startsWith("savepoint"))
                // allow
                .map(KAFKA_SOURCE_SAVEPOINT_PATH::relativize)
                .map(Arguments::arguments);
    }

    @Disabled("Enable if you want to create savepoint of KafkaSource")
    @Test
    void createAndStoreSavepoint(
            @InjectClusterClient ClusterClient<?> clusterClient,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Throwable {

        // this is the part that has been read already in the savepoint
        final List<ProducerRecord<String, Integer>> writtenRecords = writeInitialData();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.getConfig().enableObjectReuse();

        final KafkaSource<ConsumerRecord<byte[], byte[]>> source = createSource();
        final int enumVersion = source.getEnumeratorCheckpointSerializer().getVersion();
        final int splitVersion = source.getSplitSerializer().getVersion();
        String testCase = String.format("enum%s-split%s", enumVersion, splitVersion);

        Path savepointPath = KAFKA_SOURCE_SAVEPOINT_PATH.resolve(testCase);
        Files.createDirectories(savepointPath);

        final SharedReference<ConcurrentLinkedQueue<ConsumerRecord<byte[], byte[]>>> readRecords =
                SHARED_OBJECTS.add(new ConcurrentLinkedQueue<>());
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestDataSource")
                .uid(KAFKA_SOURCE_UID)
                .map(r -> readRecords.get().add(r));

        final JobClient jobClient = env.executeAsync();
        final JobID jobID = jobClient.getJobID();

        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient.getJobID(), false);
        CommonTestUtils.waitUntilCondition(
                () -> readRecords.get().size() >= writtenRecords.size(), 100L, 100);
        CompletableFuture<String> savepointFuture =
                clusterClient.stopWithSavepoint(
                        jobID, false, savepointPath.toString(), SavepointFormatType.NATIVE);
        savepointFuture.get(2, TimeUnit.MINUTES);

        final long maxTS = getMaxTS(writtenRecords);
        assertThat(readRecords.get()).hasSize(NUM_RECORDS).allMatch(r -> r.timestamp() <= maxTS);
    }

    private static List<ProducerRecord<String, Integer>> writeInitialData() throws Throwable {
        KafkaSourceTestEnv.createTestTopic(TOPIC);
        final List<ProducerRecord<String, Integer>> writtenRecords =
                KafkaSourceTestEnv.getRecordsForTopic(TOPIC);
        KafkaSourceTestEnv.produceToKafka(writtenRecords);
        return writtenRecords;
    }

    private static long getMaxTS(List<ProducerRecord<String, Integer>> writtenRecords) {
        return writtenRecords.stream().mapToLong(ProducerRecord::timestamp).max().orElseThrow();
    }

    private static KafkaSource<ConsumerRecord<byte[], byte[]>> createSource() {
        return KafkaSource.<ConsumerRecord<byte[], byte[]>>builder()
                .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                .setTopics(TOPIC)
                .setDeserializer(new ForwardingDeserializer())
                .build();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getKafkaSourceSavepoint")
    void testRestoreFromSavepointWithCurrentVersion(
            Path savepointPath, @InjectMiniCluster MiniCluster miniCluster) throws Throwable {
        // this is the part that has been read already in the savepoint
        final List<ProducerRecord<String, Integer>> existingRecords = writeInitialData();
        // the new data supposed to be read after resuming from the savepoint
        final List<ProducerRecord<String, Integer>> writtenRecords =
                KafkaSourceTestEnv.getRecordsForTopicWithoutTimestamp(TOPIC);
        KafkaSourceTestEnv.produceToKafka(writtenRecords);

        final Configuration configuration = new Configuration();
        configuration.set(
                SAVEPOINT_PATH, KAFKA_SOURCE_SAVEPOINT_PATH.resolve(savepointPath).toString());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.getConfig().enableObjectReuse();

        final KafkaSource<ConsumerRecord<byte[], byte[]>> source = createSource();

        final SharedReference<ConcurrentLinkedQueue<ConsumerRecord<byte[], byte[]>>> readRecords =
                SHARED_OBJECTS.add(new ConcurrentLinkedQueue<>());
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestDataSource")
                .uid(KAFKA_SOURCE_UID)
                .map(r -> readRecords.get().add(r));

        StreamGraph streamGraph = env.getStreamGraph();

        final JobClient jobClient = env.executeAsync(streamGraph);
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient.getJobID(), false);
        CommonTestUtils.waitUntilCondition(
                () -> readRecords.get().size() >= writtenRecords.size(), 100L, 100);

        jobClient.cancel().get(10, TimeUnit.SECONDS);

        // records of old run, all have artificial timestamp up to maxPreviousTS (=9000)
        final long maxPreviousTS = getMaxTS(existingRecords);
        // new records should all have epoch timestamp assigned by broker and should be much,
        // much
        // larger than maxPreviousTS, so we can verify exactly once by checking for timestamp
        assertThat(readRecords.get())
                .hasSize(writtenRecords.size()) // smaller size indicates deadline passed
                .allMatch(r -> r.timestamp() > maxPreviousTS);
    }

    private static class ForwardingDeserializer
            implements KafkaRecordDeserializationSchema<ConsumerRecord<byte[], byte[]>> {
        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record,
                Collector<ConsumerRecord<byte[], byte[]>> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
            return TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() {});
        }
    }
}
