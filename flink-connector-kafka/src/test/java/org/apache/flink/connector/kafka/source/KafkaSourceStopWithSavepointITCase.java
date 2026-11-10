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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Map;

import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv.NUM_PARTITIONS;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the offsets that {@link KafkaSource} commits to Kafka on stop-with-savepoint. */
@ResourceLock("KafkaTestBase")
class KafkaSourceStopWithSavepointITCase {
    private static final String SOURCE_TOPIC = "KafkaSourceStopWithSavepointITCase_source";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @TempDir private Path savepointBasePath;

    @BeforeAll
    static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.createTestTopic(SOURCE_TOPIC);
        KafkaSourceTestEnv.produceToKafka(KafkaSourceTestEnv.getRecordsForTopic(SOURCE_TOPIC));
    }

    @AfterAll
    static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @ParameterizedTest(name = "drain = {0}")
    @ValueSource(booleans = {true, false})
    void testCommitOffsetsOfSavepoint(boolean drain, @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        final String groupId = "testCommitOffsetsOfSavepoint-" + drain;
        // Each run waits for all its records in its own sink topic.
        final String sinkTopic = "KafkaSourceStopWithSavepointITCase_sink-" + drain;
        KafkaSourceTestEnv.createTestTopic(sinkTopic);
        // Without periodic checkpoints the savepoint is the only checkpoint that can commit.
        final JobGraph jobGraph = getJobGraph(groupId, sinkTopic);
        miniCluster.submitJob(jobGraph).get();
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobGraph.getJobID(), false);
        KafkaSourceTestEnv.waitForRecordsInTopic(
                sinkTopic, NUM_PARTITIONS * NUM_RECORDS_PER_PARTITION);

        miniCluster
                .stopWithSavepoint(
                        jobGraph.getJobID(),
                        savepointBasePath.toString(),
                        drain,
                        SavepointFormatType.DEFAULT)
                .get();
        miniCluster.requestJobResult(jobGraph.getJobID()).get();

        final Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                KafkaSourceTestEnv.listCommittedOffsets(groupId);
        assertThat(committedOffsets).hasSize(NUM_PARTITIONS);
        assertThat(committedOffsets.values())
                .extracting(OffsetAndMetadata::offset)
                .containsOnly((long) NUM_RECORDS_PER_PARTITION);
    }

    private static JobGraph getJobGraph(String groupId, String sinkTopic) {
        final KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                        .setTopics(SOURCE_TOPIC)
                        .setGroupId(groupId)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        final KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(sinkTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source").sinkTo(sink);
        return env.getStreamGraph().getJobGraph();
    }
}
