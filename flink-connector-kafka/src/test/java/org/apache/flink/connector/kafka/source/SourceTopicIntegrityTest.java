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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.metadata.TopicIntegrityException;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Integration tests for topic integrity checking in KafkaSource. */
@ResourceLock("KafkaTestBase")
public class SourceTopicIntegrityTest {
    private static final Logger LOG = LoggerFactory.getLogger(SourceTopicIntegrityTest.class);
    private static final String SOURCE_TOPIC_NAME = "SourceTopicIntegrityTest_source-topic";
    private static final String SOURCE_TOPIC_PATTERN = "SourceTopicIntegrityTest_source.*";
    private static final String SINK_TOPIC_NAME = "SourceTopicIntegrityTest_sink-topic";
    private static final long DISCOVERY_INTERVAL = 50L;
    private static final Duration ERROR_DISCOVERY_TIMEOUT = Duration.ofSeconds(2);
    @TempDir private Path savepointBasePath;

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(3)
                            .setNumberSlotsPerTaskManager(3)
                            .build());

    public void setupKafka(boolean createTopics) throws Throwable {
        KafkaSourceTestEnv.setup();
        if (createTopics) {
            KafkaSourceTestEnv.createTestTopic(SOURCE_TOPIC_NAME);
            KafkaSourceTestEnv.produceToKafka(
                    KafkaSourceTestEnv.getRecordsForTopic(SOURCE_TOPIC_NAME));
            KafkaSourceTestEnv.createTestTopic(SINK_TOPIC_NAME);
        }
    }

    @BeforeEach
    public void setup() throws Throwable {
        setupKafka(true);
    }

    @AfterEach
    public void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    private static Properties getSourceProperties() {
        final Properties props = new Properties();
        props.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                String.valueOf(DISCOVERY_INTERVAL));
        return props;
    }

    private final JobGraph getJobGraph(
            Configuration extraConf, SourceSubscriptionMode sourceSubscriptionMode)
            throws Throwable {
        KafkaSourceBuilder<String> sourceBuilder =
                KafkaSource.<String>builder()
                        .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .enableTopicIntegrityCheck()
                        .setProperties(getSourceProperties());
        switch (sourceSubscriptionMode) {
            case PARTITIONS:
                sourceBuilder.setPartitions(
                        IntStream.range(0, KafkaSourceTestEnv.NUM_PARTITIONS)
                                .mapToObj(i -> new TopicPartition(SOURCE_TOPIC_NAME, i))
                                .collect(Collectors.toSet()));
                break;
            case TOPICS:
                sourceBuilder.setTopics(SOURCE_TOPIC_NAME);
                break;
            case PATTERN:
                sourceBuilder.setTopicPattern(Pattern.compile(SOURCE_TOPIC_PATTERN));
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported subscription mode " + sourceSubscriptionMode);
        }

        KafkaSource<String> source = sourceBuilder.build();
        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(SINK_TOPIC_NAME)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .setPartitioner(new FlinkFixedPartitioner())
                                        .build())
                        .build();
        Configuration configuration = new Configuration();
        configuration.addAll(extraConf);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        stream.sinkTo(sink);
        return env.getStreamGraph().getJobGraph();
    }

    private static Stream<Arguments> testTopicIntegrityFailureArgsProvider() {
        return Stream.of(
                Arguments.of(SourceSubscriptionMode.PARTITIONS, false),
                Arguments.of(SourceSubscriptionMode.TOPICS, false),
                Arguments.of(SourceSubscriptionMode.PATTERN, false),
                Arguments.of(SourceSubscriptionMode.PARTITIONS, true),
                Arguments.of(SourceSubscriptionMode.TOPICS, true),
                Arguments.of(SourceSubscriptionMode.PATTERN, true));
    }

    /**
     * Test that job fails if the source topic id does not match the current topic id.
     *
     * @throws Throwable
     */
    @ParameterizedTest
    @MethodSource("testTopicIntegrityFailureArgsProvider")
    public void testTopicIntegrityFailure(
            SourceSubscriptionMode sourceSubscriptionMode,
            boolean recreateTopic,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Throwable {
        JobGraph firstJobGraph = getJobGraph(new Configuration(), sourceSubscriptionMode);
        miniCluster.submitJob(firstJobGraph).get();
        org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning(
                miniCluster, firstJobGraph.getJobID(), true);
        String savepointPath =
                miniCluster
                        .stopWithSavepoint(
                                firstJobGraph.getJobID(),
                                savepointBasePath.toFile().toString(),
                                false,
                                SavepointFormatType.DEFAULT)
                        .get();

        // Restart kafka to simulate topic recreation
        tearDown();
        setupKafka(recreateTopic);

        // resume from savepoint
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        JobGraph secondJobGraph = getJobGraph(configuration, sourceSubscriptionMode);
        miniCluster.submitJob(secondJobGraph).get();
        final JobID secondJobId = secondJobGraph.getJobID();
        final String expectedError =
                String.format(
                        recreateTopic ? "%s: Topic %s was recreated" : "%s: Topic %s is missing",
                        TopicIntegrityException.class.getName(),
                        SOURCE_TOPIC_NAME);
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        return miniCluster
                                .getArchivedExecutionGraph(secondJobId)
                                .get()
                                .getFailureInfo()
                                .getExceptionAsString()
                                .contains(expectedError);
                    } catch (Exception e) {
                        LOG.warn(
                                "Error while fetching execution graph for job {}: {}",
                                secondJobId,
                                e.getMessage());
                        return false;
                    }
                },
                ERROR_DISCOVERY_TIMEOUT,
                Duration.ofMillis(DISCOVERY_INTERVAL),
                "Waiting for job to fail with " + expectedError);
    }

    /**
     * Test that job can resume and run successfully if the source topic id matches the one before
     * restart.
     *
     * @throws Throwable
     */
    @ParameterizedTest
    @EnumSource(
            value = SourceSubscriptionMode.class,
            names = {"PARTITIONS", "TOPICS", "PATTERN"})
    public void testTopicIntegritySuccess(
            SourceSubscriptionMode sourceSubscriptionMode,
            @InjectMiniCluster MiniCluster miniCluster)
            throws Throwable {
        JobGraph firstJobGraph = getJobGraph(new Configuration(), sourceSubscriptionMode);
        miniCluster.submitJob(firstJobGraph).get();
        org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning(
                miniCluster, firstJobGraph.getJobID(), true);
        final int initialExpectedRecords =
                KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION * KafkaSourceTestEnv.NUM_PARTITIONS;
        KafkaSourceTestEnv.waitForRecordsInTopic(SINK_TOPIC_NAME, initialExpectedRecords);
        String savepointPath =
                miniCluster
                        .stopWithSavepoint(
                                firstJobGraph.getJobID(),
                                savepointBasePath.toFile().toString(),
                                false,
                                SavepointFormatType.DEFAULT)
                        .get();

        // generate more records to the same topic
        KafkaSourceTestEnv.produceToKafka(KafkaSourceTestEnv.getRecordsForTopic(SOURCE_TOPIC_NAME));
        final int expectedTotalRecords = 2 * initialExpectedRecords;

        // resume from savepoint
        Configuration configuration = new Configuration();
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        JobGraph secondJobGraph = getJobGraph(configuration, sourceSubscriptionMode);
        miniCluster.submitJob(secondJobGraph).get();
        final JobID secondJobId = secondJobGraph.getJobID();
        org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning(
                miniCluster, secondJobId, true);

        // Expect the job has run and produced the extra records to the sink
        KafkaSourceTestEnv.waitForRecordsInTopic(SINK_TOPIC_NAME, expectedTotalRecords);
    }

    private enum SourceSubscriptionMode {
        PARTITIONS,
        TOPICS,
        PATTERN
    }
}
