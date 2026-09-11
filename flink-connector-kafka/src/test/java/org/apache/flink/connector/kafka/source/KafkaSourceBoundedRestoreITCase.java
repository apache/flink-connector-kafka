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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A bounded {@link KafkaSource} must still finish when the job is restored from a savepoint, a
 * retained checkpoint, or after a JobManager failover.
 *
 * <p>All three recreate the enumerator from state, and the recreated enumerator finds every
 * subscribed partition already assigned. On an unfixed enumerator that empty partition change makes
 * {@code checkPartitionChanges} return before it reaches the only place that marks the discovery as
 * finished, so the restored readers consume up to their stopping offset and then wait forever for a
 * {@code NoMoreSplitsEvent} that is never sent (FLINK-31006).
 *
 * <p>Do not weaken this test by enabling partition discovery, by putting the stopping offset within
 * the records produced before the savepoint, or by running it in batch mode: the first two stop the
 * restored partition change from being empty, and the third removes the checkpoint coordinator that
 * the savepoint needs.
 */
@ResourceLock("KafkaTestBase")
public class KafkaSourceBoundedRestoreITCase {

    private static final String TOPIC = "KafkaSourceBoundedRestoreITCase-topic";

    /** Records produced before the savepoint. */
    private static final int RECORDS_BEFORE_SAVEPOINT = 10;

    /** Records produced while the job is stopped, which the restored job must still consume. */
    private static final int RECORDS_AFTER_SAVEPOINT = 5;

    private static final int TOTAL_RECORDS = RECORDS_BEFORE_SAVEPOINT + RECORDS_AFTER_SAVEPOINT;
    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    /** Values seen by the pipeline, across both runs of the job. */
    private static final Set<Integer> COLLECTED = ConcurrentHashMap.newKeySet();

    @TempDir private Path savepointBasePath;

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @BeforeEach
    public void setup() throws Throwable {
        COLLECTED.clear();
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.createTestTopic(TOPIC, 1, 1);
        produce(0, RECORDS_BEFORE_SAVEPOINT);
    }

    @AfterEach
    public void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    private static void produce(int fromValue, int count) throws Throwable {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int i = fromValue; i < fromValue + count; i++) {
            records.add(new ProducerRecord<>(TOPIC, 0, "key-" + i, i));
        }
        KafkaSourceTestEnv.produceToKafka(records);
    }

    /**
     * The job graph is built by one helper for both runs so that the restored job has the identical
     * topology, and therefore the identical operator IDs, as the job the savepoint was taken from.
     */
    private JobGraph getJobGraph(Configuration extraConf) {
        KafkaSource<Integer> source =
                KafkaSource.<Integer>builder()
                        .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
                        .setTopics(TOPIC)
                        .setGroupId("KafkaSourceBoundedRestoreITCase")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        // Beyond what has been produced so far, so the first run cannot finish on
                        // its own and the savepoint is always taken from a running source.
                        .setBounded(
                                OffsetsInitializer.offsets(
                                        Collections.singletonMap(
                                                new TopicPartition(TOPIC, 0),
                                                (long) TOTAL_RECORDS)))
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        IntegerDeserializer.class))
                        .build();

        Configuration configuration = new Configuration();
        configuration.addAll(extraConf);
        // A hang must surface as a timeout on the job, not as restart churn.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                        .uid("kafka-source")
                        .map(
                                (MapFunction<Integer, Integer>)
                                        value -> {
                                            COLLECTED.add(value);
                                            return value;
                                        })
                        .uid("collector");
        stream.sinkTo(new DiscardingSink<>()).uid("sink");
        return env.getStreamGraph().getJobGraph();
    }

    /**
     * Waits for the job to finish, failing immediately if it reaches any other terminal state so
     * that an unrelated failure is reported as itself rather than as the hang under test.
     */
    private static void awaitJobFinished(MiniCluster miniCluster, JobID jobId) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    final JobStatus status;
                    try {
                        status = miniCluster.getJobStatus(jobId).get();
                    } catch (Exception e) {
                        // The job may not be known to the cluster yet.
                        return false;
                    }
                    if (status == JobStatus.FINISHED) {
                        return true;
                    }
                    if (status.isGloballyTerminalState()) {
                        throw new IllegalStateException(
                                String.format(
                                        "The job reached %s instead of finishing. %s",
                                        status, failureCause(miniCluster, jobId)));
                    }
                    return false;
                },
                TIMEOUT,
                Duration.ofMillis(50),
                "The restored bounded job did not finish; the readers were never told that no "
                        + "more splits are coming (FLINK-31006)");
    }

    private static String failureCause(MiniCluster miniCluster, JobID jobId) {
        try {
            final ErrorInfo failureInfo =
                    miniCluster.getArchivedExecutionGraph(jobId).get().getFailureInfo();
            return failureInfo == null ? "No failure info." : failureInfo.getExceptionAsString();
        } catch (Exception e) {
            return "Failure info unavailable: " + e;
        }
    }

    @Test
    public void testBoundedSourceFinishesAfterRestoreFromSavepoint(
            @InjectMiniCluster MiniCluster miniCluster) throws Throwable {
        JobGraph firstJobGraph = getJobGraph(new Configuration());
        JobID firstJobId = firstJobGraph.getJobID();
        miniCluster.submitJob(firstJobGraph).get();

        CommonTestUtils.waitUtil(
                () -> COLLECTED.size() >= RECORDS_BEFORE_SAVEPOINT,
                TIMEOUT,
                Duration.ofMillis(50),
                "The first run did not consume the records produced before the savepoint");

        String savepointPath =
                miniCluster
                        .stopWithSavepoint(
                                firstJobId,
                                savepointBasePath.toFile().toString(),
                                false,
                                SavepointFormatType.CANONICAL)
                        .get();
        assertThat(savepointPath).isNotBlank();

        produce(RECORDS_BEFORE_SAVEPOINT, RECORDS_AFTER_SAVEPOINT);

        Configuration restoreConf = new Configuration();
        restoreConf.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
        JobGraph secondJobGraph = getJobGraph(restoreConf);
        JobID secondJobId = secondJobGraph.getJobID();
        miniCluster.submitJob(secondJobGraph).get();

        // The restored source reaches its stopping offset and then needs the enumerator to tell it
        // that no more splits are coming. Without that signal the job hangs here.
        awaitJobFinished(miniCluster, secondJobId);

        assertThat(COLLECTED)
                .as("Every produced record must be consumed across the two runs")
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, TOTAL_RECORDS).boxed().collect(Collectors.toList()));
    }
}
