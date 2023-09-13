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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** ITCase tests class for {@link FlinkKafkaConsumer}. */
@TestInstance(Lifecycle.PER_CLASS)
public class FlinkKafkaConsumerITCase {
    private static final String TOPIC1 = "FlinkKafkaConsumerITCase_topic1";

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(new Configuration())
                            .build());

    @BeforeAll
    public void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(
                TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopicWithoutTimestamp);
    }

    @AfterAll
    public void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testStopWithSavepoint(@TempDir Path savepointsDir) throws Exception {
        Configuration config =
                new Configuration()
                        .set(
                                CheckpointingOptions.SAVEPOINT_DIRECTORY,
                                savepointsDir.toUri().toString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaSourceTestEnv.brokerConnectionStrings);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testStopWithSavepoint");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer<Integer> kafkaConsumer =
                new FlinkKafkaConsumer<>(
                        TOPIC1,
                        new TypeInformationSerializationSchema<>(
                                BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig()),
                        properties);
        DataStreamSource<Integer> stream = env.addSource(kafkaConsumer);

        ProgressLatchingIdentityFunction.resetBeforeUse();
        stream.map(new ProgressLatchingIdentityFunction()).addSink(new DiscardingSink<>());

        JobClient jobClient = env.executeAsync();

        ProgressLatchingIdentityFunction.getProgressLatch().await();

        // Check that stopWithSavepoint completes successfully
        jobClient.stopWithSavepoint(false, null, SavepointFormatType.CANONICAL).get();
        // TODO: ideally we should test recovery, that there were no data losses etc, but this
        // is already a deprecated class, so I'm not adding new tests for that now.
    }

    private static class ProgressLatchingIdentityFunction implements MapFunction<Integer, Integer> {

        static CountDownLatch progressLatch;

        static void resetBeforeUse() {
            progressLatch = new CountDownLatch(1);
        }

        public static CountDownLatch getProgressLatch() {
            return progressLatch;
        }

        @Override
        public Integer map(Integer integer) throws Exception {
            progressLatch.countDown();
            return integer;
        }
    }
}
