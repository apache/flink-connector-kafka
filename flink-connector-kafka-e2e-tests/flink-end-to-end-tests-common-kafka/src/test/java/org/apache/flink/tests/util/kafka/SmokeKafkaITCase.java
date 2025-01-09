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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.JobSubmission;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;

/** smoke test for the kafka connectors. */
@ExtendWith({TestLoggerExtension.class})
@Testcontainers
class SmokeKafkaITCase {

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final String EXAMPLE_JAR_MATCHER = "flink-streaming-kafka-test.*";

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(SmokeKafkaITCase.class)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .logger(KafkaUtil.getLogger("flink", SmokeKafkaITCase.class))
                    .dependsOn(KAFKA_CONTAINER)
                    .build();

    @RegisterExtension
    public static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.basedOn(getConfiguration()))
                    .withTestcontainersSettings(TESTCONTAINERS_SETTINGS)
                    .build();

    private static AdminClient admin;
    private static KafkaProducer<Void, Integer> producer;

    private static Configuration getConfiguration() {
        // modify configuration to have enough slots
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        flinkConfig.set(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        flinkConfig.set(
                org.apache.flink.configuration.JobManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.ofMebiBytes(1024));
        flinkConfig.set(
                org.apache.flink.configuration.TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                MemorySize.ofMebiBytes(1024));
        // Workaround for FLINK-36454 ; default config is entirely overwritten
        flinkConfig.setString(
                "env.java.opts.all",
                "--add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED");
        return flinkConfig;
    }

    @BeforeAll
    static void setUp() {
        final Map<String, Object> adminProperties = new HashMap<>();
        adminProperties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(adminProperties);
        final Properties producerProperties = new Properties();
        producerProperties.putAll(adminProperties);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producer = new KafkaProducer<>(producerProperties);
    }

    @AfterAll
    static void teardown() {
        admin.close();
        producer.close();
    }

    @Test
    public void testKafka() throws Exception {
        final Path kafkaExampleJar = ResourceTestUtils.getResource(EXAMPLE_JAR_MATCHER);

        final String inputTopic = "test-input-" + "-" + UUID.randomUUID();
        final String outputTopic = "test-output" + "-" + UUID.randomUUID();

        // create the required topics
        final short replicationFactor = 1;
        admin.createTopics(
                        Arrays.asList(
                                new NewTopic(inputTopic, 1, replicationFactor),
                                new NewTopic(outputTopic, 1, replicationFactor)))
                .all()
                .get();

        producer.send(new ProducerRecord<>(inputTopic, 1));
        producer.send(new ProducerRecord<>(inputTopic, 2));
        producer.send(new ProducerRecord<>(inputTopic, 3));

        // run the Flink job
        FLINK.submitJob(
                new JobSubmission.JobSubmissionBuilder(kafkaExampleJar)
                        .setDetached(false)
                        .addArgument("--input-topic", inputTopic)
                        .addArgument("--output-topic", outputTopic)
                        .addArgument("--prefix", "PREFIX")
                        .addArgument(
                                "--bootstrap.servers",
                                String.join(
                                        ",",
                                        KAFKA_CONTAINER.getBootstrapServers(),
                                        KAFKA_CONTAINER.getNetworkAliases().stream()
                                                .map(
                                                        host ->
                                                                String.join(
                                                                        ":",
                                                                        host,
                                                                        Integer.toString(9092)))
                                                .collect(Collectors.joining(","))))
                        .addArgument("--group.id", "myconsumer")
                        .addArgument("--auto.offset.reset", "earliest")
                        .addArgument("--transaction.timeout.ms", "900000")
                        .addArgument("--flink.partition-discovery.interval-millis", "1000")
                        .build());
        final Properties consumerProperties = new Properties();
        consumerProperties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, VoidDeserializer.class);
        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        List<Integer> records =
                KafkaUtil.drainAllRecordsFromTopic(outputTopic, consumerProperties).stream()
                        .map(r -> ByteBuffer.wrap(r.value()).getInt())
                        .collect(Collectors.toList());
        assertThat(records).hasSize(3).containsExactly(1, 2, 3);
    }
}
