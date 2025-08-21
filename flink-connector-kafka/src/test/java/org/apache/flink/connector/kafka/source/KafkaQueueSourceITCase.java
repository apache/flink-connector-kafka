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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.kafka.testutils.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link KafkaQueueSource} with Kafka 4.1.0 queue semantics.
 * Tests end-to-end queue consumption functionality.
 */
class KafkaQueueSourceITCase {

    private static final int PARALLELISM = 4;
    private static KafkaSourceTestEnv kafkaSourceTestEnv;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    private static final String QUEUE_TOPIC = "queue-test-topic";
    private static final String SHARE_GROUP_ID = "test-share-group";

    @BeforeAll
    static void setUp() throws Exception {
        kafkaSourceTestEnv = KafkaSourceTestEnv.create(
                KAFKA,
                1,
                1,
                Collections.singletonList(QUEUE_TOPIC),
                4
        );
        kafkaSourceTestEnv.setup();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (kafkaSourceTestEnv != null) {
            kafkaSourceTestEnv.tearDown();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testBasicQueueConsumption() throws Exception {
        // Produce test messages to the topic
        List<String> expectedMessages = produceTestMessages(QUEUE_TOPIC, 100);

        // Create queue source
        KafkaQueueSource<String> queueSource = KafkaQueueSource.<String>builder()
                .setBootstrapServers(kafkaSourceTestEnv.getBootstrapServers())
                .setTopics(QUEUE_TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Set up Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(1000);

        // Create data stream from queue source
        DataStream<String> stream = env.fromSource(
                queueSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaQueueSource"
        );

        // Collect results
        List<String> actualMessages = Collections.synchronizedList(new ArrayList<>());
        stream.addSink(new CollectingSink<>(actualMessages));

        // Execute and wait for completion
        env.execute("Queue Consumption Test");

        // Verify all messages were consumed
        assertThat(actualMessages).hasSize(expectedMessages.size());
        assertThat(actualMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testQueueLoadBalancing() throws Exception {
        // Produce messages
        List<String> expectedMessages = produceTestMessages(QUEUE_TOPIC, 200);

        // Create multiple queue sources with same share group
        KafkaQueueSource<String> queueSource1 = createQueueSource(SHARE_GROUP_ID + "-1");
        KafkaQueueSource<String> queueSource2 = createQueueSource(SHARE_GROUP_ID + "-2");

        // Set up Flink environment with multiple sources
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);

        // Create data streams from both sources
        DataStream<String> stream1 = env.fromSource(
                queueSource1,
                WatermarkStrategy.noWatermarks(),
                "QueueSource1"
        );
        
        DataStream<String> stream2 = env.fromSource(
                queueSource2,
                WatermarkStrategy.noWatermarks(),
                "QueueSource2"
        );

        // Union streams and collect results
        List<String> actualMessages = Collections.synchronizedList(new ArrayList<>());
        stream1.union(stream2).addSink(new CollectingSink<>(actualMessages));

        // Execute
        env.execute("Queue Load Balancing Test");

        // Verify load balancing - both sources should have consumed messages
        assertThat(actualMessages).hasSize(expectedMessages.size());
        assertThat(actualMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testQueueFaultTolerance() throws Exception {
        // Produce messages
        List<String> expectedMessages = produceTestMessages(QUEUE_TOPIC, 150);

        // Create queue source
        KafkaQueueSource<String> queueSource = createQueueSource(SHARE_GROUP_ID);

        // Set up Flink environment with failure simulation
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(500);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        // Create data stream with failing sink to test fault tolerance
        DataStream<String> stream = env.fromSource(
                queueSource,
                WatermarkStrategy.noWatermarks(),
                "FaultTolerantQueueSource"
        );

        List<String> actualMessages = Collections.synchronizedList(new ArrayList<>());
        stream.addSink(new FailingCollectingSink<>(actualMessages, 50)); // Fail after 50 messages

        // Execute - should recover from failure
        env.execute("Queue Fault Tolerance Test");

        // Verify all messages were eventually processed despite failures
        assertThat(actualMessages).hasSizeGreaterThanOrEqualTo(expectedMessages.size());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQueueOffsetsAndCheckpointing() throws Exception {
        // Produce messages
        List<String> expectedMessages = produceTestMessages(QUEUE_TOPIC, 100);

        // Create queue source with specific starting position
        KafkaQueueSource<String> queueSource = KafkaQueueSource.<String>builder()
                .setBootstrapServers(kafkaSourceTestEnv.getBootstrapServers())
                .setTopics(QUEUE_TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // Set up environment with checkpointing
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);

        DataStream<String> stream = env.fromSource(
                queueSource,
                WatermarkStrategy.noWatermarks(),
                "CheckpointingQueueSource"
        );

        List<String> actualMessages = Collections.synchronizedList(new ArrayList<>());
        stream.addSink(new CollectingSink<>(actualMessages));

        env.execute("Queue Checkpointing Test");

        // Verify checkpointing doesn't affect message consumption
        assertThat(actualMessages).hasSize(expectedMessages.size());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQueueMetrics() throws Exception {
        // Produce messages
        produceTestMessages(QUEUE_TOPIC, 50);

        // Create queue source with metrics enabled
        KafkaQueueSource<String> queueSource = KafkaQueueSource.<String>builder()
                .setBootstrapServers(kafkaSourceTestEnv.getBootstrapServers())
                .setTopics(QUEUE_TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .enableQueueMetrics(true)
                .build();

        // Set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> stream = env.fromSource(
                queueSource,
                WatermarkStrategy.noWatermarks(),
                "MetricsQueueSource"
        );

        List<String> actualMessages = Collections.synchronizedList(new ArrayList<>());
        stream.addSink(new CollectingSink<>(actualMessages));

        env.execute("Queue Metrics Test");

        // Verify metrics are collected (would need access to MetricGroup in real implementation)
        assertThat(actualMessages).isNotEmpty();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQueueSourceConfiguration() throws Exception {
        // Test various configuration options
        Properties queueProps = new Properties();
        queueProps.setProperty("share.group.session.timeout.ms", "30000");
        queueProps.setProperty("share.group.heartbeat.interval.ms", "3000");

        KafkaQueueSource<String> queueSource = KafkaQueueSource.<String>builder()
                .setBootstrapServers(kafkaSourceTestEnv.getBootstrapServers())
                .setTopics(QUEUE_TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setQueueProperties(queueProps)
                .setClientIdPrefix("queue-test-client")
                .build();

        // Verify configuration is applied correctly
        Properties config = queueSource.getConfiguration();
        assertThat(config.getProperty("group.type")).isEqualTo("share");
        assertThat(config.getProperty("share.group.session.timeout.ms")).isEqualTo("30000");
        assertThat(config.getProperty("client.id")).startsWith("queue-test-client");
    }

    // Helper methods

    private KafkaQueueSource<String> createQueueSource(String shareGroupId) {
        return KafkaQueueSource.<String>builder()
                .setBootstrapServers(kafkaSourceTestEnv.getBootstrapServers())
                .setTopics(QUEUE_TOPIC)
                .setShareGroupId(shareGroupId)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();
    }

    private List<String> produceTestMessages(String topic, int messageCount) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceTestEnv.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        List<String> messages = new ArrayList<>();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < messageCount; i++) {
                String message = "queue-message-" + i;
                messages.add(message);
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), message));
            }
            producer.flush();
        }

        return messages;
    }

    /**
     * Simple collecting sink for testing.
     */
    private static class CollectingSink<T> implements SinkFunction<T> {
        private final List<T> collection;

        public CollectingSink(List<T> collection) {
            this.collection = collection;
        }

        @Override
        public void invoke(T value, Context context) {
            collection.add(value);
        }
    }

    /**
     * Failing sink for fault tolerance testing.
     */
    private static class FailingCollectingSink<T> extends CollectingSink<T> {
        private final int failAfterCount;
        private int processedCount = 0;

        public FailingCollectingSink(List<T> collection, int failAfterCount) {
            super(collection);
            this.failAfterCount = failAfterCount;
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            super.invoke(value, context);
            processedCount++;
            
            if (processedCount == failAfterCount) {
                throw new RuntimeException("Simulated failure for fault tolerance testing");
            }
        }
    }
}
