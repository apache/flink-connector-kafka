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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.connector.kafka.testutils.DockerImageVersions.APACHE_KAFKA;
import static org.apache.flink.connector.kafka.testutils.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test to verify Flink Kafka connector works with both Confluent Platform and Apache
 * Kafka images.
 *
 * <p>This test validates the actual integration between Flink and Kafka by running real Flink jobs
 * that read from and write to Kafka.
 */
class FlinkKafkaIntegrationCompatibilityTest {

    private TestKafkaContainer kafkaContainer;
    private AdminClient adminClient;

    @AfterEach
    void tearDown() {
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    /**
     * Tests Flink KafkaSource integration by reading records from Kafka and verifying the sum of
     * values.
     *
     * <p>This is adapted from {@code KafkaSourceITCase.testValueOnlyDeserializer}.
     */
    @ParameterizedTest
    @ValueSource(strings = {KAFKA, APACHE_KAFKA})
    void testFlinkKafkaSourceIntegration(String dockerImage) throws Exception {
        // Start Kafka container
        kafkaContainer = new TestKafkaContainer(dockerImage);
        kafkaContainer.start();

        String topic1 = "test-source-topic1-" + UUID.randomUUID();
        String topic2 = "test-source-topic2-" + UUID.randomUUID();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        int numPartitions = 4;
        int numRecordsPerPartition = 5;

        // Create topics
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminConfig);
        adminClient
                .createTopics(
                        Arrays.asList(
                                new NewTopic(topic1, numPartitions, (short) 1),
                                new NewTopic(topic2, numPartitions, (short) 1)))
                .all()
                .get();

        // Produce test data to both topics
        // Values in partition N should be {N, N+1, N+2, ..., numRecordsPerPartition-1}
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        try (KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(producerConfig)) {
            for (String topic : Arrays.asList(topic1, topic2)) {
                for (int partition = 0; partition < numPartitions; partition++) {
                    for (int value = partition; value < numRecordsPerPartition; value++) {
                        producer.send(new ProducerRecord<>(topic, partition, partition, value))
                                .get();
                    }
                }
            }
        }

        // Create Flink KafkaSource
        KafkaSource<Integer> source =
                KafkaSource.<Integer>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setGroupId("testFlinkKafkaSource-" + UUID.randomUUID())
                        .setTopics(Arrays.asList(topic1, topic2))
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        IntegerDeserializer.class))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setBounded(OffsetsInitializer.latest())
                        .build();

        // Execute Flink job and collect results
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        int actualSum = 0;
        try (CloseableIterator<Integer> resultIterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testFlinkKafkaSource")
                        .executeAndCollect()) {
            while (resultIterator.hasNext()) {
                actualSum += resultIterator.next();
            }
        }

        // Calculate expected sum
        // Each partition N has values: N, N+1, ..., numRecordsPerPartition-1
        int expectedSum = 0;
        for (int partition = 0; partition < numPartitions; partition++) {
            for (int value = partition; value < numRecordsPerPartition; value++) {
                expectedSum += value;
            }
        }
        // Two topics, so double the sum
        expectedSum *= 2;

        assertThat(actualSum)
                .as(
                        "Flink should read and sum all values correctly from Kafka with image: %s",
                        dockerImage)
                .isEqualTo(expectedSum);
    }

    /**
     * Tests Flink KafkaSink integration by writing records to Kafka and verifying they are
     * persisted.
     *
     * <p>This is adapted from {@code KafkaSinkITCase.testWriteRecordsToKafkaWithNoneGuarantee}.
     */
    @ParameterizedTest
    @ValueSource(strings = {KAFKA, APACHE_KAFKA})
    void testFlinkKafkaSinkIntegration(String dockerImage) throws Exception {
        // Start Kafka container
        kafkaContainer = new TestKafkaContainer(dockerImage);
        kafkaContainer.start();

        String topic = "test-sink-topic-" + UUID.randomUUID();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        int numRecords = 100;

        // Create topic
        Map<String, Object> adminConfig = new HashMap<>();
        adminConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminConfig);
        adminClient
                .createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)))
                .all()
                .get();

        // Create Flink KafkaSink and write records
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source = env.fromSequence(0, numRecords - 1).map(i -> "record-" + i);

        source.sinkTo(
                KafkaSink.<String>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build());

        env.execute("testFlinkKafkaSink");

        // Verify records were written to Kafka
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<String> receivedRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(Collections.singleton(topic));

            long startTime = System.currentTimeMillis();
            while (receivedRecords.size() < numRecords
                    && System.currentTimeMillis() - startTime < 30000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    receivedRecords.add(record.value());
                }
            }
        }

        assertThat(receivedRecords)
                .as("Flink should write all records to Kafka with image: %s", dockerImage)
                .hasSize(numRecords)
                .allMatch(record -> record.startsWith("record-"));
    }
}
