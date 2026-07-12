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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base class for KafkaSource unit tests. */
public class KafkaSourceTestEnv extends KafkaTestBase {
    public static final String GROUP_ID = "KafkaSourceTestEnv";
    public static final int NUM_PARTITIONS = 10;
    public static final int NUM_RECORDS_PER_PARTITION = 10;

    private static final Duration WAIT_DATA_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration WAIT_DATA_INTERVAL = Duration.ofSeconds(2);

    private static AdminClient adminClient;
    private static KafkaConsumer<String, Integer> consumer;

    public static void setup() throws Throwable {
        prepare();
        adminClient = getAdminClient();
        consumer = getConsumer();
    }

    public static void tearDown() throws Exception {
        consumer.close();
        adminClient.close();
        shutDownServices();
    }

    // --------------------- public client related helpers ------------------

    public static AdminClient getAdminClient() {
        Properties props = new Properties();
        props.putAll(standardProps);
        return AdminClient.create(props);
    }

    public static KafkaConsumer<String, Integer> getConsumer() {
        return new KafkaConsumer<>(getConsumerProperties(IntegerDeserializer.class));
    }

    public static Properties getConsumerProperties(Class<?> deserializerClass) {
        Properties props = new Properties();
        props.putAll(standardProps);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass.getName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass.getName());
        return props;
    }

    // ------------------- topic information helpers -------------------

    public static Map<Integer, Map<String, KafkaPartitionSplit>> getSplitsByOwners(
            final Collection<String> topics, final int numSubtasks) {
        final Map<Integer, Map<String, KafkaPartitionSplit>> splitsByOwners = new HashMap<>();
        for (String topic : topics) {
            getPartitionsForTopic(topic)
                    .forEach(
                            tp -> {
                                int ownerReader = Math.abs(tp.hashCode()) % numSubtasks;
                                KafkaPartitionSplit split =
                                        new KafkaPartitionSplit(
                                                tp,
                                                getEarliestOffset(tp),
                                                (long) NUM_RECORDS_PER_PARTITION);
                                splitsByOwners
                                        .computeIfAbsent(ownerReader, r -> new HashMap<>())
                                        .put(KafkaPartitionSplit.toSplitId(tp), split);
                            });
        }
        return splitsByOwners;
    }

    /**
     * For a given partition {@code TOPIC-PARTITION} the {@code i}-th records looks like following.
     *
     * <pre>{@code
     * topic: TOPIC
     * partition: PARTITION
     * timestamp: 1000 * PARTITION
     * key: TOPIC-PARTITION
     * value: i
     * }</pre>
     */
    public static List<ProducerRecord<String, Integer>> getRecordsForPartition(TopicPartition tp) {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            records.add(
                    new ProducerRecord<>(tp.topic(), tp.partition(), i * 1000L, tp.toString(), i));
        }
        return records;
    }

    /**
     * For a given partition {@code TOPIC-PARTITION} the {@code i}-th records looks like following.
     *
     * <pre>{@code
     * topic: TOPIC
     * partition: PARTITION
     * timestamp: null
     * key: TOPIC-PARTITION
     * value: i
     * }</pre>
     */
    public static List<ProducerRecord<String, Integer>> getRecordsForPartitionWithoutTimestamp(
            TopicPartition tp) {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            records.add(new ProducerRecord<>(tp.topic(), tp.partition(), null, tp.toString(), i));
        }
        return records;
    }

    public static List<ProducerRecord<String, Integer>> getRecordsForTopic(String topic) {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (TopicPartition tp : getPartitionsForTopic(topic)) {
            records.addAll(getRecordsForPartition(tp));
        }
        return records;
    }

    public static List<ProducerRecord<String, Integer>> getRecordsForTopicWithoutTimestamp(
            String topic) {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (TopicPartition tp : getPartitionsForTopic(topic)) {
            records.addAll(getRecordsForPartitionWithoutTimestamp(tp));
        }
        return records;
    }

    public static List<TopicPartition> getPartitionsForTopics(Collection<String> topics) {
        List<TopicPartition> partitions = new ArrayList<>();
        topics.forEach(t -> partitions.addAll(getPartitionsForTopic(t)));
        return partitions;
    }

    public static List<TopicPartition> getPartitionsForTopic(String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toList());
    }

    public static Map<TopicPartition, Long> getEarliestOffsets(List<TopicPartition> partitions) {
        Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            earliestOffsets.put(tp, getEarliestOffset(tp));
        }
        return earliestOffsets;
    }

    public static Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(
            List<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            committedOffsets.put(tp, new OffsetAndMetadata(tp.partition() + 2));
        }
        return committedOffsets;
    }

    public static long getEarliestOffset(TopicPartition tp) {
        return tp.partition();
    }

    // --------------- topic manipulation helpers ---------------

    public static void createTestTopic(String topic) {
        createTestTopic(topic, NUM_PARTITIONS, 1);
    }

    public static void setupEarliestOffsets(String topic) throws Throwable {
        // Delete some records to move the starting partition.
        List<TopicPartition> partitions = getPartitionsForTopic(topic);
        setupEarliestOffsets(partitions);
    }

    public static void setupEarliestOffsets(List<TopicPartition> partitions) throws Throwable {
        Map<TopicPartition, RecordsToDelete> toDelete = new HashMap<>();
        getEarliestOffsets(partitions)
                .forEach((tp, offset) -> toDelete.put(tp, RecordsToDelete.beforeOffset(offset)));
        adminClient.deleteRecords(toDelete).all().get();
    }

    public static void setupCommittedOffsets(String topic)
            throws ExecutionException, InterruptedException {
        List<TopicPartition> partitions = getPartitionsForTopic(topic);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommittedOffsets(partitions);
        consumer.commitSync(committedOffsets);
        Map<TopicPartition, OffsetAndMetadata> toVerify =
                adminClient
                        .listConsumerGroupOffsets(
                                Collections.singletonMap(
                                        GROUP_ID,
                                        new ListConsumerGroupOffsetsSpec()
                                                .topicPartitions(committedOffsets.keySet())))
                        .partitionsToOffsetAndMetadata()
                        .get();
        assertThat(toVerify).as("The offsets are not committed").isEqualTo(committedOffsets);
    }

    public static void produceToKafka(Collection<ProducerRecord<String, Integer>> records)
            throws Throwable {
        produceToKafka(records, StringSerializer.class, IntegerSerializer.class, null);
    }

    public static void produceToKafka(
            Collection<ProducerRecord<String, Integer>> records, Properties extraProps)
            throws Throwable {
        produceToKafka(records, StringSerializer.class, IntegerSerializer.class, extraProps);
    }

    public static void setupTopic(
            String topic,
            boolean setupEarliestOffsets,
            boolean setupCommittedOffsets,
            Function<String, List<ProducerRecord<String, Integer>>> testDataGenerator)
            throws Throwable {
        createTestTopic(topic);
        produceToKafka(testDataGenerator.apply(topic));
        if (setupEarliestOffsets) {
            setupEarliestOffsets(topic);
        }
        if (setupCommittedOffsets) {
            setupCommittedOffsets(topic);
        }
    }

    public static String getTopicId(String topicName)
            throws InterruptedException, ExecutionException {
        return getAdminClient()
                .listTopics()
                .namesToListings()
                .get()
                .get(topicName)
                .topicId()
                .toString();
    }

    // Helper method to count records in a topic
    private static long getRecordCountInTopic(String topicName) {
        try (Consumer<String, Integer> consumer = getConsumer()) {
            List<TopicPartition> partitions =
                    consumer.partitionsFor(topicName).stream()
                            .map(info -> new TopicPartition(topicName, info.partition()))
                            .collect(Collectors.toList());

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            long totalRecords = 0;
            ConsumerRecords<String, Integer> records;
            do {
                records = consumer.poll(Duration.ofSeconds(1));
                totalRecords += records.count();
            } while (!records.isEmpty());

            return totalRecords;
        }
    }

    // Helper method to wait for specific number of records in a topic
    public static void waitForRecordsInTopic(String topicName, int expectedCount) throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    long count = KafkaSourceTestEnv.getRecordCountInTopic(topicName);
                    LOG.info(
                            "Found {} records in topic {} (expected: {})",
                            count,
                            topicName,
                            expectedCount);
                    if (count == expectedCount) {
                        return true;
                    }
                    return false;
                },
                WAIT_DATA_TIMEOUT,
                WAIT_DATA_INTERVAL,
                String.format(
                        "Timeout waiting for %d records in topic %s after %s",
                        expectedCount, topicName, WAIT_DATA_TIMEOUT));
    }
}
