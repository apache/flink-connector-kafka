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

import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Brings up multiple kafka clusters and provides utilities to setup test data. */
public class DynamicKafkaSourceTestHelper extends KafkaTestBase {
    public static final int NUM_KAFKA_CLUSTERS = 2;

    public static void setup() throws Throwable {
        setNumKafkaClusters(NUM_KAFKA_CLUSTERS);
        prepare();
    }

    public static void tearDown() throws Exception {
        shutDownServices();
    }

    public static KafkaClusterTestEnvMetadata getKafkaClusterTestEnvMetadata(int kafkaClusterIdx) {
        return kafkaClusters.get(kafkaClusterIdx);
    }

    public static MetadataUpdateEvent getMetadataUpdateEvent(String topic) {
        return new MetadataUpdateEvent(Collections.singleton(getKafkaStream(topic)));
    }

    public static String getKafkaClusterId(int kafkaClusterIdx) {
        return kafkaClusters.get(kafkaClusterIdx).getKafkaClusterId();
    }

    /** Stream is a topic across multiple clusters. */
    public static KafkaStream getKafkaStream(String topic) {
        Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();
        for (int i = 0; i < NUM_KAFKA_CLUSTERS; i++) {
            KafkaClusterTestEnvMetadata kafkaClusterTestEnvMetadata =
                    getKafkaClusterTestEnvMetadata(i);

            Set<String> topics = new HashSet<>();
            topics.add(topic);

            ClusterMetadata clusterMetadata =
                    new ClusterMetadata(
                            topics, kafkaClusterTestEnvMetadata.getStandardProperties());
            clusterMetadataMap.put(
                    kafkaClusterTestEnvMetadata.getKafkaClusterId(), clusterMetadata);
        }

        return new KafkaStream(topic, clusterMetadataMap);
    }

    public static void createTopic(String topic, int numPartitions, int replicationFactor) {
        for (int i = 0; i < NUM_KAFKA_CLUSTERS; i++) {
            createTopic(i, topic, numPartitions, replicationFactor);
        }
    }

    public static void createTopic(String topic, int numPartitions) {
        createTopic(topic, numPartitions, 1);
    }

    public static void createTopic(int kafkaClusterIdx, String topic, int numPartitions) {
        createTopic(kafkaClusterIdx, topic, numPartitions, 1);
    }

    private static void createTopic(
            int kafkaClusterIdx, String topic, int numPartitions, int replicationFactor) {
        kafkaClusters
                .get(kafkaClusterIdx)
                .getKafkaTestEnvironment()
                .createTestTopic(topic, numPartitions, replicationFactor);
    }

    /** Produces [0, numPartitions*numRecordsPerSplit) range of records to the specified topic. */
    public static List<ProducerRecord<String, Integer>> produceToKafka(
            String topic, int numPartitions, int numRecordsPerSplit) throws Throwable {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();

        int counter = 0;
        for (int kafkaClusterIdx = 0; kafkaClusterIdx < NUM_KAFKA_CLUSTERS; kafkaClusterIdx++) {
            String kafkaClusterId = getKafkaClusterId(kafkaClusterIdx);
            List<ProducerRecord<String, Integer>> recordsForCluster = new ArrayList<>();
            for (int part = 0; part < numPartitions; part++) {
                for (int i = 0; i < numRecordsPerSplit; i++) {
                    recordsForCluster.add(
                            new ProducerRecord<>(
                                    topic,
                                    part,
                                    topic + "-" + part,
                                    counter++,
                                    Collections.singleton(
                                            new RecordHeader(
                                                    "flink.kafka-cluster-name",
                                                    kafkaClusterId.getBytes(
                                                            StandardCharsets.UTF_8)))));
                }
            }

            produceToKafka(kafkaClusterIdx, recordsForCluster);
            records.addAll(recordsForCluster);
        }

        return records;
    }

    /**
     * Produces [recordValueStartingOffset, recordValueStartingOffset +
     * numPartitions*numRecordsPerSplit) range of records to the specified topic and cluster.
     */
    public static int produceToKafka(
            int kafkaClusterIdx,
            String topic,
            int numPartitions,
            int numRecordsPerSplit,
            int recordValueStartingOffset)
            throws Throwable {
        int counter = recordValueStartingOffset;
        String kafkaClusterId = getKafkaClusterId(kafkaClusterIdx);
        List<ProducerRecord<String, Integer>> recordsForCluster = new ArrayList<>();
        for (int part = 0; part < numPartitions; part++) {
            for (int i = 0; i < numRecordsPerSplit; i++) {
                recordsForCluster.add(
                        new ProducerRecord<>(
                                topic,
                                part,
                                topic + "-" + part,
                                counter++,
                                Collections.singleton(
                                        new RecordHeader(
                                                "flink.kafka-cluster-name",
                                                kafkaClusterId.getBytes(StandardCharsets.UTF_8)))));
            }
        }

        produceToKafka(kafkaClusterIdx, recordsForCluster);

        return counter;
    }

    public static void produceToKafka(
            int kafkaClusterIdx, Collection<ProducerRecord<String, Integer>> records)
            throws Throwable {
        produceToKafka(kafkaClusterIdx, records, StringSerializer.class, IntegerSerializer.class);
    }

    public static <K, V> void produceToKafka(
            int id,
            Collection<ProducerRecord<K, V>> records,
            Class<? extends org.apache.kafka.common.serialization.Serializer<K>> keySerializerClass,
            Class<? extends org.apache.kafka.common.serialization.Serializer<V>>
                    valueSerializerClass)
            throws Throwable {
        produceToKafka(
                kafkaClusters.get(id).getStandardProperties(),
                records,
                keySerializerClass,
                valueSerializerClass);
    }

    public static <K, V> void produceToKafka(
            Properties clusterProperties,
            Collection<ProducerRecord<K, V>> records,
            Class<? extends org.apache.kafka.common.serialization.Serializer<K>> keySerializerClass,
            Class<? extends org.apache.kafka.common.serialization.Serializer<V>>
                    valueSerializerClass)
            throws Throwable {
        Properties props = new Properties();
        props.putAll(clusterProperties);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        props.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());

        AtomicReference<Throwable> sendingError = new AtomicReference<>();
        Callback callback =
                (metadata, exception) -> {
                    if (exception != null) {
                        if (!sendingError.compareAndSet(null, exception)) {
                            sendingError.get().addSuppressed(exception);
                        }
                    }
                };
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(props)) {
            for (ProducerRecord<K, V> record : records) {
                producer.send(record, callback).get();
            }
        }
        if (sendingError.get() != null) {
            throw sendingError.get();
        }
    }
}
