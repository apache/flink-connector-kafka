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

package org.apache.flink.connector.kafka.source.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaShareGroupSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.util.KafkaVersionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example demonstrating both traditional Kafka source and new share group source usage.
 * This example shows backward compatibility and feature selection based on Kafka version.
 */
public class KafkaSourceExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Common configuration
        String bootstrapServers = "localhost:9092";
        String topic = "example-topic";
        String consumerGroup = "example-group";
        String shareGroupId = "example-share-group";

        // Check Kafka version and capabilities
        System.out.println("Kafka Version: " + KafkaVersionUtils.getKafkaVersion());
        System.out.println("Share Group Support: " + KafkaVersionUtils.isShareGroupSupported());

        // Example 1: Traditional Kafka Source (works with all Kafka versions)
        createTraditionalKafkaSource(env, bootstrapServers, topic, consumerGroup);

        // Example 2: Share Group Source (requires Kafka 4.1.0+)
        if (KafkaVersionUtils.isShareGroupSupported()) {
            createShareGroupKafkaSource(env, bootstrapServers, topic, shareGroupId);
        } else {
            System.out.println("Share group not supported, skipping share group example");
        }

        env.execute("Kafka Source Examples");
    }

    /**
     * Traditional partition-based Kafka source example.
     * This approach works with all Kafka versions.
     */
    private static void createTraditionalKafkaSource(
            StreamExecutionEnvironment env,
            String bootstrapServers,
            String topic,
            String consumerGroup) {

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        DataStream<String> traditionalStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Traditional Kafka Source");

        traditionalStream
                .map(value -> "Traditional: " + value)
                .print();
    }

    /**
     * Share group-based Kafka source example.
     * This approach requires Kafka 4.1.0+ with share group support.
     */
    private static void createShareGroupKafkaSource(
            StreamExecutionEnvironment env,
            String bootstrapServers,
            String topic,
            String shareGroupId) {

        KafkaShareGroupSource<String> shareGroupSource = KafkaShareGroupSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setShareGroupId(shareGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .enableShareGroupMetrics(true)
                .build();

        DataStream<String> shareGroupStream = env.fromSource(
                shareGroupSource,
                WatermarkStrategy.noWatermarks(),
                "Share Group Kafka Source");

        shareGroupStream
                .map(value -> "ShareGroup: " + value)
                .print();
    }
}