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
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.util.KafkaVersionUtils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test demonstrating the configuration and setup of both traditional and share group Kafka sources.
 * This test validates the builder patterns and configuration without requiring a running Kafka cluster.
 */
class KafkaShareGroupSourceConfigurationTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSourceConfigurationTest.class);

    @Test
    void testTraditionalKafkaSourceConfiguration() {
        // Test that traditional KafkaSource still works with Kafka 4.1.0
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test-topic")
                .setGroupId("test-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        assertThat(kafkaSource).isNotNull();
        assertThat(kafkaSource.getBoundedness()).isNotNull();
        
        LOG.info("✅ Traditional KafkaSource configuration successful");
    }

    @Test
    void testShareGroupSourceConfiguration() {
        // Only run this test if share groups are supported
        assumeTrue(KafkaVersionUtils.isShareGroupSupported(), 
                   "Share groups not supported in current Kafka version: " + KafkaVersionUtils.getKafkaVersion());

        KafkaShareGroupSource<String> shareGroupSource = KafkaShareGroupSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test-topic")
                .setShareGroupId("test-share-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .enableShareGroupMetrics(true)
                .build();

        assertThat(shareGroupSource).isNotNull();
        assertThat(shareGroupSource.getBoundedness()).isNotNull();
        assertThat(shareGroupSource.getShareGroupId()).isEqualTo("test-share-group");
        assertThat(shareGroupSource.isShareGroupEnabled()).isTrue();
        assertThat(shareGroupSource.isShareGroupMetricsEnabled()).isTrue();

        LOG.info("✅ KafkaShareGroupSource configuration successful");
    }

    @Test
    void testVersionCompatibility() {
        String kafkaVersion = KafkaVersionUtils.getKafkaVersion();
        boolean shareGroupSupported = KafkaVersionUtils.isShareGroupSupported();

        LOG.info("Kafka Version: {}", kafkaVersion);
        LOG.info("Share Group Support: {}", shareGroupSupported);

        // Version should be detected
        assertThat(kafkaVersion).isNotNull();
        assertThat(kafkaVersion).isNotEqualTo("unknown");

        // Share groups should be supported with Kafka 4.1.0
        if (kafkaVersion.startsWith("4.1")) {
            assertThat(shareGroupSupported).isTrue();
            LOG.info("✅ Share group support correctly detected for Kafka 4.1.x");
        }
    }

    @Test
    void testShareGroupPropertiesValidation() {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported(), 
                   "Share groups not supported in current Kafka version");

        // Test that share group properties are automatically configured
        KafkaShareGroupSource<String> source = KafkaShareGroupSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test-topic")
                .setShareGroupId("test-share-group")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Verify internal configuration
        assertThat(source.getConfiguration().getProperty("group.type")).isEqualTo("share");
        assertThat(source.getConfiguration().getProperty("group.id")).isEqualTo("test-share-group");
        assertThat(source.getConfiguration().getProperty("enable.auto.commit")).isEqualTo("false");

        LOG.info("✅ Share group properties automatically configured correctly");
    }

    @Test 
    void testBackwardCompatibility() {
        // Ensure both sources can coexist and be configured independently
        
        // Traditional source
        KafkaSource<String> traditional = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("traditional-topic")
                .setGroupId("traditional-group")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Share group source (if supported)
        if (KafkaVersionUtils.isShareGroupSupported()) {
            KafkaShareGroupSource<String> shareGroup = KafkaShareGroupSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("sharegroup-topic")
                    .setShareGroupId("sharegroup-id")
                    .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                    .build();

            assertThat(shareGroup.getShareGroupId()).isEqualTo("sharegroup-id");
            LOG.info("✅ Both traditional and share group sources configured successfully");
        } else {
            LOG.info("✅ Traditional source works without share group support");
        }

        assertThat(traditional).isNotNull();
    }
}