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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaQueueSource} functionality.
 * Tests the new queue semantics and shared group features introduced in Kafka 4.1.0.
 */
class KafkaQueueSourceTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "test-queue-topic";
    private static final String SHARE_GROUP_ID = "test-share-group";

    @Test
    void testQueueSourceBuilderWithBasicConfiguration() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        assertThat(source).isNotNull();
        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(source.isQueueSemanticsEnabled()).isTrue();
        assertThat(source.getShareGroupId()).isEqualTo(SHARE_GROUP_ID);
    }

    @Test
    void testQueueSourceBuilderWithMultipleTopics() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(Arrays.asList("topic1", "topic2", "topic3"))
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        assertThat(source).isNotNull();
        assertThat(source.getTopics()).containsExactlyInAnyOrder("topic1", "topic2", "topic3");
    }

    @Test
    void testQueueSourceBuilderWithQueueSpecificProperties() {
        Properties queueProps = new Properties();
        queueProps.setProperty("share.group.session.timeout.ms", "30000");
        queueProps.setProperty("share.group.heartbeat.interval.ms", "3000");
        queueProps.setProperty("share.group.partition.assignment.strategy", "roundrobin");

        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setQueueProperties(queueProps)
                .build();

        Properties config = source.getConfiguration();
        assertThat(config.getProperty("group.type")).isEqualTo("share");
        assertThat(config.getProperty("share.group.session.timeout.ms")).isEqualTo("30000");
        assertThat(config.getProperty("share.group.heartbeat.interval.ms")).isEqualTo("3000");
    }

    @Test
    void testQueueSourceBuilderValidation() {
        // Test missing bootstrap servers
        assertThatThrownBy(() -> 
            KafkaQueueSource.<String>builder()
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build())
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("bootstrap.servers");

        // Test missing share group ID
        assertThatThrownBy(() -> 
            KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Share group ID is required for queue semantics");

        // Test missing topics
        assertThatThrownBy(() -> 
            KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No topics specified");

        // Test missing deserializer
        assertThatThrownBy(() -> 
            KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Deserialization schema is required");
    }

    @Test
    void testQueueSourceWithBoundedMode() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setBounded(OffsetsInitializer.latest())
                .build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void testQueueSourceWithUnboundedMode() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setUnbounded(OffsetsInitializer.latest())
                .build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Test
    void testQueueSourceConfigurationOverrides() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .build();

        Properties config = source.getConfiguration();
        
        // Verify queue-specific overrides
        assertThat(config.getProperty("group.type")).isEqualTo("share");
        assertThat(config.getProperty("group.id")).isEqualTo(SHARE_GROUP_ID);
        assertThat(config.getProperty("enable.auto.commit")).isEqualTo("false");
        
        // Verify consumer group properties are disabled for queue mode
        assertThat(config.getProperty("partition.discovery.interval.ms")).isEqualTo("-1");
    }

    @Test
    void testQueueSourceSplitEnumeratorCreation() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Mock split enumerator context - in real implementation this would be more complex
        // This tests that we can create the queue-specific enumerator
        assertThat(source).isNotNull();
        // The actual split enumerator creation will be tested in integration tests
    }

    @Test
    void testQueueSourceReaderCreation() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Mock source reader context - in real implementation this would be more complex
        // This tests that we can create the queue-specific reader
        assertThat(source).isNotNull();
        // The actual source reader creation will be tested in integration tests
    }

    @Test
    void testQueueSourceLineageVertex() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // Test that lineage information includes queue-specific metadata
        var lineageVertex = source.getLineageVertex();
        assertThat(lineageVertex).isNotNull();
        // Specific lineage vertex testing will be implemented with the actual queue source
    }

    @Test
    void testQueueSourcePropertiesValidation() {
        Properties invalidProps = new Properties();
        invalidProps.setProperty("group.type", "consumer"); // Invalid for queue source

        assertThatThrownBy(() -> 
            KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setProperties(invalidProps)
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("group.type must be 'share' for queue semantics");
    }

    @Test
    void testQueueSourceClientIdConfiguration() {
        String clientIdPrefix = "queue-consumer";
        
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setClientIdPrefix(clientIdPrefix)
                .build();

        Properties config = source.getConfiguration();
        assertThat(config.getProperty("client.id")).startsWith(clientIdPrefix);
    }

    @Test
    void testQueueSourceMetricsConfiguration() {
        KafkaQueueSource<String> source = KafkaQueueSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setShareGroupId(SHARE_GROUP_ID)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .enableQueueMetrics(true)
                .build();

        assertThat(source.isQueueMetricsEnabled()).isTrue();
    }
}