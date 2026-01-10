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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Comprehensive test suite for {@link KafkaShareGroupSourceBuilder}.
 *
 * <p>This test validates builder functionality, error handling, and property management for Kafka
 * share group source construction.
 */
@DisplayName("KafkaShareGroupSourceBuilder Tests")
class KafkaShareGroupSourceBuilderTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaShareGroupSourceBuilderTest.class);

    private static final String TEST_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_SHARE_GROUP_ID = "test-share-group";

    private KafkaRecordDeserializationSchema<String> testDeserializer;

    @BeforeEach
    void setUp() {
        testDeserializer = KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema());
    }

    @Nested
    @DisplayName("Builder Validation Tests")
    class BuilderValidationTests {

        @Test
        @DisplayName("Should reject null bootstrap servers")
        void testNullBootstrapServers() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            assertThatThrownBy(
                            () -> KafkaShareGroupSource.<String>builder().setBootstrapServers(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Bootstrap servers cannot be null");
        }

        @Test
        @DisplayName("Should reject empty bootstrap servers")
        void testEmptyBootstrapServers() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            assertThatThrownBy(
                            () ->
                                    KafkaShareGroupSource.<String>builder()
                                            .setBootstrapServers("   "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Bootstrap servers cannot be empty");
        }

        @Test
        @DisplayName("Should reject null share group ID")
        void testNullShareGroupId() {
            assertThatThrownBy(() -> KafkaShareGroupSource.<String>builder().setShareGroupId(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Share group ID cannot be null");
        }

        @Test
        @DisplayName("Should reject empty share group ID")
        void testEmptyShareGroupId() {
            assertThatThrownBy(() -> KafkaShareGroupSource.<String>builder().setShareGroupId("   "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Share group ID cannot be empty");
        }

        @Test
        @DisplayName("Should reject null topic arrays")
        void testNullTopics() {
            assertThatThrownBy(
                            () ->
                                    KafkaShareGroupSource.<String>builder()
                                            .setTopics((String[]) null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Topics cannot be null");
        }

        @Test
        @DisplayName("Should reject empty topic arrays")
        void testEmptyTopics() {
            assertThatThrownBy(() -> KafkaShareGroupSource.<String>builder().setTopics())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("At least one topic must be specified");
        }

        @Test
        @DisplayName("Should reject topics with null elements")
        void testTopicsWithNullElements() {
            assertThatThrownBy(
                            () ->
                                    KafkaShareGroupSource.<String>builder()
                                            .setTopics("valid-topic", null, "another-topic"))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Topic name cannot be null");
        }

        @Test
        @DisplayName("Should reject topics with empty elements")
        void testTopicsWithEmptyElements() {
            assertThatThrownBy(
                            () ->
                                    KafkaShareGroupSource.<String>builder()
                                            .setTopics("valid-topic", "   ", "another-topic"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Topic name cannot be empty");
        }
    }

    @Nested
    @DisplayName("Property Management Tests")
    class PropertyManagementTests {

        @Test
        @DisplayName("Should handle null properties gracefully")
        void testNullProperties() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            // Should not throw exception
            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .setProperties(null)
                            .build();

            assertThat(source).isNotNull();
        }

        @Test
        @DisplayName("Should validate incompatible group.type property")
        void testInvalidGroupTypeProperty() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            Properties invalidProps = new Properties();
            invalidProps.setProperty("group.type", "consumer");

            assertThatThrownBy(
                            () ->
                                    KafkaShareGroupSource.<String>builder()
                                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                                            .setTopics(TEST_TOPIC)
                                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                                            .setDeserializer(testDeserializer)
                                            .setProperties(invalidProps))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("group.type must be 'share'");
        }

        @Test
        @DisplayName("Should accept compatible group.type property")
        void testValidGroupTypeProperty() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            Properties validProps = new Properties();
            validProps.setProperty("group.type", "share");
            validProps.setProperty("session.timeout.ms", "30000");

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .setProperties(validProps)
                            .build();

            assertThat(source).isNotNull();
            Properties config = source.getConfiguration();
            assertThat(config.getProperty("group.type")).isEqualTo("share");
            assertThat(config.getProperty("session.timeout.ms")).isEqualTo("30000");
        }

        @Test
        @DisplayName("Should override conflicting properties with warning")
        void testPropertyOverrides() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            Properties userProps = new Properties();
            userProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "user-group");
            userProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            userProps.setProperty("custom.property", "custom.value");

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .setProperties(userProps)
                            .build();

            Properties config = source.getConfiguration();

            // Verify overrides
            assertThat(config.getProperty("group.type")).isEqualTo("share");
            assertThat(config.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                    .isEqualTo(TEST_SHARE_GROUP_ID);
            assertThat(config.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                    .isEqualTo("false");

            // Verify custom properties are preserved
            assertThat(config.getProperty("custom.property")).isEqualTo("custom.value");
        }
    }

    @Nested
    @DisplayName("Configuration Tests")
    class ConfigurationTests {

        @Test
        @DisplayName("Should configure default properties correctly")
        void testDefaultConfiguration() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .build();

            Properties config = source.getConfiguration();

            // Verify required share group properties
            assertThat(config.getProperty("group.type")).isEqualTo("share");
            assertThat(config.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                    .isEqualTo(TEST_SHARE_GROUP_ID);
            assertThat(config.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                    .isEqualTo("false");
            assertThat(config.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
                    .isEqualTo(TEST_BOOTSTRAP_SERVERS);

            // Verify deserializers are set
            assertThat(config.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
                    .isEqualTo("org.apache.kafka.common.serialization.ByteArrayDeserializer");
            assertThat(config.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
                    .isEqualTo("org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }

        @Test
        @DisplayName("Should configure metrics when enabled")
        void testMetricsConfiguration() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .enableShareGroupMetrics(true)
                            .build();

            assertThat(source.isShareGroupMetricsEnabled()).isTrue();
        }

        @Test
        @DisplayName("Should handle multiple topics configuration")
        void testMultipleTopicsConfiguration() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            String[] topics = {"topic1", "topic2", "topic3"};

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(topics)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .build();

            assertThat(source.getTopics()).containsExactlyInAnyOrder(topics);
        }

        @Test
        @DisplayName("Should configure starting offsets correctly")
        void testStartingOffsetsConfiguration() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .setStartingOffsets(OffsetsInitializer.latest())
                            .build();

            assertThat(source.getStartingOffsetsInitializer()).isNotNull();

            Properties config = source.getConfiguration();
            assertThat(config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
                    .isEqualTo("latest");
        }
    }

    @Nested
    @DisplayName("Builder Pattern Tests")
    class BuilderPatternTests {

        @Test
        @DisplayName("Should support method chaining")
        void testMethodChaining() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            // All methods should return the builder instance for chaining
            KafkaShareGroupSourceBuilder<String> builder =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            .enableShareGroupMetrics(true)
                            .setProperty("max.poll.records", "500");

            assertThat(builder).isNotNull();

            KafkaShareGroupSource<String> source = builder.build();
            assertThat(source).isNotNull();
        }

        @Test
        @DisplayName("Should handle builder reuse correctly")
        void testBuilderReuse() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            KafkaShareGroupSourceBuilder<String> builder =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setDeserializer(testDeserializer);

            // First source
            KafkaShareGroupSource<String> source1 =
                    builder.setTopics("topic1").setShareGroupId("group1").build();

            // Second source (builder should be reusable)
            KafkaShareGroupSource<String> source2 =
                    builder.setTopics("topic2").setShareGroupId("group2").build();

            assertThat(source1.getShareGroupId()).isEqualTo("group1");
            assertThat(source2.getShareGroupId()).isEqualTo("group2");
        }

        @Test
        @DisplayName("Should maintain builder state independence")
        void testBuilderStateIndependence() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            KafkaShareGroupSourceBuilder<String> builder1 = KafkaShareGroupSource.<String>builder();
            KafkaShareGroupSourceBuilder<String> builder2 = KafkaShareGroupSource.<String>builder();

            // Configure builders differently
            builder1.setShareGroupId("group1").enableShareGroupMetrics(true);
            builder2.setShareGroupId("group2").enableShareGroupMetrics(false);

            // Complete configurations and build
            KafkaShareGroupSource<String> source1 =
                    builder1.setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setDeserializer(testDeserializer)
                            .build();

            KafkaShareGroupSource<String> source2 =
                    builder2.setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setDeserializer(testDeserializer)
                            .build();

            // Verify independence
            assertThat(source1.getShareGroupId()).isEqualTo("group1");
            assertThat(source1.isShareGroupMetricsEnabled()).isTrue();

            assertThat(source2.getShareGroupId()).isEqualTo("group2");
            assertThat(source2.isShareGroupMetricsEnabled()).isFalse();
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle complex topic names")
        void testComplexTopicNames() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            String[] complexTopics = {
                "topic_with_underscores",
                "topic-with-dashes",
                "topic.with.dots",
                "topic123with456numbers"
            };

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(complexTopics)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .build();

            assertThat(source.getTopics()).containsExactlyInAnyOrder(complexTopics);
        }

        @Test
        @DisplayName("Should handle complex share group IDs")
        void testComplexShareGroupIds() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            String complexGroupId = "share-group_123.with-various.characters";

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(complexGroupId)
                            .setDeserializer(testDeserializer)
                            .build();

            assertThat(source.getShareGroupId()).isEqualTo(complexGroupId);
        }

        @Test
        @DisplayName("Should handle large property sets")
        void testLargePropertySets() {
            assumeTrue(KafkaVersionUtils.isShareGroupSupported());

            Properties largeProps = new Properties();
            for (int i = 0; i < 100; i++) {
                largeProps.setProperty("custom.property." + i, "value." + i);
            }

            KafkaShareGroupSource<String> source =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(TEST_BOOTSTRAP_SERVERS)
                            .setTopics(TEST_TOPIC)
                            .setShareGroupId(TEST_SHARE_GROUP_ID)
                            .setDeserializer(testDeserializer)
                            .setProperties(largeProps)
                            .build();

            Properties config = source.getConfiguration();
            assertThat(config.getProperty("custom.property.50")).isEqualTo("value.50");
        }
    }
}
