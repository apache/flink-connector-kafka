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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.util.KafkaVersionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test demonstrating comprehensive usage of {@link KafkaShareGroupSource}.
 *
 * <p>This test showcases real-world usage patterns including:
 *
 * <ul>
 *   <li>Share group source configuration and setup
 *   <li>Integration with Flink streaming environment
 *   <li>Watermark strategy configuration
 *   <li>Custom processing functions
 *   <li>Metrics and monitoring setup
 *   <li>Error handling and recovery
 * </ul>
 *
 * <p><strong>Note:</strong> These tests demonstrate configuration and setup without requiring a
 * running Kafka cluster. For actual message processing tests, a real Kafka environment would be
 * needed.
 */
@DisplayName("KafkaShareGroupSource Integration Tests")
class KafkaShareGroupSourceIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaShareGroupSourceIntegrationTest.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SHARE_GROUP_ID = "integration-test-group";
    private static final String[] TEST_TOPICS = {"orders", "payments", "inventory"};

    private StreamExecutionEnvironment env;
    private KafkaRecordDeserializationSchema<String> deserializer;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        deserializer = KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema());
    }

    @Test
    @DisplayName("Should demonstrate basic share group source usage")
    void testBasicShareGroupSourceUsage() throws Exception {
        assumeTrue(
                KafkaVersionUtils.isShareGroupSupported(),
                "Share groups not supported in current Kafka version");

        // Create share group source with basic configuration
        KafkaShareGroupSource<String> source =
                KafkaShareGroupSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics(TEST_TOPICS)
                        .setShareGroupId(SHARE_GROUP_ID)
                        .setDeserializer(deserializer)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build();

        // Create data stream with watermark strategy
        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "ShareGroupKafkaSource");

        // Verify stream setup
        assertThat(stream).isNotNull();
        assertThat(stream.getType()).isEqualTo(Types.STRING);

        // Verify source configuration
        assertThat(source.getShareGroupId()).isEqualTo(SHARE_GROUP_ID);
        assertThat(source.isShareGroupEnabled()).isTrue();

        LOG.info("✅ Basic share group source setup completed successfully");
    }

    @Test
    @DisplayName("Should demonstrate advanced share group configuration")
    void testAdvancedShareGroupConfiguration() throws Exception {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported());

        // Advanced properties for production use
        Properties advancedProps = new Properties();
        advancedProps.setProperty("session.timeout.ms", "45000");
        advancedProps.setProperty("heartbeat.interval.ms", "15000");
        advancedProps.setProperty("max.poll.records", "1000");
        advancedProps.setProperty("fetch.min.bytes", "50000");
        advancedProps.setProperty("fetch.max.wait.ms", "500");

        KafkaShareGroupSource<String> source =
                KafkaShareGroupSource.<String>builder()
                        .setBootstrapServers(
                                "kafka-cluster-1:9092,kafka-cluster-2:9092,kafka-cluster-3:9092")
                        .setTopics(TEST_TOPICS)
                        .setShareGroupId("production-order-processing-group")
                        .setDeserializer(deserializer)
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .enableShareGroupMetrics(true)
                        .setProperties(advancedProps)
                        .build();

        // Verify advanced configuration
        Properties config = source.getConfiguration();
        assertThat(config.getProperty("session.timeout.ms")).isEqualTo("45000");
        assertThat(config.getProperty("max.poll.records")).isEqualTo("1000");
        assertThat(config.getProperty("group.type")).isEqualTo("share");
        assertThat(source.isShareGroupMetricsEnabled()).isTrue();

        LOG.info("✅ Advanced share group configuration validated");
    }

    @Test
    @DisplayName("Should demonstrate processing pipeline with share group source")
    void testProcessingPipelineIntegration() throws Exception {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported());

        KafkaShareGroupSource<String> source =
                KafkaShareGroupSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics("user-events")
                        .setShareGroupId("analytics-processing")
                        .setDeserializer(deserializer)
                        .enableShareGroupMetrics(true)
                        .build();

        // Create processing pipeline
        DataStream<String> events =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "UserEventsSource");

        AtomicInteger processedCount = new AtomicInteger(0);

        // Add processing function
        DataStream<ProcessedEvent> processed =
                events.process(new EventProcessingFunction(processedCount))
                        .name("ProcessUserEvents");

        // Verify pipeline setup
        assertThat(processed).isNotNull();
        assertThat(processed.getType().getTypeClass()).isEqualTo(ProcessedEvent.class);

        LOG.info("✅ Processing pipeline integration completed");
    }

    @Test
    @DisplayName("Should demonstrate watermark strategy integration")
    void testWatermarkStrategyIntegration() throws Exception {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported());

        KafkaShareGroupSource<String> source =
                KafkaShareGroupSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics("timestamped-events")
                        .setShareGroupId("watermark-test-group")
                        .setDeserializer(deserializer)
                        .build();

        // Custom watermark strategy with idleness handling
        WatermarkStrategy<String> watermarkStrategy =
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
                        .withIdleness(java.time.Duration.ofSeconds(30));

        DataStream<String> stream =
                env.fromSource(source, watermarkStrategy, "TimestampedEventsSource");

        assertThat(stream).isNotNull();

        LOG.info("✅ Watermark strategy integration validated");
    }

    @Test
    @DisplayName("Should demonstrate multi-source setup with traditional and share group sources")
    void testMultiSourceSetup() throws Exception {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported());

        // Traditional Kafka source for control data
        KafkaSource<String> traditionalSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics("control-messages")
                        .setGroupId("control-group")
                        .setDeserializer(deserializer)
                        .build();

        // Share group source for high-throughput data
        KafkaShareGroupSource<String> shareGroupSource =
                KafkaShareGroupSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics("high-volume-data")
                        .setShareGroupId("data-processing-group")
                        .setDeserializer(deserializer)
                        .enableShareGroupMetrics(true)
                        .build();

        // Create streams
        DataStream<String> controlStream =
                env.fromSource(
                        traditionalSource, WatermarkStrategy.noWatermarks(), "ControlSource");

        DataStream<String> dataStream =
                env.fromSource(shareGroupSource, WatermarkStrategy.noWatermarks(), "DataSource");

        // Union streams for combined processing
        DataStream<String> combined = controlStream.union(dataStream);

        assertThat(combined).isNotNull();

        LOG.info("✅ Multi-source setup with traditional and share group sources validated");
    }

    @Test
    @DisplayName("Should demonstrate error handling and configuration validation")
    void testErrorHandlingAndValidation() {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported());

        // Test that proper error handling works
        try {
            // This should work fine
            KafkaShareGroupSource<String> validSource =
                    KafkaShareGroupSource.<String>builder()
                            .setBootstrapServers(BOOTSTRAP_SERVERS)
                            .setTopics("valid-topic")
                            .setShareGroupId("valid-group")
                            .setDeserializer(deserializer)
                            .build();

            assertThat(validSource).isNotNull();

            // Test configuration access
            Properties config = validSource.getConfiguration();
            assertThat(config.getProperty("group.type")).isEqualTo("share");
            assertThat(config.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
                    .isEqualTo("false");

        } catch (Exception e) {
            LOG.error("Unexpected error in valid configuration test", e);
            throw e;
        }

        LOG.info("✅ Error handling and validation test completed");
    }

    @Test
    @DisplayName("Should demonstrate compatibility with existing Flink features")
    void testFlinkFeatureCompatibility() throws Exception {
        assumeTrue(KafkaVersionUtils.isShareGroupSupported());

        KafkaShareGroupSource<String> source =
                KafkaShareGroupSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics("compatibility-test")
                        .setShareGroupId("compatibility-group")
                        .setDeserializer(deserializer)
                        .build();

        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "CompatibilityTestSource");

        // Test various Flink operations
        DataStream<String> processed =
                stream.filter(value -> !value.isEmpty())
                        .map(String::toUpperCase)
                        .keyBy(value -> value.hashCode() % 10)
                        .process(
                                new KeyedProcessFunction<Integer, String, String>() {
                                    @Override
                                    public void processElement(
                                            String value, Context ctx, Collector<String> out) {
                                        out.collect("Processed: " + value);
                                    }
                                });

        assertThat(processed).isNotNull();

        LOG.info("✅ Flink feature compatibility validated");
    }

    /** Sample processing function for demonstration. */
    private static class EventProcessingFunction extends ProcessFunction<String, ProcessedEvent> {

        private final AtomicInteger counter;

        public EventProcessingFunction(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void processElement(String value, Context ctx, Collector<ProcessedEvent> out) {

            int count = counter.incrementAndGet();
            long timestamp = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();

            ProcessedEvent event = new ProcessedEvent(value, timestamp, count);

            out.collect(event);
        }
    }

    /** Sample event class for processing pipeline demonstration. */
    public static class ProcessedEvent {

        private final String originalValue;
        private final long timestamp;
        private final int sequenceNumber;

        public ProcessedEvent(String originalValue, long timestamp, int sequenceNumber) {
            this.originalValue = originalValue;
            this.timestamp = timestamp;
            this.sequenceNumber = sequenceNumber;
        }

        public String getOriginalValue() {
            return originalValue;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return String.format(
                    "ProcessedEvent{value='%s', timestamp=%d, seq=%d}",
                    originalValue, timestamp, sequenceNumber);
        }
    }
}
