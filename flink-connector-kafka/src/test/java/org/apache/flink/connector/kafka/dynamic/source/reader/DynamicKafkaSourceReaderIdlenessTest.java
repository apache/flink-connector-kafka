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

package org.apache.flink.connector.kafka.dynamic.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests idleness transitions for {@link DynamicKafkaSourceReader}. */
class DynamicKafkaSourceReaderIdlenessTest {
    private static final String TOPIC = "topic";
    private static final String REMAINING_TOPIC = "remaining-topic";
    private static final String KAFKA_CLUSTER_ID = "cluster-1";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMetadataRemovalMarksReaderAndSplitOutputIdle(boolean outputAvailableBeforeRemoval)
            throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicKafkaSourceReader<byte[]> reader =
                new DynamicKafkaSourceReader<>(
                        context,
                        KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class),
                        getRequiredProperties())) {
            reader.start();
            reader.handleSourceEvents(getMetadataUpdateEvent());

            DynamicKafkaSourceSplit split = getSplit(TOPIC);
            reader.addSplits(Collections.singletonList(split));

            CompletableFuture<Void> availabilityFuture = reader.isAvailable();
            assertThat(availabilityFuture).isNotDone();

            TrackingReaderOutputWithIdleness<byte[]> readerOutput =
                    new TrackingReaderOutputWithIdleness<>();
            if (outputAvailableBeforeRemoval) {
                assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            }

            reader.handleSourceEvents(getMetadataUpdateEvent(REMAINING_TOPIC));
            assertThat(reader.getAvailabilityHelperSize()).isEqualTo(1);
            assertThat(availabilityFuture).isDone();

            assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(readerOutput.splitOutputEvents())
                    .containsExactly("idle:" + split.splitId(), "release:" + split.splitId());
            assertThat(readerOutput.idleCount()).isEqualTo(1);
            assertThat(readerOutput.activeCount()).isZero();
            assertThat(readerOutput.isIdle()).isTrue();
        }
    }

    @Test
    void testSplitReassignmentBeforeIdlePollReactivatesReaderOutput() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        try (DynamicKafkaSourceReader<byte[]> reader =
                new DynamicKafkaSourceReader<>(
                        context,
                        KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class),
                        getRequiredProperties())) {
            reader.start();
            reader.handleSourceEvents(getMetadataUpdateEvent());

            DynamicKafkaSourceSplit removedSplit = getSplit(TOPIC);
            reader.addSplits(Collections.singletonList(removedSplit));

            TrackingReaderOutputWithIdleness<byte[]> readerOutput =
                    new TrackingReaderOutputWithIdleness<>();
            assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

            reader.handleSourceEvents(getMetadataUpdateEvent(REMAINING_TOPIC));
            assertThat(readerOutput.isIdle()).isTrue();

            reader.addSplits(Collections.singletonList(getSplit(REMAINING_TOPIC)));
            assertThat(reader.pollNext(readerOutput)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

            assertThat(readerOutput.splitOutputEvents())
                    .containsExactly(
                            "idle:" + removedSplit.splitId(), "release:" + removedSplit.splitId());
            assertThat(readerOutput.idleCount()).isEqualTo(1);
            assertThat(readerOutput.activeCount()).isEqualTo(1);
            assertThat(readerOutput.isIdle()).isFalse();
        }
    }

    private static MetadataUpdateEvent getMetadataUpdateEvent() {
        return getMetadataUpdateEvent(TOPIC);
    }

    private static MetadataUpdateEvent getMetadataUpdateEvent(String topic) {
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ClusterMetadata clusterMetadata =
                new ClusterMetadata(Collections.singleton(topic), clusterProperties);
        return new MetadataUpdateEvent(
                Collections.singleton(
                        new KafkaStream(
                                TOPIC,
                                Collections.singletonMap(KAFKA_CLUSTER_ID, clusterMetadata))));
    }

    private static DynamicKafkaSourceSplit getSplit(String topic) {
        return new DynamicKafkaSourceSplit(
                KAFKA_CLUSTER_ID,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, 0), 0L, KafkaPartitionSplit.NO_STOPPING_OFFSET));
    }

    private static Properties getRequiredProperties() {
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        return properties;
    }

    private static class TrackingReaderOutputWithIdleness<E> extends TestingReaderOutput<E> {
        private int idleCount;
        private int activeCount;
        private boolean idle;
        private final List<String> splitOutputEvents = new ArrayList<>();

        @Override
        public SourceOutput<E> createOutputForSplit(String splitId) {
            return new SourceOutput<E>() {
                @Override
                public void collect(E record) {}

                @Override
                public void collect(E record, long timestamp) {}

                @Override
                public void emitWatermark(Watermark watermark) {}

                @Override
                public void markIdle() {
                    splitOutputEvents.add("idle:" + splitId);
                    // Model the split-output multiplexer publishing aggregate idleness.
                    idle = true;
                }

                @Override
                public void markActive() {}
            };
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {
            idleCount++;
            idle = true;
        }

        @Override
        public void markActive() {
            activeCount++;
            idle = false;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            splitOutputEvents.add("release:" + splitId);
        }

        private int idleCount() {
            return idleCount;
        }

        private int activeCount() {
            return activeCount;
        }

        private boolean isIdle() {
            return idle;
        }

        private List<String> splitOutputEvents() {
            return splitOutputEvents;
        }
    }
}
