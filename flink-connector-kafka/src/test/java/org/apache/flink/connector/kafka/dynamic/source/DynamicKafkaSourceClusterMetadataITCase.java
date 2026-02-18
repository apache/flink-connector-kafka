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

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.reader.KafkaClusterAwareDeserializer;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for cluster metadata injection in {@link DynamicKafkaSource}. */
public class DynamicKafkaSourceClusterMetadataITCase {

    private static final String TOPIC = "DynamicKafkaSourceClusterMetadataITCase";
    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_RECORDS_PER_SPLIT = 5;

    private static final InMemoryReporter REPORTER = InMemoryReporter.create();

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .setConfiguration(REPORTER.addToConfiguration(new Configuration()))
                            .build());

    @BeforeAll
    static void beforeAll() throws Throwable {
        DynamicKafkaSourceTestHelper.setup();
        DynamicKafkaSourceTestHelper.createTopic(TOPIC, NUM_PARTITIONS, 1);
        DynamicKafkaSourceTestHelper.produceToKafka(TOPIC, NUM_PARTITIONS, NUM_RECORDS_PER_SPLIT);
    }

    @AfterAll
    static void afterAll() throws Exception {
        REPORTER.close();
        DynamicKafkaSourceTestHelper.tearDown();
    }

    @Test
    void testClusterMetadataInjection() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");

        MockKafkaMetadataService mockKafkaMetadataService =
                new MockKafkaMetadataService(
                        Collections.singleton(DynamicKafkaSourceTestHelper.getKafkaStream(TOPIC)));

        DynamicKafkaSource<String> dynamicKafkaSource =
                DynamicKafkaSource.<String>builder()
                        .setStreamIds(
                                mockKafkaMetadataService.getAllStreams().stream()
                                        .map(KafkaStream::getStreamId)
                                        .collect(Collectors.toSet()))
                        .setKafkaMetadataService(mockKafkaMetadataService)
                        .setDeserializer(new ClusterIdDeserializationSchema())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setProperties(properties)
                        .build();

        DataStreamSource<String> stream =
                env.fromSource(
                        dynamicKafkaSource, WatermarkStrategy.noWatermarks(), "dynamic-kafka-src");
        CloseableIterator<String> iterator = stream.executeAndCollect();
        List<String> results = new ArrayList<>();
        int expectedCount =
                DynamicKafkaSourceTestHelper.NUM_KAFKA_CLUSTERS
                        * NUM_PARTITIONS
                        * NUM_RECORDS_PER_SPLIT;
        while (results.size() < expectedCount && iterator.hasNext()) {
            results.add(iterator.next());
        }
        iterator.close();

        assertThat(results).hasSize(expectedCount);

        String cluster0 = DynamicKafkaSourceTestHelper.getKafkaClusterId(0);
        String cluster1 = DynamicKafkaSourceTestHelper.getKafkaClusterId(1);
        assertThat(results).contains(cluster0, cluster1);

        Map<String, Long> counts =
                results.stream()
                        .collect(Collectors.groupingBy(value -> value, Collectors.counting()));
        assertThat(counts)
                .containsEntry(cluster0, (long) NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT)
                .containsEntry(cluster1, (long) NUM_PARTITIONS * NUM_RECORDS_PER_SPLIT);
    }

    private static final class ClusterIdDeserializationSchema
            implements KafkaRecordDeserializationSchema<String>, KafkaClusterAwareDeserializer {
        private static final long serialVersionUID = 1L;

        private @Nullable String kafkaClusterId;

        @Override
        public boolean needsKafkaClusterId() {
            return true;
        }

        @Override
        public void setKafkaClusterId(@Nullable String kafkaClusterId) {
            this.kafkaClusterId = kafkaClusterId;
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
            out.collect(
                    Preconditions.checkNotNull(
                            kafkaClusterId,
                            "Kafka cluster id is missing for dynamic source metadata"));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
