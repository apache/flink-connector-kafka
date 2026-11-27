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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.DefaultTypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaSource}. */
public class KafkaSourceTest {
    Properties kafkaProperties;

    @BeforeEach
    void setup() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "host1;host2");
    }

    @Test
    public void testGetLineageVertexWhenSubscriberNotAnKafkaDatasetFacetProvider() {
        KafkaSource<String> source =
                new KafkaSourceBuilder<String>()
                        .setKafkaSubscriber(
                                new KafkaSubscriber() {
                                    @Override
                                    public Set<TopicPartition> getSubscribedTopicPartitions(
                                            AdminClient adminClient) {
                                        return null;
                                    }
                                })
                        .setProperties(kafkaProperties)
                        .setGroupId("")
                        .setDeserializer(
                                new KafkaRecordDeserializationSchema<String>() {
                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return null;
                                    }

                                    @Override
                                    public void deserialize(
                                            ConsumerRecord<byte[], byte[]> record,
                                            Collector<String> out)
                                            throws IOException {}
                                })
                        .setUnbounded(OffsetsInitializer.committedOffsets())
                        .build();

        assertThat(source.getLineageVertex())
                .extracting(LineageVertex::datasets)
                .asList()
                .isEmpty();
    }

    @Test
    public void testGetLineageVertexWhenNoKafkaTopicsIdentifier() {
        KafkaSource<String> source =
                new KafkaSourceBuilder<String>()
                        .setKafkaSubscriber(
                                new TestingKafkaSubscriber() {
                                    @Override
                                    public Optional<DefaultKafkaDatasetIdentifier>
                                            getDatasetIdentifier() {
                                        return Optional.empty();
                                    }
                                })
                        .setProperties(kafkaProperties)
                        .setGroupId("")
                        .setDeserializer(
                                new KafkaRecordDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            ConsumerRecord<byte[], byte[]> record,
                                            Collector<String> out)
                                            throws IOException {}

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return TypeInformation.of(String.class);
                                    }
                                })
                        .setUnbounded(OffsetsInitializer.committedOffsets())
                        .build();
        assertThat(source.getLineageVertex())
                .extracting(LineageVertex::datasets)
                .asList()
                .isEmpty();
    }

    @Test
    public void testGetLineageVertex() {
        TypeInformation<String> typeInformation = TypeInformation.of(String.class);
        KafkaSource<String> source =
                new KafkaSourceBuilder<String>()
                        .setKafkaSubscriber(new TestingKafkaSubscriber())
                        .setProperties(kafkaProperties)
                        .setGroupId("")
                        .setDeserializer(
                                new KafkaRecordDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            ConsumerRecord<byte[], byte[]> record,
                                            Collector<String> out)
                                            throws IOException {}

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return typeInformation;
                                    }
                                })
                        .setUnbounded(OffsetsInitializer.committedOffsets())
                        .build();

        LineageVertex lineageVertex = source.getLineageVertex();
        assertThat(lineageVertex.datasets()).hasSize(1);
        LineageDataset dataset = lineageVertex.datasets().get(0);

        assertThat(dataset.namespace()).isEqualTo("kafka://host1");
        assertThat(dataset.name()).isEqualTo("topic1");

        assertThat(dataset.facets()).containsKey(DefaultKafkaDatasetFacet.KAFKA_FACET_NAME);
        DefaultKafkaDatasetFacet kafkaFacet =
                (DefaultKafkaDatasetFacet)
                        dataset.facets().get(DefaultKafkaDatasetFacet.KAFKA_FACET_NAME);

        assertThat(kafkaFacet.getProperties()).containsEntry("bootstrap.servers", "host1;host2");
        assertThat(kafkaFacet.config()).containsEntry("bootstrap.servers", "host1;host2");

        assertThat(dataset.facets()).containsKey(DefaultTypeDatasetFacet.TYPE_FACET_NAME);
        assertThat(dataset.facets().get(DefaultTypeDatasetFacet.TYPE_FACET_NAME))
                .hasFieldOrPropertyWithValue("typeInformation", TypeInformation.of(String.class));
    }

    @Test
    public void testEnumeratorRegistersRemovedPartitionsGauge() throws Exception {
        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setProperties(kafkaProperties)
                        .setTopics("topic1")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        MetricListener createListener = new MetricListener();
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        contextWithMetricGroup(createListener);
                SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> enumerator =
                        source.createEnumerator(context)) {
            assertRemovedPartitionsGauge(createListener);
        }

        MetricListener restoreListener = new MetricListener();
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        contextWithMetricGroup(restoreListener);
                SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> enumerator =
                        source.restoreEnumerator(
                                context, new KafkaSourceEnumState(Collections.emptySet(), true))) {
            assertRemovedPartitionsGauge(restoreListener);
        }
    }

    private static MockSplitEnumeratorContext<KafkaPartitionSplit> contextWithMetricGroup(
            MetricListener metricListener) {
        return new MockSplitEnumeratorContext<KafkaPartitionSplit>(1) {
            @Override
            public SplitEnumeratorMetricGroup metricGroup() {
                return new InternalSplitEnumeratorMetricGroup(metricListener.getMetricGroup());
            }
        };
    }

    private static void assertRemovedPartitionsGauge(MetricListener metricListener) {
        assertThat(
                        metricListener.<Integer>getGauge(
                                "enumerator",
                                KafkaSourceEnumerator.KAFKA_SOURCE_ENUMERATOR_METRIC_GROUP,
                                KafkaSourceEnumerator.REMOVED_PARTITIONS_METRIC_GAUGE))
                .hasValueSatisfying(gauge -> assertThat(gauge.getValue()).isZero());
    }

    private static class TestingKafkaSubscriber
            implements KafkaSubscriber, KafkaDatasetIdentifierProvider {
        @Override
        public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
            return Optional.of(
                    DefaultKafkaDatasetIdentifier.ofTopics(Collections.singletonList("topic1")));
        }

        @Override
        public Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
            return null;
        }
    }
}
