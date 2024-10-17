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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.DefaultTypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
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

    private interface TestingKafkaSubscriber
            extends KafkaSubscriber, KafkaDatasetIdentifierProvider {}

    @BeforeEach
    void setup() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "host1;host2");
    }

    @Test
    public void testGetLineageVertexWhenSubscriberNotAnKafkaDatasetFacetProvider() {
        KafkaSource source =
                new KafkaSource(
                        new KafkaSubscriber() {
                            @Override
                            public Set<TopicPartition> getSubscribedTopicPartitions(
                                    AdminClient adminClient) {
                                return null;
                            }
                        },
                        null,
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        null,
                        kafkaProperties,
                        null);
        assertThat(source.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertexWhenNoKafkaTopicsIdentifier() {
        KafkaSource source =
                new KafkaSource(
                        new TestingKafkaSubscriber() {
                            @Override
                            public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
                                return Optional.empty();
                            }

                            @Override
                            public Set<TopicPartition> getSubscribedTopicPartitions(
                                    AdminClient adminClient) {
                                return null;
                            }
                        },
                        null,
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        null,
                        kafkaProperties,
                        null);
        assertThat(source.getLineageVertex().datasets()).isEmpty();
        assertThat(source.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertex() {
        TypeInformation<String> typeInformation = TypeInformation.of(String.class);
        KafkaSource source =
                new KafkaSource(
                        new TestingKafkaSubscriber() {
                            @Override
                            public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
                                return Optional.of(
                                        DefaultKafkaDatasetIdentifier.ofTopics(
                                                Collections.singletonList("topic1")));
                            }

                            @Override
                            public Set<TopicPartition> getSubscribedTopicPartitions(
                                    AdminClient adminClient) {
                                return null;
                            }
                        },
                        null,
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        new KafkaRecordDeserializationSchema() {
                            @Override
                            public void deserialize(ConsumerRecord record, Collector out)
                                    throws IOException {}

                            @Override
                            public TypeInformation getProducedType() {
                                return typeInformation;
                            }
                        },
                        kafkaProperties,
                        null);

        LineageVertex lineageVertex = source.getLineageVertex();
        assertThat(lineageVertex.datasets()).hasSize(1);

        assertThat(lineageVertex.datasets().get(0).namespace()).isEqualTo("kafka://host1");
        assertThat(lineageVertex.datasets().get(0).name()).isEqualTo("topic1");

        assertThat(
                        lineageVertex
                                .datasets()
                                .get(0)
                                .facets()
                                .get(DefaultKafkaDatasetFacet.KAFKA_FACET_NAME))
                .hasFieldOrPropertyWithValue("properties", kafkaProperties)
                .hasFieldOrPropertyWithValue(
                        "topicIdentifier",
                        DefaultKafkaDatasetIdentifier.ofTopics(
                                Collections.singletonList("topic1")));

        assertThat(
                        lineageVertex
                                .datasets()
                                .get(0)
                                .facets()
                                .get(DefaultTypeDatasetFacet.TYPE_FACET_NAME))
                .hasFieldOrPropertyWithValue("typeInformation", TypeInformation.of(String.class));
    }
}
