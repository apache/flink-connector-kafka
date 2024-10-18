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
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;
import org.apache.flink.connector.kafka.lineage.facets.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.facets.KafkaDatasetFacet.KafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.lineage.LineageVertex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
        KafkaSource source =
                new KafkaSource(
                        mock(KafkaSubscriber.class),
                        mock(OffsetsInitializer.class),
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        mock(KafkaRecordDeserializationSchema.class),
                        kafkaProperties,
                        null);
        assertThat(source.getLineageVertex()).isNull();
    }

    @Test
    public void testGetLineageVertexWhenNoKafkaTopicsIdentifier() {
        KafkaSubscriber subscriber =
                mock(
                        KafkaSubscriber.class,
                        withSettings().extraInterfaces(KafkaDatasetIdentifierProvider.class));
        when(((KafkaDatasetIdentifierProvider) subscriber).getDatasetIdentifier())
                .thenReturn(Optional.empty());

        KafkaSource source =
                new KafkaSource(
                        subscriber,
                        mock(OffsetsInitializer.class),
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        mock(KafkaRecordDeserializationSchema.class),
                        kafkaProperties,
                        null);
        assertThat(source.getLineageVertex()).isNull();
    }

    @Test
    public void testGetLineageVertex() {
        KafkaRecordSerializationSchema recordSerializer =
                mock(
                        KafkaRecordSerializationSchema.class,
                        withSettings().extraInterfaces(KafkaDatasetFacetProvider.class));

        when(((KafkaDatasetFacetProvider) recordSerializer).getKafkaDatasetFacet())
                .thenReturn(
                        Optional.of(
                                new KafkaDatasetFacet(
                                        KafkaDatasetIdentifier.of(
                                                Collections.singletonList("topic1")),
                                        new Properties(),
                                        TypeInformation.of(String.class))));

        TypeInformation typeInformation = mock(TypeInformation.class);
        KafkaRecordDeserializationSchema schema = mock(KafkaRecordDeserializationSchema.class);
        when(schema.getProducedType()).thenReturn(typeInformation);

        KafkaSource source =
                new KafkaSource(
                        KafkaSubscriber.getTopicListSubscriber(Arrays.asList("topic1")),
                        mock(OffsetsInitializer.class),
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        schema,
                        kafkaProperties,
                        null);

        LineageVertex lineageVertex = source.getLineageVertex();
        assertThat(lineageVertex.datasets()).hasSize(1);

        assertThat(lineageVertex.datasets().get(0).namespace()).isEqualTo("kafka://host1");
        assertThat(lineageVertex.datasets().get(0).name()).isEqualTo("topic1");

        assertThat(lineageVertex.datasets().get(0).facets().get(KafkaDatasetFacet.KAFKA_FACET_NAME))
                .hasFieldOrPropertyWithValue("properties", kafkaProperties)
                .hasFieldOrPropertyWithValue("typeInformation", typeInformation)
                .hasFieldOrPropertyWithValue(
                        "topicIdentifier",
                        KafkaDatasetIdentifier.of(Collections.singletonList("topic1")));
    }
}
