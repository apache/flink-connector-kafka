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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.lineage.LineageFacetProvider;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Tests for {@link KafkaSource}. */
public class KafkaSourceTest {

    @Test
    public void testGetLineageVertex() {
        LineageDatasetFacet facet1 = mock(LineageDatasetFacet.class);
        LineageDatasetFacet facet2 = mock(LineageDatasetFacet.class);
        when(facet1.name()).thenReturn("facet1");
        when(facet2.name()).thenReturn("facet2");

        KafkaRecordDeserializationSchema schema =
                mock(
                        KafkaRecordDeserializationSchema.class,
                        withSettings().extraInterfaces(LineageFacetProvider.class));

        when(((LineageFacetProvider) schema).getDatasetFacets())
                .thenReturn(Arrays.asList(facet1, facet2));
        Properties kafkaProperties = new Properties();

        kafkaProperties.put("bootstrap.servers", "host1;host2");
        KafkaSource source =
                new KafkaSource(
                        KafkaSubscriber.getTopicListSubscriber(Arrays.asList("topic1", "topic2")),
                        mock(OffsetsInitializer.class),
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        schema,
                        kafkaProperties,
                        null);

        LineageVertex lineageVertex = source.getLineageVertex();
        assertThat(lineageVertex.datasets()).hasSize(2);

        assertThat(lineageVertex.datasets().get(0).namespace()).isEqualTo("kafka://host1");
        assertThat(lineageVertex.datasets().get(0).name()).isEqualTo("topic1");

        assertThat(lineageVertex.datasets().get(1).namespace()).isEqualTo("kafka://host1");
        assertThat(lineageVertex.datasets().get(1).name()).isEqualTo("topic2");

        // facets shall be the same for both datasets
        assertThat(lineageVertex.datasets().get(0).facets())
                .isEqualTo(lineageVertex.datasets().get(1).facets());

        assertThat(lineageVertex.datasets().get(0).facets())
                .containsEntry("facet1", facet1)
                .containsEntry("facet2", facet2);
    }
}
