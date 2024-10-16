package org.apache.flink.connector.kafka.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.LineageFacetProvider;
import org.apache.flink.connector.kafka.lineage.facets.KafkaTopicListFacet;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Tests for {@link KafkaSink}. */
public class KafkaSinkTest {

    @Test
    public void testGetLineageVertex() {
        LineageDatasetFacet facet1 = mock(LineageDatasetFacet.class);
        LineageDatasetFacet facet2 = mock(LineageDatasetFacet.class);
        when(facet1.name()).thenReturn("facet1");
        when(facet2.name()).thenReturn("facet2");
        LineageDatasetFacet topicSelector =
                new KafkaTopicListFacet(Arrays.asList("topic1", "topic2"));

        KafkaRecordSerializationSchema schema =
                mock(
                        KafkaRecordSerializationSchema.class,
                        withSettings().extraInterfaces(LineageFacetProvider.class));

        when(((LineageFacetProvider) schema).getDatasetFacets())
                .thenReturn(Arrays.asList(facet1, facet2, topicSelector));
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "host1;host2");
        KafkaSink sink = new KafkaSink(DeliveryGuarantee.EXACTLY_ONCE, kafkaProperties, "", schema);

        LineageVertex lineageVertex = sink.getLineageVertex();
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
