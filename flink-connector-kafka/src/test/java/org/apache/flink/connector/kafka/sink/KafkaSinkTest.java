package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.facets.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.facets.KafkaDatasetFacet.KafkaDatasetIdentifier;
import org.apache.flink.streaming.api.lineage.LineageVertex;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Tests for {@link KafkaSink}. */
public class KafkaSinkTest {

    Properties kafkaProperties;

    @BeforeEach
    void setup() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "host1;host2");
    }

    @Test
    public void testGetLineageVertexWhenSerializerNotAnKafkaDatasetFacetProvider() {
        KafkaRecordSerializationSchema recordSerializer =
                mock(KafkaRecordSerializationSchema.class);
        KafkaSink sink =
                new KafkaSink(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        mock(Properties.class),
                        "",
                        recordSerializer);

        assertThat(sink.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertexWhenNoKafkaDatasetFacetReturnedFromSerializer() {
        KafkaRecordSerializationSchema recordSerializer =
                mock(
                        KafkaRecordSerializationSchema.class,
                        withSettings().extraInterfaces(KafkaDatasetFacetProvider.class));
        when(((KafkaDatasetFacetProvider) recordSerializer).getKafkaDatasetFacet())
                .thenReturn(Optional.empty());

        KafkaSink sink =
                new KafkaSink(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        mock(Properties.class),
                        "",
                        recordSerializer);

        assertThat(sink.getLineageVertex()).isNull();
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

        KafkaSink sink =
                new KafkaSink(
                        DeliveryGuarantee.EXACTLY_ONCE, kafkaProperties, "", recordSerializer);

        LineageVertex lineageVertex = sink.getLineageVertex();

        assertThat(lineageVertex.datasets().get(0).namespace()).isEqualTo("kafka://host1");
        assertThat(lineageVertex.datasets().get(0).name()).isEqualTo("topic1");

        assertThat(lineageVertex.datasets().get(0).facets().get(KafkaDatasetFacet.KAFKA_FACET_NAME))
                .hasFieldOrPropertyWithValue("properties", kafkaProperties)
                .hasFieldOrPropertyWithValue("typeInformation", TypeInformation.of(String.class))
                .hasFieldOrPropertyWithValue(
                        "topicIdentifier",
                        KafkaDatasetIdentifier.of(Collections.singletonList("topic1")));
    }
}
