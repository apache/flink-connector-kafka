package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.DefaultTypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacetProvider;
import org.apache.flink.streaming.api.lineage.LineageVertex;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

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
                new KafkaRecordSerializationSchemaWithoutKafkaDatasetProvider();
        KafkaSink sink =
                new KafkaSink(
                        DeliveryGuarantee.EXACTLY_ONCE, new Properties(), "", recordSerializer);

        assertThat(sink.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertexWhenNoKafkaDatasetFacetReturnedFromSerializer() {
        KafkaRecordSerializationSchema recordSerializer =
                new KafkaRecordSerializationSchemaWithEmptyKafkaDatasetProvider();

        KafkaSink sink =
                new KafkaSink(
                        DeliveryGuarantee.EXACTLY_ONCE, new Properties(), "", recordSerializer);

        assertThat(sink.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertex() {
        KafkaRecordSerializationSchema recordSerializer =
                new TestingKafkaRecordSerializationSchema();

        KafkaSink sink =
                new KafkaSink(
                        DeliveryGuarantee.EXACTLY_ONCE, kafkaProperties, "", recordSerializer);

        LineageVertex lineageVertex = sink.getLineageVertex();

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

    private static class KafkaRecordSerializationSchemaWithoutKafkaDatasetProvider
            implements KafkaRecordSerializationSchema {
        @Nullable
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Object element, KafkaSinkContext context, Long timestamp) {
            return null;
        }
    }

    private static class KafkaRecordSerializationSchemaWithEmptyKafkaDatasetProvider
            implements KafkaRecordSerializationSchema, KafkaDatasetFacetProvider {
        @Nullable
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Object element, KafkaSinkContext context, Long timestamp) {
            return null;
        }

        @Override
        public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
            return Optional.empty();
        }
    }

    private static class TestingKafkaRecordSerializationSchema
            implements KafkaRecordSerializationSchema,
                    KafkaDatasetFacetProvider,
                    TypeDatasetFacetProvider {

        @Override
        public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
            return Optional.of(
                    new DefaultKafkaDatasetFacet(
                            DefaultKafkaDatasetIdentifier.ofTopics(
                                    Collections.singletonList("topic1"))));
        }

        @Nullable
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Object element, KafkaSinkContext context, Long timestamp) {
            return null;
        }

        @Override
        public Optional<TypeDatasetFacet> getTypeDatasetFacet() {
            return Optional.of(new DefaultTypeDatasetFacet(TypeInformation.of(String.class)));
        }
    }
}
