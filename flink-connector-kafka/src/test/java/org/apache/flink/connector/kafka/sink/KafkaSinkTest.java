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
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
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
        KafkaRecordSerializationSchema<Object> recordSerializer =
                new KafkaRecordSerializationSchemaWithoutKafkaDatasetProvider();
        KafkaSink<Object> sink =
                new KafkaSink<>(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new Properties(),
                        "",
                        recordSerializer,
                        TransactionNamingStrategy.DEFAULT);

        assertThat(sink.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertexWhenNoKafkaDatasetFacetReturnedFromSerializer() {
        KafkaRecordSerializationSchema<Object> recordSerializer =
                new KafkaRecordSerializationSchemaWithEmptyKafkaDatasetProvider();

        KafkaSink<Object> sink =
                new KafkaSink<>(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new Properties(),
                        "",
                        recordSerializer,
                        TransactionNamingStrategy.DEFAULT);

        assertThat(sink.getLineageVertex().datasets()).isEmpty();
    }

    @Test
    public void testGetLineageVertex() {
        KafkaRecordSerializationSchema<Object> recordSerializer =
                new TestingKafkaRecordSerializationSchema();

        KafkaSink<Object> sink =
                new KafkaSink<>(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        kafkaProperties,
                        "",
                        recordSerializer,
                        TransactionNamingStrategy.DEFAULT);

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

    @Test
    public void testCoLocation() {
        String colocationKey = "testCoLocation";
        KafkaSink<Object> sink =
                KafkaSink.builder()
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setKafkaProducerConfig(kafkaProperties)
                        .setTransactionalIdPrefix(colocationKey)
                        .setRecordSerializer(new TestingKafkaRecordSerializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.<Object>fromData(1).sinkTo(sink);

        StreamGraph streamGraph = env.getStreamGraph();
        assertThat(streamGraph.getStreamNodes())
                .filteredOn(node -> !node.getInEdges().isEmpty())
                .hasSize(2) // writer and committer
                .extracting(StreamNode::getCoLocationGroup)
                .containsOnly(colocationKey);
    }

    @Test
    public void testPreserveCustomCoLocation() {
        String colocationKey = "testPreserveCustomCoLocation";
        String customColocationKey = "customCoLocation";
        KafkaSink<Object> sink =
                KafkaSink.builder()
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setKafkaProducerConfig(kafkaProperties)
                        .setTransactionalIdPrefix(colocationKey)
                        .setRecordSerializer(new TestingKafkaRecordSerializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Object> stream = env.<Object>fromData(1).sinkTo(sink);
        stream.getTransformation().setCoLocationGroupKey(customColocationKey);

        StreamGraph streamGraph = env.getStreamGraph();
        assertThat(streamGraph.getStreamNodes())
                .filteredOn(node -> !node.getInEdges().isEmpty())
                .hasSize(2) // writer and committer
                .extracting(StreamNode::getCoLocationGroup)
                .containsOnly(customColocationKey);
    }

    private static class KafkaRecordSerializationSchemaWithoutKafkaDatasetProvider<T>
            implements KafkaRecordSerializationSchema<Object> {
        @Nullable
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Object element, KafkaSinkContext context, Long timestamp) {
            return null;
        }
    }

    private static class KafkaRecordSerializationSchemaWithEmptyKafkaDatasetProvider
            implements KafkaRecordSerializationSchema<Object>, KafkaDatasetFacetProvider {
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
            implements KafkaRecordSerializationSchema<Object>,
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
