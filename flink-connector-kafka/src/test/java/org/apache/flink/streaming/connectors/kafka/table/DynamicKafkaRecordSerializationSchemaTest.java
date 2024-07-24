package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link DynamicKafkaRecordSerializationSchema}. */
public class DynamicKafkaRecordSerializationSchemaTest extends TestLogger {
    private static final List<String> TOPICS = Arrays.asList("topic1;topic2".split(";"));
    private static final String TOPIC = "topic";

    @ParameterizedTest
    @MethodSource("provideTopicMetadataTestParameters")
    public void testTopicMetadata(List<String> topics, String rowTopic, String expectedTopic) {
        GenericRowData rowData = createRowData(rowTopic);
        DynamicKafkaRecordSerializationSchema schema = createSchema(topics);
        KafkaRecordSerializationSchema.KafkaSinkContext context = createContext();

        // Call serialize method
        ProducerRecord<byte[], byte[]> record = schema.serialize(rowData, context, null);

        // Assert the returned ProducerRecord is routed to the correct topic
        assertEquals(record.topic(), expectedTopic);
    }

    @ParameterizedTest
    @MethodSource("provideInvalidTopicMetadataTestParameters")
    public void testInvalidTopicMetadata(
            List<String> topics, String rowTopic, String expectedError) {
        GenericRowData rowData = createRowData(rowTopic);
        DynamicKafkaRecordSerializationSchema schema = createSchema(topics);
        KafkaRecordSerializationSchema.KafkaSinkContext context = createContext();

        // Call serialize method
        assertThatThrownBy(() -> schema.serialize(rowData, context, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedError);
    }

    private static Stream<Arguments> provideTopicMetadataTestParameters() {
        String topic1 = "topic1";
        return Stream.of(
                Arguments.of(Collections.singletonList(TOPIC), TOPIC, TOPIC),
                Arguments.of(Collections.singletonList(TOPIC), topic1, TOPIC),
                Arguments.of(Collections.singletonList(TOPIC), null, TOPIC),
                Arguments.of(null, TOPIC, TOPIC),
                Arguments.of(TOPICS, topic1, topic1));
    }

    private static Stream<Arguments> provideInvalidTopicMetadataTestParameters() {
        String other = "other";
        return Stream.of(
                Arguments.of(
                        null,
                        null,
                        "The topic of the sink record is not valid. Expected a single topic but no topic is set."),
                Arguments.of(
                        TOPICS,
                        other,
                        String.format(
                                "The topic of the sink record is not valid. Expected topic to be in: %s but was: %s",
                                TOPICS, other)));
    }

    private DynamicKafkaRecordSerializationSchema createSchema(List<String> topics) {
        // Create a SerializationSchema for RowData
        SerializationSchema<RowData> serializationSchema =
                new SerializationSchema<RowData>() {
                    @Override
                    public byte[] serialize(RowData element) {
                        return ((StringData) element.getString(0)).toBytes();
                    }

                    @Override
                    public void open(InitializationContext context) throws Exception {}
                };

        int[] metadataPositions = new int[3];
        metadataPositions[KafkaDynamicSink.WritableMetadata.TOPIC.ordinal()] = 1;
        metadataPositions[KafkaDynamicSink.WritableMetadata.HEADERS.ordinal()] = 2;
        metadataPositions[KafkaDynamicSink.WritableMetadata.TIMESTAMP.ordinal()] = 3;

        return new DynamicKafkaRecordSerializationSchema(
                topics,
                null,
                null,
                serializationSchema,
                new RowData.FieldGetter[] {r -> r.getString(0)},
                new RowData.FieldGetter[] {r -> r.getString(0)},
                true,
                metadataPositions,
                false);
    }

    private GenericRowData createRowData(String topic) {
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, StringData.fromString("test"));
        rowData.setField(1, StringData.fromString(topic));
        rowData.setField(2, null);
        rowData.setField(3, null);
        return rowData;
    }

    private KafkaRecordSerializationSchema.KafkaSinkContext createContext() {
        return new KafkaRecordSerializationSchema.KafkaSinkContext() {
            @Override
            public int getParallelInstanceId() {
                return 0;
            }

            @Override
            public int getNumberOfParallelInstances() {
                return 1;
            }

            @Override
            public int[] getPartitionsForTopic(String topic) {
                return new int[] {0};
            }
        };
    }
}
