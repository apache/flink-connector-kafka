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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacetProvider;
import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaRecordSerializationSchemaBuilder}. */
public class KafkaRecordSerializationSchemaBuilderTest extends TestLogger {

    private static final String DEFAULT_TOPIC = "test";

    private static Map<String, ?> configurableConfiguration;
    private static Map<String, ?> configuration;

    private interface TestingTopicSelector<T>
            extends TopicSelector<T>, KafkaDatasetIdentifierProvider {}

    private interface SerializationSchemaWithResultQueryable<T>
            extends SerializationSchema<T>, ResultTypeQueryable<T> {}

    private static boolean isKeySerializer;

    @Before
    public void setUp() {
        configurableConfiguration = new HashMap<>();
        configuration = new HashMap<>();
        isKeySerializer = false;
    }

    @Test
    public void testDoNotAllowMultipleKeySerializer() {
        assertOnlyOneSerializerAllowed(keySerializationSetter());
    }

    @Test
    public void testDoNotAllowMultipleValueSerializer() {
        assertOnlyOneSerializerAllowed(valueSerializationSetter());
    }

    @Test
    public void testDoNotAllowMultipleTopicSelector() {
        assertThatThrownBy(
                        () ->
                                KafkaRecordSerializationSchema.builder()
                                        .setTopicSelector(e -> DEFAULT_TOPIC)
                                        .setTopic(DEFAULT_TOPIC))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(DEFAULT_TOPIC)
                                        .setTopicSelector(e -> DEFAULT_TOPIC))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testExpectTopicSelector() {
        assertThatThrownBy(
                        KafkaRecordSerializationSchema.builder()
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                ::build)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testExpectValueSerializer() {
        assertThatThrownBy(KafkaRecordSerializationSchema.builder().setTopic(DEFAULT_TOPIC)::build)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSerializeRecordWithTopicSelector() {
        final TopicSelector<String> topicSelector =
                (e) -> {
                    if (e.equals("a")) {
                        return "topic-a";
                    }
                    return "topic-b";
                };
        final KafkaRecordSerializationSchemaBuilder<String> builder =
                KafkaRecordSerializationSchema.builder().setTopicSelector(topicSelector);
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final KafkaRecordSerializationSchema<String> schema =
                builder.setValueSerializationSchema(serializationSchema).build();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(record.topic()).isEqualTo("topic-a");
        assertThat(record.key()).isNull();
        assertThat(record.value()).isEqualTo(serializationSchema.serialize("a"));

        final ProducerRecord<byte[], byte[]> record2 = schema.serialize("b", null, null);
        assertThat(record2.topic()).isEqualTo("topic-b");
        assertThat(record2.key()).isNull();
        assertThat(record2.value()).isEqualTo(serializationSchema.serialize("b"));
    }

    @Test
    public void testSerializeRecordWithPartitioner() throws Exception {
        AtomicBoolean opened = new AtomicBoolean(false);
        final int partition = 5;
        final KafkaPartitioner<Object> partitioner = new ConstantPartitioner<>(opened, partition);
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(partitioner)
                        .build();
        final KafkaRecordSerializationSchema.KafkaSinkContext sinkContext = new TestSinkContext();
        schema.open(null, sinkContext);
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", sinkContext, null);
        assertThat(record.partition()).isEqualTo(partition);
        assertThat(opened.get()).isTrue();
    }

    @Test
    public void testSerializeRecordWithHeaderProvider() throws Exception {
        final HeaderProvider<String> headerProvider =
                (ignored) ->
                        new RecordHeaders(
                                Collections.singletonList(new RecordHeader("a", "a".getBytes())));

        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setHeaderProvider(headerProvider)
                        .build();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(record).isNotNull();
        assertThat(record.headers())
                .singleElement()
                .extracting(Header::key, Header::value)
                .containsExactly("a", "a".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSerializeRecordWithKey() {
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(serializationSchema.serialize("a"))
                .isEqualTo(record.key())
                .isEqualTo(record.value());
    }

    @Test
    public void testKafkaKeySerializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = Collections.singletonMap("simpleKey", "simpleValue");
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        // Use StringSerializer as dummy Serializer, since ValueSerializer is
                        // mandatory.
                        .setKafkaValueSerializer(StringSerializer.class, config)
                        .setKafkaKeySerializer(SimpleStringSerializer.class, config)
                        .build();
        open(schema);
        assertThat(config).isEqualTo(configuration);
        assertThat(isKeySerializer).isTrue();
        assertThat(configurableConfiguration).isEmpty();
    }

    @Test
    public void testKafkaValueSerializerWrapperWithoutConfigurable() throws Exception {
        final Map<String, String> config = Collections.singletonMap("simpleKey", "simpleValue");
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setKafkaValueSerializer(SimpleStringSerializer.class, config)
                        .build();
        open(schema);
        assertThat(config).isEqualTo(configuration);
        assertThat(isKeySerializer).isFalse();
        assertThat(configurableConfiguration).isEmpty();
    }

    @Test
    public void testSerializeRecordWithKafkaSerializer() throws Exception {
        final Map<String, String> config = Collections.singletonMap("configKey", "configValue");
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setKafkaValueSerializer(ConfigurableStringSerializer.class, config)
                        .build();
        open(schema);
        assertThat(config).isEqualTo(configurableConfiguration);
        assertThat(configuration).isEmpty();
        final Deserializer<String> deserializer = new StringDeserializer();
        final ProducerRecord<byte[], byte[]> record = schema.serialize("a", null, null);
        assertThat(deserializer.deserialize(DEFAULT_TOPIC, record.value())).isEqualTo("a");
    }

    @Test
    public void testSerializeRecordWithTimestamp() {
        final SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        final KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopic(DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        final ProducerRecord<byte[], byte[]> recordWithTimestamp =
                schema.serialize("a", null, 100L);
        assertThat((long) recordWithTimestamp.timestamp()).isEqualTo(100L);

        final ProducerRecord<byte[], byte[]> recordWithTimestampZero =
                schema.serialize("a", null, 0L);
        assertThat((long) recordWithTimestampZero.timestamp()).isEqualTo(0L);

        final ProducerRecord<byte[], byte[]> recordWithoutTimestamp =
                schema.serialize("a", null, null);
        assertThat(recordWithoutTimestamp.timestamp()).isNull();

        final ProducerRecord<byte[], byte[]> recordWithInvalidTimestamp =
                schema.serialize("a", null, -100L);
        assertThat(recordWithInvalidTimestamp.timestamp()).isNull();
    }

    @Test
    public void testGetLineageDatasetFacetsWhenTopicSelectorNotKafkaTopicsIdentifierProvider() {
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopicSelector((TopicSelector<Object>) o -> DEFAULT_TOPIC)
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();

        assertThat(schema)
                .asInstanceOf(InstanceOfAssertFactories.type(KafkaDatasetFacetProvider.class))
                .returns(Optional.empty(), KafkaDatasetFacetProvider::getKafkaDatasetFacet);
    }

    @Test
    public void testGetLineageDatasetFacetsWhenNoTopicsIdentifiersFound() {
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopicSelector(
                                new TestingTopicSelector<Object>() {
                                    @Override
                                    public Optional<DefaultKafkaDatasetIdentifier>
                                            getDatasetIdentifier() {
                                        return Optional.empty();
                                    }

                                    @Override
                                    public String apply(Object o) {
                                        return DEFAULT_TOPIC;
                                    }
                                })
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();
        assertThat(schema)
                .asInstanceOf(InstanceOfAssertFactories.type(KafkaDatasetFacetProvider.class))
                .returns(Optional.empty(), KafkaDatasetFacetProvider::getKafkaDatasetFacet);
    }

    @Test
    public void testGetLineageDatasetFacetsValueSerializationSchemaIsResultTypeQueryable() {
        TypeInformation<String> stringTypeInformation = TypeInformation.of(String.class);
        SerializationSchemaWithResultQueryable<String> serializationSchema =
                new SerializationSchemaWithResultQueryable<String>() {

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return stringTypeInformation;
                    }

                    @Override
                    public byte[] serialize(String o) {
                        return new byte[0];
                    }
                };

        KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopicSelector(
                                new TestingTopicSelector<Object>() {
                                    @Override
                                    public Optional<DefaultKafkaDatasetIdentifier>
                                            getDatasetIdentifier() {
                                        return Optional.of(
                                                DefaultKafkaDatasetIdentifier.ofTopics(
                                                        Arrays.asList("topic1", "topic2")));
                                    }

                                    @Override
                                    public String apply(Object o) {
                                        return DEFAULT_TOPIC;
                                    }
                                })
                        .setValueSerializationSchema(serializationSchema)
                        .setKeySerializationSchema(serializationSchema)
                        .build();

        Optional<KafkaDatasetFacet> kafkaDatasetFacet =
                ((KafkaDatasetFacetProvider) schema).getKafkaDatasetFacet();

        assertThat(kafkaDatasetFacet).isPresent();
        assertThat(kafkaDatasetFacet.get().getTopicIdentifier().getTopics())
                .containsExactly("topic1", "topic2");
        assertThat(((TypeDatasetFacetProvider) schema).getTypeDatasetFacet())
                .isPresent()
                .get()
                .extracting(TypeDatasetFacet::getTypeInformation)
                .isEqualTo(stringTypeInformation);
    }

    @Test
    public void testGetLineageDatasetFacets() {
        KafkaRecordSerializationSchema<String> schema =
                KafkaRecordSerializationSchema.builder()
                        .setTopicSelector(
                                new TestingTopicSelector<Object>() {
                                    @Override
                                    public Optional<DefaultKafkaDatasetIdentifier>
                                            getDatasetIdentifier() {
                                        return Optional.of(
                                                DefaultKafkaDatasetIdentifier.ofTopics(
                                                        Arrays.asList("topic1", "topic2")));
                                    }

                                    @Override
                                    public String apply(Object o) {
                                        return DEFAULT_TOPIC;
                                    }
                                })
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setKeySerializationSchema(new SimpleStringSchema())
                        .build();

        Optional<KafkaDatasetFacet> kafkaDatasetFacet =
                ((KafkaDatasetFacetProvider) schema).getKafkaDatasetFacet();

        assertThat(kafkaDatasetFacet).isPresent();
        assertThat(kafkaDatasetFacet.get().getTopicIdentifier().getTopics())
                .containsExactly("topic1", "topic2");
        assertThat(((TypeDatasetFacetProvider) schema).getTypeDatasetFacet())
                .isPresent()
                .get()
                .extracting(TypeDatasetFacet::getTypeInformation)
                .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
    }

    private static void assertOnlyOneSerializerAllowed(
            List<
                            Function<
                                    KafkaRecordSerializationSchemaBuilder<String>,
                                    KafkaRecordSerializationSchemaBuilder<String>>>
                    serializers) {
        for (final Function<
                        KafkaRecordSerializationSchemaBuilder<String>,
                        KafkaRecordSerializationSchemaBuilder<String>>
                setter : serializers) {
            final KafkaRecordSerializationSchemaBuilder<String> builder =
                    KafkaRecordSerializationSchema.<String>builder().setTopic(DEFAULT_TOPIC);
            setter.apply(builder);
            for (final Function<
                            KafkaRecordSerializationSchemaBuilder<String>,
                            KafkaRecordSerializationSchemaBuilder<String>>
                    updater : serializers) {
                assertThatThrownBy(() -> updater.apply(builder))
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    private static List<
                    Function<
                            KafkaRecordSerializationSchemaBuilder<String>,
                            KafkaRecordSerializationSchemaBuilder<String>>>
            valueSerializationSetter() {
        return Arrays.asList(
                (b) -> b.setKafkaValueSerializer(StringSerializer.class),
                (b) -> b.setValueSerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setKafkaValueSerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    private static List<
                    Function<
                            KafkaRecordSerializationSchemaBuilder<String>,
                            KafkaRecordSerializationSchemaBuilder<String>>>
            keySerializationSetter() {
        return Arrays.asList(
                (b) -> b.setKafkaKeySerializer(StringSerializer.class),
                (b) -> b.setKeySerializationSchema(new SimpleStringSchema()),
                (b) ->
                        b.setKafkaKeySerializer(
                                ConfigurableStringSerializer.class, Collections.emptyMap()));
    }

    /**
     * Serializer based on Kafka's serialization stack. This is the special case that implements
     * {@link Configurable}
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class ConfigurableStringSerializer extends StringSerializer
            implements Configurable {
        @Override
        public void configure(Map<String, ?> configs) {
            configurableConfiguration = configs;
        }
    }

    /**
     * Serializer based on Kafka's serialization stack.
     *
     * <p>This class must be public to make it instantiable by the tests.
     */
    public static class SimpleStringSerializer extends StringSerializer {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            configuration = configs;
            isKeySerializer = isKey;
        }
    }

    private static class TestSinkContext
            implements KafkaRecordSerializationSchema.KafkaSinkContext {
        @Override
        public int getParallelInstanceId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelInstances() {
            return 0;
        }

        @Override
        public int[] getPartitionsForTopic(String topic) {
            return new int[0];
        }
    }

    private static class ConstantPartitioner<T> implements KafkaPartitioner<T> {

        private final AtomicBoolean opened;
        private final int partition;

        ConstantPartitioner(AtomicBoolean opened, int partition) {
            this.opened = opened;
            this.partition = partition;
        }

        @Override
        public void open(int parallelInstanceId, int parallelInstances) {
            opened.set(true);
        }

        @Override
        public int partition(
                T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return partition;
        }
    }

    private void open(KafkaRecordSerializationSchema<String> schema) throws Exception {
        schema.open(new DummyInitializationContext(), null);
    }
}
