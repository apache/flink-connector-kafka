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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.DefaultTypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacetProvider;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct {@link KafkaRecordSerializationSchema}.
 *
 * <p>This class should give a first entrypoint when trying to serialize elements to {@link
 * ProducerRecord}. The following examples show some of the possibilities.
 *
 * <pre>Simple key-value serialization:
 * {@code
 * KafkaRecordSerializationSchema.builder()
 *     .setTopic("topic)
 *     .setKeySerializationSchema(new SimpleStringSchema())
 *     .setValueSerializationSchema(new SimpleStringSchema())
 *     .build()
 * }</pre>
 *
 * <pre>Using Kafka's serialization stack:
 * {@code
 * KafkaRecordSerializationSchema.builder()
 *     .setTopic("topic)
 *     .setKeySerializer(StringSerializer.class)
 *     .setKafkaValueSerializer(StringSerializer.class)
 *     .build()
 * }</pre>
 *
 * <pre>With custom partitioner:
 * {@code
 * KafkaRecordSerializationSchema.builder()
 *     .setTopic("topic)
 *     .setPartitioner(MY_FLINK_PARTITIONER)
 *     .setValueSerializationSchema(StringSerializer.class)
 *     .build()
 * }</pre>
 *
 * <p>The different serialization methods for key and value are mutually exclusive thus i.e. it is
 * not possible to use {@link #setKeySerializationSchema(SerializationSchema)} and {@link
 * #setKafkaKeySerializer(Class)} on the same builder instance.
 *
 * <p>It is necessary to configure exactly one serialization method for the value and a topic.
 *
 * @param <IN> type of records to be serialized
 * @see KafkaRecordSerializationSchema#builder()
 */
@PublicEvolving
public class KafkaRecordSerializationSchemaBuilder<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    @Nullable private Function<? super IN, String> topicSelector;
    @Nullable private SerializationSchema<? super IN> valueSerializationSchema;
    @Nullable private KafkaPartitioner<? super IN> partitioner;
    @Nullable private SerializationSchema<? super IN> keySerializationSchema;
    @Nullable private HeaderProvider<? super IN> headerProvider;

    /**
     * Sets a custom partitioner determining the target partition of the target topic.
     *
     * @param partitioner
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setPartitioner(
            KafkaPartitioner<? super T> partitioner) {
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.partitioner = checkNotNull(partitioner);
        return self;
    }

    /**
     * Sets a fixed topic which used as destination for all records.
     *
     * @param topic
     * @return {@code this}
     */
    public KafkaRecordSerializationSchemaBuilder<IN> setTopic(String topic) {
        checkState(this.topicSelector == null, "Topic selector already set.");
        checkNotNull(topic);

        this.topicSelector = new ConstantTopicSelector<>(topic);
        return this;
    }

    /**
     * Sets a topic selector which computes the target topic for every incoming record.
     *
     * @param topicSelector
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setTopicSelector(
            TopicSelector<? super T> topicSelector) {
        checkState(this.topicSelector == null, "Topic selector already set.");
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.topicSelector = new CachingTopicSelector<>(checkNotNull(topicSelector));
        return self;
    }

    /**
     * Sets a {@link SerializationSchema} which is used to serialize the incoming element to the key
     * of the {@link ProducerRecord}.
     *
     * @param keySerializationSchema
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setKeySerializationSchema(
            SerializationSchema<? super T> keySerializationSchema) {
        checkKeySerializerNotSet();
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.keySerializationSchema = checkNotNull(keySerializationSchema);
        return self;
    }

    /**
     * Sets Kafka's {@link Serializer} to serialize incoming elements to the key of the {@link
     * ProducerRecord}.
     *
     * @param keySerializer
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setKafkaKeySerializer(
            Class<? extends Serializer<? super T>> keySerializer) {
        checkKeySerializerNotSet();
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.keySerializationSchema =
                new KafkaSerializerWrapper<>(keySerializer, true, topicSelector);
        return self;
    }

    /**
     * Sets a configurable Kafka {@link Serializer} and pass a configuration to serialize incoming
     * elements to the key of the {@link ProducerRecord}.
     *
     * @param keySerializer
     * @param configuration
     * @param <S> type of the used serializer class
     * @return {@code this}
     */
    public <T extends IN, S extends Serializer<? super T>>
            KafkaRecordSerializationSchemaBuilder<T> setKafkaKeySerializer(
                    Class<S> keySerializer, Map<String, String> configuration) {
        checkKeySerializerNotSet();
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.keySerializationSchema =
                new KafkaSerializerWrapper<>(keySerializer, true, configuration, topicSelector);
        return self;
    }

    /**
     * Sets a {@link SerializationSchema} which is used to serialize the incoming element to the
     * value of the {@link ProducerRecord}.
     *
     * @param valueSerializationSchema
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setValueSerializationSchema(
            SerializationSchema<T> valueSerializationSchema) {
        checkValueSerializerNotSet();
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.valueSerializationSchema = checkNotNull(valueSerializationSchema);
        return self;
    }

    /**
     * Sets a {@link HeaderProvider} which is used to add headers to the {@link ProducerRecord} for
     * the current element.
     *
     * @param headerProvider
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setHeaderProvider(
            HeaderProvider<? super T> headerProvider) {
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.headerProvider = checkNotNull(headerProvider);
        return self;
    }

    @SuppressWarnings("unchecked")
    private <T extends IN> KafkaRecordSerializationSchemaBuilder<T> self() {
        return (KafkaRecordSerializationSchemaBuilder<T>) this;
    }

    /**
     * Sets Kafka's {@link Serializer} to serialize incoming elements to the value of the {@link
     * ProducerRecord}.
     *
     * @param valueSerializer
     * @return {@code this}
     */
    public <T extends IN> KafkaRecordSerializationSchemaBuilder<T> setKafkaValueSerializer(
            Class<? extends Serializer<? super T>> valueSerializer) {
        checkValueSerializerNotSet();
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.valueSerializationSchema =
                new KafkaSerializerWrapper<>(valueSerializer, false, topicSelector);
        return self;
    }

    /**
     * Sets a configurable Kafka {@link Serializer} and pass a configuration to serialize incoming
     * elements to the value of the {@link ProducerRecord}.
     *
     * @param valueSerializer
     * @param configuration
     * @param <S> type of the used serializer class
     * @return {@code this}
     */
    public <T extends IN, S extends Serializer<? super T>>
            KafkaRecordSerializationSchemaBuilder<T> setKafkaValueSerializer(
                    Class<S> valueSerializer, Map<String, String> configuration) {
        checkValueSerializerNotSet();
        KafkaRecordSerializationSchemaBuilder<T> self = self();
        self.valueSerializationSchema =
                new KafkaSerializerWrapper<>(valueSerializer, false, configuration, topicSelector);
        return self;
    }

    /**
     * Constructs the {@link KafkaRecordSerializationSchemaBuilder} with the configured properties.
     *
     * @return {@link KafkaRecordSerializationSchema}
     */
    public KafkaRecordSerializationSchema<IN> build() {
        checkState(valueSerializationSchema != null, "No value serializer is configured.");
        checkState(topicSelector != null, "No topic selector is configured.");
        return new KafkaRecordSerializationSchemaWrapper<>(
                topicSelector,
                valueSerializationSchema,
                keySerializationSchema,
                partitioner,
                headerProvider);
    }

    private void checkValueSerializerNotSet() {
        checkState(valueSerializationSchema == null, "Value serializer already set.");
    }

    private void checkKeySerializerNotSet() {
        checkState(keySerializationSchema == null, "Key serializer already set.");
    }

    private static class ConstantTopicSelector<IN>
            implements Function<IN, String>, Serializable, KafkaDatasetIdentifierProvider {

        private String topic;

        ConstantTopicSelector(String topic) {
            this.topic = topic;
        }

        @Override
        public String apply(IN in) {
            return topic;
        }

        @Override
        public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
            return Optional.of(
                    DefaultKafkaDatasetIdentifier.ofTopics(Collections.singletonList(topic)));
        }
    }

    private static class CachingTopicSelector<IN>
            implements Function<IN, String>, KafkaDatasetIdentifierProvider, Serializable {

        private static final int CACHE_RESET_SIZE = 5;
        private final Map<IN, String> cache;
        private final TopicSelector<IN> topicSelector;

        CachingTopicSelector(TopicSelector<IN> topicSelector) {
            this.topicSelector = topicSelector;
            this.cache = new HashMap<>();
        }

        @Override
        public String apply(IN in) {
            final String topic = cache.getOrDefault(in, topicSelector.apply(in));
            cache.put(in, topic);
            if (cache.size() >= CACHE_RESET_SIZE) {
                cache.clear();
            }
            return topic;
        }

        @Override
        public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
            if (topicSelector instanceof KafkaDatasetIdentifierProvider) {
                return ((KafkaDatasetIdentifierProvider) topicSelector).getDatasetIdentifier();
            } else {
                return Optional.empty();
            }
        }
    }

    private static class KafkaRecordSerializationSchemaWrapper<IN>
            implements KafkaDatasetFacetProvider,
                    KafkaRecordSerializationSchema<IN>,
                    TypeDatasetFacetProvider {
        private final SerializationSchema<? super IN> valueSerializationSchema;
        private final Function<? super IN, String> topicSelector;
        private final KafkaPartitioner<? super IN> partitioner;
        private final SerializationSchema<? super IN> keySerializationSchema;
        private final HeaderProvider<? super IN> headerProvider;

        KafkaRecordSerializationSchemaWrapper(
                Function<? super IN, String> topicSelector,
                SerializationSchema<? super IN> valueSerializationSchema,
                @Nullable SerializationSchema<? super IN> keySerializationSchema,
                @Nullable KafkaPartitioner<? super IN> partitioner,
                @Nullable HeaderProvider<? super IN> headerProvider) {
            this.topicSelector = checkNotNull(topicSelector);
            this.valueSerializationSchema = checkNotNull(valueSerializationSchema);
            this.partitioner = partitioner;
            this.keySerializationSchema = keySerializationSchema;
            this.headerProvider = headerProvider;
        }

        @Override
        public void open(
                SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
                throws Exception {
            valueSerializationSchema.open(context);
            if (keySerializationSchema != null) {
                keySerializationSchema.open(context);
            }
            if (partitioner != null) {
                partitioner.open(
                        sinkContext.getParallelInstanceId(),
                        sinkContext.getNumberOfParallelInstances());
            }
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                IN element, KafkaSinkContext context, Long timestamp) {
            final String targetTopic = topicSelector.apply(element);
            final byte[] value = valueSerializationSchema.serialize(element);
            byte[] key = null;
            if (keySerializationSchema != null) {
                key = keySerializationSchema.serialize(element);
            }
            final OptionalInt partition =
                    partitioner != null
                            ? OptionalInt.of(
                                    partitioner.partition(
                                            element,
                                            key,
                                            value,
                                            targetTopic,
                                            context.getPartitionsForTopic(targetTopic)))
                            : OptionalInt.empty();

            return new ProducerRecord<>(
                    targetTopic,
                    partition.isPresent() ? partition.getAsInt() : null,
                    timestamp == null || timestamp < 0L ? null : timestamp,
                    key,
                    value,
                    headerProvider != null ? headerProvider.getHeaders(element) : null);
        }

        @Override
        public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
            if (!(topicSelector instanceof KafkaDatasetIdentifierProvider)) {
                LOG.info("Cannot identify topics. Not an TopicsIdentifierProvider");
                return Optional.empty();
            }

            Optional<DefaultKafkaDatasetIdentifier> topicsIdentifier =
                    ((KafkaDatasetIdentifierProvider) (topicSelector)).getDatasetIdentifier();

            if (!topicsIdentifier.isPresent()) {
                LOG.info("No topics' identifiers provided");
                return Optional.empty();
            }

            return Optional.of(new DefaultKafkaDatasetFacet(topicsIdentifier.get()));
        }

        @Override
        public Optional<TypeDatasetFacet> getTypeDatasetFacet() {
            if (this.valueSerializationSchema instanceof ResultTypeQueryable) {
                return Optional.of(
                        new DefaultTypeDatasetFacet(
                                ((ResultTypeQueryable<?>) this.valueSerializationSchema)
                                        .getProducedType()));
            } else {
                // gets type information from serialize method signature
                Type type =
                        TypeExtractor.getParameterType(
                                SerializationSchema.class, valueSerializationSchema.getClass(), 0);
                try {
                    return Optional.of(
                            new DefaultTypeDatasetFacet(TypeExtractor.createTypeInfo(type)));
                } catch (Exception e) {
                    LOG.info(
                            "Could not extract type information from {}",
                            valueSerializationSchema.getClass(),
                            e);
                }
            }
            return Optional.empty();
        }
    }
}
