/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.StreamPatternSubscriber;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/** A builder class to make it easier for users to construct a {@link DynamicKafkaSource}. */
@Experimental
public class DynamicKafkaSourceBuilder<T> {
    private static final Logger logger = LoggerFactory.getLogger(DynamicKafkaSourceBuilder.class);
    private KafkaStreamSubscriber kafkaStreamSubscriber;
    private KafkaMetadataService kafkaMetadataService;
    private KafkaRecordDeserializationSchema<T> deserializationSchema;
    private OffsetsInitializer startingOffsetsInitializer;
    private OffsetsInitializer stoppingOffsetsInitializer;
    private Boundedness boundedness;
    private final Properties props;

    DynamicKafkaSourceBuilder() {
        this.kafkaStreamSubscriber = null;
        this.kafkaMetadataService = null;
        this.deserializationSchema = null;
        this.startingOffsetsInitializer = OffsetsInitializer.earliest();
        this.stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.props = new Properties();
    }

    /**
     * Set the stream ids belonging to the {@link KafkaMetadataService}.
     *
     * @param streamIds the stream ids.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setStreamIds(Set<String> streamIds) {
        Preconditions.checkNotNull(streamIds);
        ensureSubscriberIsNull("streamIds");
        this.kafkaStreamSubscriber = new KafkaStreamSetSubscriber(streamIds);
        return this;
    }

    /**
     * Set the stream pattern to determine stream ids belonging to the {@link KafkaMetadataService}.
     *
     * @param streamPattern the stream pattern.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setStreamPattern(Pattern streamPattern) {
        Preconditions.checkNotNull(streamPattern);
        ensureSubscriberIsNull("stream pattern");
        this.kafkaStreamSubscriber = new StreamPatternSubscriber(streamPattern);
        return this;
    }

    /**
     * Set a custom Kafka stream subscriber.
     *
     * @param kafkaStreamSubscriber the {@link KafkaStreamSubscriber}.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setKafkaStreamSubscriber(
            KafkaStreamSubscriber kafkaStreamSubscriber) {
        Preconditions.checkNotNull(kafkaStreamSubscriber);
        ensureSubscriberIsNull("custom");
        this.kafkaStreamSubscriber = kafkaStreamSubscriber;
        return this;
    }

    /**
     * Set the source in bounded mode and specify what offsets to end at. This is used for all
     * clusters unless overridden by cluster metadata.
     *
     * @param stoppingOffsetsInitializer the {@link OffsetsInitializer}.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setBounded(OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.BOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    /**
     * Set the {@link KafkaMetadataService}.
     *
     * @param kafkaMetadataService the {@link KafkaMetadataService}.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setKafkaMetadataService(
            KafkaMetadataService kafkaMetadataService) {
        this.kafkaMetadataService = kafkaMetadataService;
        return this;
    }

    /**
     * Set the {@link KafkaRecordDeserializationSchema}.
     *
     * @param recordDeserializer the {@link KafkaRecordDeserializationSchema}.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setDeserializer(
            KafkaRecordDeserializationSchema<T> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    /**
     * Set the starting offsets of the stream. This will be applied to all clusters unless
     * overridden by cluster metadata.
     *
     * @param startingOffsetsInitializer the {@link OffsetsInitializer}.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setStartingOffsets(
            OffsetsInitializer startingOffsetsInitializer) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        return this;
    }

    /**
     * Set the properties of the consumers. This will be applied to all clusters and properties like
     * {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} may be overriden by the {@link
     * KafkaMetadataService}.
     *
     * @param properties the properties.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setProperties(Properties properties) {
        this.props.putAll(properties);
        return this;
    }

    /**
     * Set a property for the consumers. This will be applied to all clusters and properties like
     * {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} may be overriden by the {@link
     * KafkaMetadataService}.
     *
     * @param key the property key.
     * @param value the properties value.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setProperty(String key, String value) {
        this.props.setProperty(key, value);
        return this;
    }

    /**
     * Set the enumerator mode used for split assignment.
     *
     * @param enumeratorMode split assignment mode.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setEnumeratorMode(
            DynamicKafkaSourceOptions.EnumeratorMode enumeratorMode) {
        Preconditions.checkNotNull(enumeratorMode);
        return setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                enumeratorMode.name().toLowerCase(Locale.ROOT));
    }

    /**
     * Set the property for {@link CommonClientConfigs#GROUP_ID_CONFIG}. This will be applied to all
     * clusters.
     *
     * @param groupId the group id.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setGroupId(String groupId) {
        return setProperty(CommonClientConfigs.GROUP_ID_CONFIG, groupId);
    }

    /**
     * Set the client id prefix. This applies {@link KafkaSourceOptions#CLIENT_ID_PREFIX} to all
     * clusters.
     *
     * @param prefix the client id prefix.
     * @return the builder.
     */
    public DynamicKafkaSourceBuilder<T> setClientIdPrefix(String prefix) {
        return setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
    }

    /**
     * Construct the source with the configuration that was set.
     *
     * @return the {@link DynamicKafkaSource}.
     */
    public DynamicKafkaSource<T> build() {
        logger.info("Building the DynamicKafkaSource");
        sanityCheck();
        setRequiredConsumerProperties();
        return new DynamicKafkaSource<>(
                kafkaStreamSubscriber,
                kafkaMetadataService,
                deserializationSchema,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                boundedness);
    }

    // Below are utility methods, code and structure are mostly copied over from KafkaSourceBuilder

    private void setRequiredConsumerProperties() {
        maybeOverride(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName(),
                true);
        maybeOverride(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName(),
                true);
        if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            logger.warn(
                    "Offset commit on checkpoint is disabled because {} is not specified",
                    ConsumerConfig.GROUP_ID_CONFIG);
            maybeOverride(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false", false);
        }
        maybeOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false", false);
        maybeOverride(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                startingOffsetsInitializer.getAutoOffsetResetStrategy().name().toLowerCase(),
                true);

        // If the source is bounded, do not run periodic partition discovery.
        maybeOverride(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                "-1",
                boundedness == Boundedness.BOUNDED);

        // If the source is bounded, do not run periodic metadata discovery
        maybeOverride(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(),
                "-1",
                boundedness == Boundedness.BOUNDED);
        maybeOverride(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD.key(),
                "0",
                boundedness == Boundedness.BOUNDED);

        // If the client id prefix is not set, reuse the consumer group id as the client id prefix,
        // or generate a random string if consumer group id is not specified.
        maybeOverride(
                KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
                props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)
                        ? props.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
                        : "DynamicKafkaSource-" + RandomStringUtils.randomAlphabetic(8),
                false);
    }

    private boolean maybeOverride(String key, String value, boolean override) {
        boolean overridden = false;
        String userValue = props.getProperty(key);
        if (userValue != null) {
            if (override) {
                logger.warn(
                        String.format(
                                "Property %s is provided but will be overridden from %s to %s",
                                key, userValue, value));
                props.setProperty(key, value);
                overridden = true;
            }
        } else {
            props.setProperty(key, value);
        }
        return overridden;
    }

    private void sanityCheck() {
        Preconditions.checkNotNull(
                kafkaStreamSubscriber, "Kafka stream subscriber is required but not provided");
        Preconditions.checkNotNull(
                kafkaMetadataService, "Kafka Metadata Service is required but not provided");
        Preconditions.checkNotNull(
                deserializationSchema, "Deserialization schema is required but not provided.");

        // Check consumer group ID
        Preconditions.checkState(
                props.containsKey(ConsumerConfig.GROUP_ID_CONFIG) || !offsetCommitEnabledManually(),
                String.format(
                        "Property %s is required when offset commit is enabled",
                        ConsumerConfig.GROUP_ID_CONFIG));
    }

    private boolean offsetCommitEnabledManually() {
        boolean autoCommit =
                props.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                        && Boolean.parseBoolean(
                                props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        boolean commitOnCheckpoint =
                props.containsKey(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key())
                        && Boolean.parseBoolean(
                                props.getProperty(
                                        KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key()));
        return autoCommit || commitOnCheckpoint;
    }

    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (kafkaStreamSubscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode,
                            kafkaStreamSubscriber.getClass().getSimpleName()));
        }
    }
}
