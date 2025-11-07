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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The builder class for {@link KafkaShareGroupSource} to make it easier for users to construct a
 * share group-based Kafka source.
 *
 * <p>The following example shows the minimum setup to create a KafkaShareGroupSource that reads
 * String values from Kafka topics using share group semantics:
 *
 * <pre>{@code
 * KafkaShareGroupSource<String> source = KafkaShareGroupSource
 *     .<String>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setTopics("my-topic")
 *     .setShareGroupId("my-share-group")
 *     .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
 *     .build();
 * }</pre>
 *
 * <p>The bootstrap servers, topics, share group ID, and deserializer are required fields. This
 * source requires Kafka 4.1.0+ with share group support enabled.
 *
 * @param <OUT> the output type of the source
 */
@PublicEvolving
public class KafkaShareGroupSourceBuilder<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSourceBuilder.class);
    private static final String[] REQUIRED_CONFIGS = {ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG};

    // Core configuration inherited from KafkaSourceBuilder
    private KafkaSubscriber subscriber;
    private OffsetsInitializer startingOffsetsInitializer;
    private OffsetsInitializer stoppingOffsetsInitializer;
    private Boundedness boundedness;
    private KafkaRecordDeserializationSchema<OUT> deserializationSchema;
    private Properties props;
    private SerializableSupplier<String> rackIdSupplier;

    // Share group specific configuration
    private String shareGroupId;
    private boolean shareGroupMetricsEnabled;

    KafkaShareGroupSourceBuilder() {
        this.subscriber = null;
        this.startingOffsetsInitializer = OffsetsInitializer.earliest();
        this.stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.deserializationSchema = null;
        this.props = new Properties();
        this.rackIdSupplier = null;
        this.shareGroupId = null;
        this.shareGroupMetricsEnabled = false;
    }

    /**
     * Sets the bootstrap servers for the Kafka consumer.
     *
     * @param bootstrapServers the bootstrap servers of the Kafka cluster
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setBootstrapServers(String bootstrapServers) {
        return setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    /**
     * Sets the share group ID for share group semantics. This is required for share group-based
     * consumption. The share group ID is used to coordinate message distribution across multiple
     * consumers.
     *
     * @param shareGroupId the share group ID
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setShareGroupId(String shareGroupId) {
        this.shareGroupId = checkNotNull(shareGroupId, "Share group ID cannot be null");
        return this;
    }

    /**
     * Set a list of topics the KafkaShareGroupSource should consume from.
     *
     * @param topics the list of topics to consume from
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setTopics(List<String> topics) {
        ensureSubscriberIsNull("topics");
        subscriber = KafkaSubscriber.getTopicListSubscriber(topics);
        return this;
    }

    /**
     * Set a list of topics the KafkaShareGroupSource should consume from.
     *
     * @param topics the list of topics to consume from
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setTopics(String... topics) {
        return setTopics(Arrays.asList(topics));
    }

    /**
     * Set a topic pattern to consume from using Java {@link Pattern}.
     *
     * @param topicPattern the pattern of the topic name to consume from
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setTopicPattern(Pattern topicPattern) {
        ensureSubscriberIsNull("topic pattern");
        subscriber = KafkaSubscriber.getTopicPatternSubscriber(topicPattern);
        return this;
    }

    /**
     * Set a set of partitions to consume from.
     *
     * @param partitions the set of partitions to consume from
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setPartitions(Set<TopicPartition> partitions) {
        ensureSubscriberIsNull("partitions");
        subscriber = KafkaSubscriber.getPartitionSetSubscriber(partitions);
        return this;
    }

    /**
     * Specify from which offsets the KafkaShareGroupSource should start consuming.
     *
     * @param startingOffsetsInitializer the {@link OffsetsInitializer} setting starting offsets
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setStartingOffsets(
            OffsetsInitializer startingOffsetsInitializer) {
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        return this;
    }

    /**
     * Set the source to run as bounded and stop at the specified offsets.
     *
     * @param stoppingOffsetsInitializer the {@link OffsetsInitializer} to specify stopping offsets
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setBounded(
            OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.BOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    /**
     * Set the source to run as unbounded but stop at the specified offsets.
     *
     * @param stoppingOffsetsInitializer the {@link OffsetsInitializer} to specify stopping offsets
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setUnbounded(
            OffsetsInitializer stoppingOffsetsInitializer) {
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        return this;
    }

    /**
     * Sets the {@link KafkaRecordDeserializationSchema deserializer} of the ConsumerRecord.
     *
     * @param recordDeserializer the deserializer for Kafka ConsumerRecord
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setDeserializer(
            KafkaRecordDeserializationSchema<OUT> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    /**
     * Sets the {@link DeserializationSchema} for deserializing only the value of ConsumerRecord.
     *
     * @param deserializationSchema the {@link DeserializationSchema} to use
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setValueOnlyDeserializer(
            DeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema =
                KafkaRecordDeserializationSchema.valueOnly(deserializationSchema);
        return this;
    }

    /**
     * Sets the client id prefix of this KafkaShareGroupSource.
     *
     * @param prefix the client id prefix to use for this KafkaShareGroupSource
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setClientIdPrefix(String prefix) {
        return setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
    }

    /**
     * Enable or disable share group-specific metrics.
     *
     * @param enabled whether to enable share group metrics
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> enableShareGroupMetrics(boolean enabled) {
        this.shareGroupMetricsEnabled = enabled;
        return this;
    }

    /**
     * Set an arbitrary property for the KafkaShareGroupSource and consumer.
     *
     * @param key the key of the property
     * @param value the value of the property
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setProperty(String key, String value) {
        props.setProperty(key, value);
        return this;
    }

    /**
     * Set arbitrary properties for the KafkaShareGroupSource and consumer.
     *
     * @param props the properties to set for the KafkaShareGroupSource
     * @return this KafkaShareGroupSourceBuilder
     */
    public KafkaShareGroupSourceBuilder<OUT> setProperties(Properties props) {
        // Validate share group-specific properties
        validateShareGroupProperties(props);
        this.props.putAll(props);
        return this;
    }

    /**
     * Build the {@link KafkaShareGroupSource}.
     *
     * @return a KafkaShareGroupSource with the share group configuration
     */
    public KafkaShareGroupSource<OUT> build() {
        sanityCheck();
        parseAndSetRequiredProperties();

        return new KafkaShareGroupSource<>(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                deserializationSchema,
                props,
                rackIdSupplier,
                shareGroupId,
                shareGroupMetricsEnabled);
    }

    // Private helper methods

    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (subscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
        }
    }

    private void parseAndSetRequiredProperties() {
        // Set key and value deserializers to byte array
        maybeOverride(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName(),
                false);
        maybeOverride(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName(),
                false);

        // Share group specific overrides
        maybeOverride("group.type", "share", true); // Force share group type
        maybeOverride("group.id", shareGroupId, true); // Use share group ID as group ID
        maybeOverride(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false", true);

        // Set auto offset reset strategy
        maybeOverride(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                startingOffsetsInitializer.getAutoOffsetResetStrategy().name().toLowerCase(),
                true);

        // Set client ID prefix for share group
        maybeOverride(
                KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
                shareGroupId + "-share-consumer-" + new Random().nextLong(),
                false);
    }

    private boolean maybeOverride(String key, String value, boolean override) {
        boolean overridden = false;
        String userValue = props.getProperty(key);
        if (userValue != null) {
            if (override) {
                LOG.warn(
                        "Property {} is provided but will be overridden from {} to {} for share group semantics",
                        key,
                        userValue,
                        value);
                props.setProperty(key, value);
                overridden = true;
            }
        } else {
            props.setProperty(key, value);
        }
        return overridden;
    }

    private void sanityCheck() {
        // Check required configs
        for (String requiredConfig : REQUIRED_CONFIGS) {
            checkNotNull(
                    props.getProperty(requiredConfig),
                    String.format("Property %s is required but not provided", requiredConfig));
        }

        // Check required settings
        checkState(
                subscriber != null,
                "No topics specified. Use setTopics(), setTopicPattern(), or setPartitions().");

        checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");

        checkState(
                shareGroupId != null && !shareGroupId.trim().isEmpty(),
                "Share group ID is required for share group semantics");
    }

    private void validateShareGroupProperties(Properties props) {
        // Validate that group.type is set to 'share' if specified
        String groupType = props.getProperty("group.type");
        if (groupType != null && !"share".equals(groupType)) {
            throw new IllegalArgumentException(
                    "group.type must be 'share' for share group semantics, but was: " + groupType);
        }
    }
}
