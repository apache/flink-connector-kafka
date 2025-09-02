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

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.KafkaShareGroupEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.KafkaShareGroupEnumeratorState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaShareGroupEnumeratorStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaShareGroupSourceReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplit;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka source that uses Kafka 4.1.0+ share group semantics for queue-like consumption.
 * 
 * <p>This source enables message-level consumption and automatic load balancing through
 * Kafka's share group functionality (KIP-932), providing several advantages over traditional
 * partition-based consumption:
 * 
 * <ul>
 *   <li><strong>Message-level distribution:</strong> Messages are distributed across consumers
 *       at the individual message level rather than partition level</li>
 *   <li><strong>Dynamic scaling:</strong> Can scale beyond partition count limitations</li>
 *   <li><strong>Automatic load balancing:</strong> Kafka broker handles load distribution</li>
 *   <li><strong>Improved resource utilization:</strong> Reduces idle consumers</li>
 *   <li><strong>Enhanced fault tolerance:</strong> Failed consumers' work is automatically
 *       redistributed</li>
 * </ul>
 *
 * <h2>Requirements</h2>
 * <ul>
 *   <li>Kafka 4.1.0+ with share group support enabled</li>
 *   <li>Share group ID must be configured</li>
 *   <li>Topics and deserializer must be specified</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * KafkaShareGroupSource<String> source = KafkaShareGroupSource
 *     .<String>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setTopics("orders-topic")
 *     .setShareGroupId("order-processors")
 *     .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
 *     .setStartingOffsets(OffsetsInitializer.earliest())
 *     .build();
 *
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "Share Group Source");
 * }</pre>
 *
 * <p><strong>Note:</strong> This source maintains full compatibility with FLIP-27 unified source API,
 * FLIP-246 dynamic sources, and supports per-partition watermark generation as specified in FLINK-3375.
 *
 * @param <OUT> the output type of the source
 * @see KafkaSource
 * @see KafkaShareGroupSourceBuilder
 */
@PublicEvolving
public class KafkaShareGroupSource<OUT>
        implements Source<OUT, KafkaShareGroupSplit, KafkaShareGroupEnumeratorState>,
                ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSource.class);
    private static final long serialVersionUID = -8755372893283732100L;

    // Configuration inherited from KafkaSource for compatibility
    private final KafkaSubscriber subscriber;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    private final Boundedness boundedness;
    private final KafkaRecordDeserializationSchema<OUT> deserializationSchema;
    private final Properties properties;
    private final SerializableSupplier<String> rackIdSupplier;
    
    // Share group specific configuration
    private final String shareGroupId;
    private final boolean shareGroupMetricsEnabled;

    KafkaShareGroupSource(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            KafkaRecordDeserializationSchema<OUT> deserializationSchema,
            Properties properties,
            SerializableSupplier<String> rackIdSupplier,
            String shareGroupId,
            boolean shareGroupMetricsEnabled) {
        
        this.subscriber = Preconditions.checkNotNull(subscriber, "KafkaSubscriber cannot be null");
        this.startingOffsetsInitializer = Preconditions.checkNotNull(
            startingOffsetsInitializer, "Starting offsets initializer cannot be null");
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = Preconditions.checkNotNull(boundedness, "Boundedness cannot be null");
        this.deserializationSchema = Preconditions.checkNotNull(
            deserializationSchema, "Deserialization schema cannot be null");
        this.properties = new Properties();
        if (properties != null) {
            this.properties.putAll(properties);
        }
        this.rackIdSupplier = rackIdSupplier;
        this.shareGroupId = Preconditions.checkNotNull(shareGroupId, "Share group ID cannot be null");
        Preconditions.checkArgument(!shareGroupId.trim().isEmpty(), 
            "Share group ID cannot be empty");
        this.shareGroupMetricsEnabled = shareGroupMetricsEnabled;
    }

    /**
     * Get a KafkaShareGroupSourceBuilder to build a {@link KafkaShareGroupSource}.
     *
     * @return a Kafka share group source builder
     */
    public static <OUT> KafkaShareGroupSourceBuilder<OUT> builder() {
        return new KafkaShareGroupSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<OUT, KafkaShareGroupSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        
        LOG.info("ShareGroup [{}]: Creating source reader for {} topics with parallelism {}", 
                 shareGroupId, getTopics().size(), readerContext.currentParallelism());
        
        // Configure properties for share group
        Properties shareConsumerProperties = new Properties();
        shareConsumerProperties.putAll(this.properties);
        
        // Ensure share group configuration is applied
        configureShareGroupProperties(shareConsumerProperties);
        
        // Pass topic information to consumer properties
        Set<String> topics = getTopics();
        if (!topics.isEmpty()) {
            shareConsumerProperties.setProperty("topic", topics.iterator().next());
        }
        
        // Create share group metrics if enabled
        KafkaShareGroupSourceMetrics shareGroupMetrics = null;
        if (shareGroupMetricsEnabled) {
            shareGroupMetrics = new KafkaShareGroupSourceMetrics(readerContext.metricGroup());
        }
        
        // Use proper KafkaShareGroupSourceReader with Flink connector architecture
        LOG.info("*** MAIN SOURCE: Creating reader for share group '{}' on subtask {} with consumer properties: {}", 
                shareGroupId, readerContext.getIndexOfSubtask(), shareConsumerProperties.stringPropertyNames());
        
        return new KafkaShareGroupSourceReader<>(
                shareConsumerProperties,
                deserializationSchema,
                readerContext,
                shareGroupMetrics
        );
    }
    
    /**
     * Configures Kafka consumer properties for share group semantics.
     * 
     * @param consumerProperties the properties to configure
     */
    private void configureShareGroupProperties(Properties consumerProperties) {
        // Force share group type - this is the key configuration that enables share group semantics
        consumerProperties.setProperty("group.type", "share");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, shareGroupId);
        
        // Remove properties not supported by share groups
        consumerProperties.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        consumerProperties.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        consumerProperties.remove(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        consumerProperties.remove(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        
        // Configure client ID for better tracking
        if (!consumerProperties.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
            consumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, 
                shareGroupId + "-share-consumer");
        }
    }

    @Override
    public SplitEnumerator<KafkaShareGroupSplit, KafkaShareGroupEnumeratorState> createEnumerator(
            SplitEnumeratorContext<KafkaShareGroupSplit> enumContext) {
        
        Set<String> topics = getTopics();
        LOG.info("ShareGroup [{}]: INIT - Creating enumerator for topics: {} with {} subtasks", 
                 shareGroupId, topics, enumContext.currentParallelism());
        
        // If no topics found from subscriber, try to get from properties as fallback
        if (topics.isEmpty()) {
            String topicFromProps = properties.getProperty("topic");
            if (topicFromProps != null && !topicFromProps.trim().isEmpty()) {
                topics = Collections.singleton(topicFromProps.trim());
                LOG.info("*** MAIN SOURCE: Using fallback topic from properties: {}", topics);
            } else {
                LOG.warn("*** MAIN SOURCE: No topics found from subscriber and no fallback topic in properties!");
            }
        }
        
        return new KafkaShareGroupEnumerator(
                topics,
                shareGroupId,
                null, // no existing state
                enumContext
        );
    }

    @Override
    public SplitEnumerator<KafkaShareGroupSplit, KafkaShareGroupEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<KafkaShareGroupSplit> enumContext,
            KafkaShareGroupEnumeratorState checkpoint)
            throws IOException {
        
        Set<String> topics = checkpoint.getTopics();
        return new KafkaShareGroupEnumerator(
                topics,
                shareGroupId,
                checkpoint,
                enumContext
        );
    }

    @Override
    public SimpleVersionedSerializer<KafkaShareGroupSplit> getSplitSerializer() {
        return new KafkaShareGroupSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KafkaShareGroupEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new KafkaShareGroupEnumeratorStateSerializer();
    }

    @Override
    public org.apache.flink.api.common.typeinfo.TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // TODO: Add proper lineage support when compatible Flink version is available
    // Lineage support would track the share group as a data lineage source

    /**
     * Returns the share group ID configured for this source.
     * 
     * @return the share group ID
     */
    public String getShareGroupId() {
        return shareGroupId;
    }

    /**
     * Returns whether share group metrics are enabled.
     * 
     * @return true if share group metrics are enabled
     */
    public boolean isShareGroupMetricsEnabled() {
        return shareGroupMetricsEnabled;
    }

    /**
     * Returns whether this source uses share group semantics.
     * Always returns true for KafkaShareGroupSource.
     * 
     * @return true
     */
    public boolean isShareGroupEnabled() {
        return true;
    }

    /**
     * Returns the topics subscribed by this source.
     * 
     * @return set of topic names, or empty set if unable to determine
     */
    public Set<String> getTopics() {
        try {
            // Handle TopicListSubscriber
            if (subscriber.getClass().getSimpleName().equals("TopicListSubscriber")) {
                java.lang.reflect.Field topicsField = subscriber.getClass().getDeclaredField("topics");
                topicsField.setAccessible(true);
                List<String> topics = (List<String>) topicsField.get(subscriber);
                LOG.info("*** MAIN SOURCE: Retrieved topics from TopicListSubscriber: {}", topics);
                return new HashSet<>(topics);
            }
            
            // Handle TopicPatternSubscriber  
            if (subscriber.getClass().getSimpleName().equals("TopicPatternSubscriber")) {
                // For pattern subscribers, we'll need to discover topics at runtime
                // For now, return empty set and let enumerator handle discovery
                LOG.info("*** MAIN SOURCE: TopicPatternSubscriber detected - topics will be discovered at runtime");
                return Collections.emptySet();
            }
            
            // Fallback: try reflection methods
            try {
                Object result = subscriber.getClass()
                    .getMethod("getSubscribedTopics")
                    .invoke(subscriber);
                if (result instanceof Set) {
                    Set<String> topics = (Set<String>) result;
                    LOG.info("*** MAIN SOURCE: Retrieved topics via getSubscribedTopics(): {}", topics);
                    return topics;
                }
            } catch (Exception reflectionEx) {
                LOG.debug("getSubscribedTopics() method not found, trying other approaches");
            }
            
            // Try getTopics() method
            try {
                Object result = subscriber.getClass()
                    .getMethod("getTopics")
                    .invoke(subscriber);
                if (result instanceof Collection) {
                    Collection<String> topics = (Collection<String>) result;
                    Set<String> topicSet = new HashSet<>(topics);
                    LOG.info("*** MAIN SOURCE: Retrieved topics via getTopics(): {}", topicSet);
                    return topicSet;
                }
            } catch (Exception reflectionEx) {
                LOG.debug("getTopics() method not found");
            }
            
        } catch (Exception e) {
            LOG.error("*** MAIN SOURCE ERROR: Failed to retrieve topics from subscriber {}: {}", 
                    subscriber.getClass().getSimpleName(), e.getMessage(), e);
        }
        
        LOG.warn("*** MAIN SOURCE: Unable to retrieve topics from subscriber: {} - returning empty set", 
                subscriber.getClass().getSimpleName());
        return Collections.emptySet();
    }

    /**
     * Returns the Kafka subscriber used by this source.
     * 
     * @return the Kafka subscriber
     */
    public KafkaSubscriber getSubscriber() {
        return subscriber;
    }
    
    /**
     * Returns the starting offsets initializer.
     * 
     * @return the starting offsets initializer
     */
    public OffsetsInitializer getStartingOffsetsInitializer() {
        return startingOffsetsInitializer;
    }
    
    /**
     * Returns the stopping offsets initializer.
     * 
     * @return the stopping offsets initializer, may be null
     */
    @Nullable
    public OffsetsInitializer getStoppingOffsetsInitializer() {
        return stoppingOffsetsInitializer;
    }

    @VisibleForTesting
    Properties getConfiguration() {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    @VisibleForTesting
    Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> 
            config.setString(key, props.getProperty(key)));
        return config;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        KafkaShareGroupSource<?> that = (KafkaShareGroupSource<?>) obj;
        return Objects.equals(subscriber, that.subscriber) &&
               Objects.equals(startingOffsetsInitializer, that.startingOffsetsInitializer) &&
               Objects.equals(stoppingOffsetsInitializer, that.stoppingOffsetsInitializer) &&
               Objects.equals(boundedness, that.boundedness) &&
               Objects.equals(deserializationSchema, that.deserializationSchema) &&
               Objects.equals(properties, that.properties) &&
               Objects.equals(shareGroupId, that.shareGroupId) &&
               shareGroupMetricsEnabled == that.shareGroupMetricsEnabled;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(
            subscriber, startingOffsetsInitializer, stoppingOffsetsInitializer,
            boundedness, deserializationSchema, properties, shareGroupId, shareGroupMetricsEnabled
        );
    }
    
    @Override
    public String toString() {
        return "KafkaShareGroupSource{" +
               "shareGroupId='" + shareGroupId + '\'' +
               ", topics=" + getTopics() +
               ", boundedness=" + boundedness +
               ", metricsEnabled=" + shareGroupMetricsEnabled +
               '}';
    }
}
