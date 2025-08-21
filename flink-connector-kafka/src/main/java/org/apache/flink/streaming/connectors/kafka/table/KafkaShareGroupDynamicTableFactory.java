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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.source.KafkaShareGroupSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Flink SQL Table Factory for Kafka Share Group Source.
 * 
 * <p>This factory creates table sources that use Kafka 4.1.0+ share group semantics
 * for queue-like message consumption in Flink SQL applications.
 * 
 * <p>Usage in Flink SQL:
 * <pre>{@code
 * CREATE TABLE kafka_share_source (
 *   message STRING,
 *   event_time TIMESTAMP(3),
 *   WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
 * ) WITH (
 *   'connector' = 'kafka-sharegroup',
 *   'bootstrap.servers' = 'localhost:9092',
 *   'share-group-id' = 'my-share-group',
 *   'topic' = 'my-topic',
 *   'format' = 'json'
 * );
 * }</pre>
 */
@PublicEvolving
public class KafkaShareGroupDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "kafka-sharegroup";
    
    // Share group specific options
    public static final ConfigOption<String> SHARE_GROUP_ID = ConfigOptions
            .key("share-group-id")
            .stringType()
            .noDefaultValue()
            .withDescription("The share group ID for queue-like consumption.");
            
    public static final ConfigOption<Integer> SOURCE_PARALLELISM = ConfigOptions
            .key("source.parallelism")
            .intType()
            .noDefaultValue()
            .withDescription("Parallelism for the share group source. Allows more subtasks than topic partitions.");
    
    public static final ConfigOption<Boolean> ENABLE_SHARE_GROUP_METRICS = ConfigOptions
            .key("enable-share-group-metrics")
            .booleanType()
            .defaultValue(false)
            .withDescription("Enable share group specific metrics collection.");
            
    // Kafka connection options (reuse from standard Kafka connector)
    public static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions
            .key("bootstrap.servers")
            .stringType()
            .noDefaultValue()
            .withDescription("Kafka bootstrap servers.");
            
    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("Kafka topic to consume from.");
            
    public static final ConfigOption<String> GROUP_TYPE = ConfigOptions
            .key("group.type")
            .stringType()
            .defaultValue("share")
            .withDescription("Consumer group type. Must be 'share' for share groups.");
            
    public static final ConfigOption<String> ENABLE_AUTO_COMMIT = ConfigOptions
            .key("enable.auto.commit")
            .stringType()
            .defaultValue("false")
            .withDescription("Enable auto commit (should be false for share groups).");
            
    public static final ConfigOption<Duration> SESSION_TIMEOUT = ConfigOptions
            .key("session.timeout.ms")
            .durationType()
            .defaultValue(Duration.ofMillis(45000))
            .withDescription("Session timeout for share group consumers.");
            
    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL = ConfigOptions
            .key("heartbeat.interval.ms")
            .durationType()
            .defaultValue(Duration.ofMillis(15000))
            .withDescription("Heartbeat interval for share group consumers.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(BOOTSTRAP_SERVERS);
        requiredOptions.add(SHARE_GROUP_ID);
        requiredOptions.add(TOPIC);
        requiredOptions.add(FactoryUtil.FORMAT); // Format is required (e.g., 'json', 'raw', etc.)
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(ENABLE_SHARE_GROUP_METRICS);
        optionalOptions.add(SOURCE_PARALLELISM);
        optionalOptions.add(GROUP_TYPE);
        optionalOptions.add(ENABLE_AUTO_COMMIT);
        optionalOptions.add(SESSION_TIMEOUT);
        optionalOptions.add(HEARTBEAT_INTERVAL);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        
        // Validate options
        helper.validate();
        
        ReadableConfig config = helper.getOptions();
        
        // Validate share group specific requirements
        validateShareGroupConfig(config);
        
        // Get format for deserialization
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = 
            helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        
        // Build properties for KafkaShareGroupSource
        Properties properties = buildKafkaProperties(config);
        
        // Create the table source
        return new KafkaShareGroupDynamicTableSource(
            context.getPhysicalRowDataType(),
            decodingFormat,
            config.get(BOOTSTRAP_SERVERS),
            config.get(SHARE_GROUP_ID),
            config.get(TOPIC),
            properties,
            config.get(ENABLE_SHARE_GROUP_METRICS),
            config.getOptional(SOURCE_PARALLELISM).orElse(null)
        );
    }
    
    private void validateShareGroupConfig(ReadableConfig config) {
        // Validate share group ID
        String shareGroupId = config.get(SHARE_GROUP_ID);
        if (shareGroupId == null || shareGroupId.trim().isEmpty()) {
            throw new ValidationException("Share group ID ('share-group-id') must be specified and non-empty.");
        }
        
        // Validate group type is 'share'
        String groupType = config.get(GROUP_TYPE);
        if (!"share".equals(groupType)) {
            throw new ValidationException("Group type ('group.type') must be 'share' for share group sources. Got: " + groupType);
        }
        
        // Note: Share groups do not use enable.auto.commit, session.timeout.ms, heartbeat.interval.ms
        // These are handled automatically by the share group protocol
    }
    
    private Properties buildKafkaProperties(ReadableConfig config) {
        Properties properties = new Properties();
        
        // Core Kafka properties for share groups
        properties.setProperty("bootstrap.servers", config.get(BOOTSTRAP_SERVERS));
        properties.setProperty("group.type", config.get(GROUP_TYPE));
        properties.setProperty("group.id", config.get(SHARE_GROUP_ID));
        
        // Client ID for SQL source
        properties.setProperty("client.id", config.get(SHARE_GROUP_ID) + "-sql-consumer");
        
        // NOTE: Share groups do not support these properties that regular consumers use:
        // - enable.auto.commit (share groups handle acknowledgment differently)  
        // - auto.offset.reset (not applicable to share groups)
        // - session.timeout.ms (share groups use different timeout semantics)
        // - heartbeat.interval.ms (share groups use different heartbeat semantics)
        
        return properties;
    }

    /**
     * Kafka Share Group Dynamic Table Source implementation.
     */
    public static class KafkaShareGroupDynamicTableSource implements ScanTableSource {
        
        private final DataType physicalDataType;
        private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
        private final String bootstrapServers;
        private final String shareGroupId;
        private final String topic;
        private final Properties kafkaProperties;
        private final boolean enableMetrics;
        private final Integer parallelism;
        
        public KafkaShareGroupDynamicTableSource(
                DataType physicalDataType,
                DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                String bootstrapServers,
                String shareGroupId,
                String topic,
                Properties kafkaProperties,
                boolean enableMetrics,
                Integer parallelism) {
            this.physicalDataType = physicalDataType;
            this.decodingFormat = decodingFormat;
            this.bootstrapServers = bootstrapServers;
            this.shareGroupId = shareGroupId;
            this.topic = topic;
            this.kafkaProperties = kafkaProperties;
            this.enableMetrics = enableMetrics;
            this.parallelism = parallelism;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            // Share groups provide insert-only semantics (like a queue)
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
            // Create deserialization schema
            DeserializationSchema<RowData> deserializationSchema = decodingFormat.createRuntimeDecoder(
                context, physicalDataType);
                
            // Create KafkaShareGroupSource
            KafkaShareGroupSource<RowData> shareGroupSource = KafkaShareGroupSource.<RowData>builder()
                .setBootstrapServers(bootstrapServers)
                .setShareGroupId(shareGroupId)
                .setTopics(topic)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(deserializationSchema))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(kafkaProperties)
                .enableShareGroupMetrics(enableMetrics)
                .build();
            
            // Create SourceProvider with custom parallelism if specified
            if (parallelism != null) {
                return SourceProvider.of(shareGroupSource, parallelism);
            } else {
                return SourceProvider.of(shareGroupSource);
            }
        }

        @Override
        public DynamicTableSource copy() {
            return new KafkaShareGroupDynamicTableSource(
                physicalDataType,
                decodingFormat,
                bootstrapServers,
                shareGroupId,
                topic,
                kafkaProperties,
                enableMetrics,
                parallelism
            );
        }

        @Override
        public String asSummaryString() {
            return String.format("KafkaShareGroup(shareGroupId=%s, topic=%s, servers=%s)", 
                shareGroupId, topic, bootstrapServers);
        }
    }
}