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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.SingleClusterTopicMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanBoundedMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaConnectorOptions.METADATA_SERVICE;
import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaConnectorOptions.METADATA_SERVICE_CLUSTER_ID;
import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaConnectorOptions.STREAM_IDS;
import static org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaConnectorOptions.STREAM_PATTERN;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_BOUNDED_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_PARALLELISM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FIELDS_INCLUDE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createKeyFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.createValueFormatProjection;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getKafkaProperties;

/** Factory for creating configured instances of {@link DynamicKafkaTableSource}. */
@Internal
public class DynamicKafkaTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "dynamic-kafka";
    private static final String METADATA_SERVICE_SINGLE_CLUSTER = "single-cluster";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(METADATA_SERVICE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FORMAT);
        options.add(VALUE_FIELDS_INCLUDE);
        options.add(STREAM_IDS);
        options.add(STREAM_PATTERN);
        options.add(METADATA_SERVICE_CLUSTER_ID);
        options.add(PROPS_GROUP_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_BOUNDED_MODE);
        options.add(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
        options.add(SCAN_PARALLELISM);
        options.add(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS);
        options.add(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD);
        options.add(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        validateTableSourceOptions(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueDecodingFormat);

        final StartupOptions startupOptions = getStartupOptions(tableOptions);
        final BoundedOptions boundedOptions = getBoundedOptions(tableOptions);

        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        // add topic-partition discovery
        final Duration partitionDiscoveryInterval =
                tableOptions.get(SCAN_TOPIC_PARTITION_DISCOVERY);
        properties.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                Long.toString(partitionDiscoveryInterval.toMillis()));

        applyDynamicDiscoveryOptions(tableOptions, properties);

        final KafkaMetadataService kafkaMetadataService =
                createMetadataService(tableOptions, properties, context.getClassLoader());

        final DataType physicalDataType = context.getPhysicalRowDataType();

        final int[] keyProjection = createKeyFormatProjection(tableOptions, physicalDataType);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        final String keyPrefix = tableOptions.getOptional(KEY_FIELDS_PREFIX).orElse(null);

        final Integer parallelism = tableOptions.getOptional(SCAN_PARALLELISM).orElse(null);

        return new DynamicKafkaTableSource(
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                getStreamIds(tableOptions),
                getStreamPattern(tableOptions),
                kafkaMetadataService,
                properties,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis,
                boundedOptions.boundedMode,
                boundedOptions.specificOffsets,
                boundedOptions.boundedTimestampMillis,
                false,
                context.getObjectIdentifier().asSummaryString(),
                parallelism);
    }

    private static void applyDynamicDiscoveryOptions(
            ReadableConfig tableOptions, Properties properties) {
        tableOptions
                .getOptional(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS)
                .ifPresent(
                        value ->
                                properties.setProperty(
                                        DynamicKafkaSourceOptions
                                                .STREAM_METADATA_DISCOVERY_INTERVAL_MS
                                                .key(),
                                        Long.toString(value)));
        tableOptions
                .getOptional(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD)
                .ifPresent(
                        value ->
                                properties.setProperty(
                                        DynamicKafkaSourceOptions
                                                .STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD
                                                .key(),
                                        Integer.toString(value)));
        tableOptions
                .getOptional(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE)
                .ifPresent(
                        value ->
                                properties.setProperty(
                                        DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                                        value));
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()));
                    }
                });
        return keyDecodingFormat;
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName,
            int[] primaryKeyIndexes,
            Map<String, String> options,
            Format format) {
        if (primaryKeyIndexes.length > 0
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration configuration = Configuration.fromMap(options);
            String formatName =
                    configuration
                            .getOptional(FactoryUtil.FORMAT)
                            .orElse(configuration.get(VALUE_FORMAT));
            throw new ValidationException(
                    String.format(
                            "The Dynamic Kafka table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                                    + " on the table, because it can't guarantee the semantic of primary key.",
                            tableName.asSummaryString(), formatName));
        }
    }

    private static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateStreams(tableOptions);
        validateMetadataService(tableOptions);
        validateScanStartupMode(tableOptions);
        validateScanBoundedMode(tableOptions);
    }

    private static void validateStreams(ReadableConfig tableOptions) {
        Optional<List<String>> streamIds = tableOptions.getOptional(STREAM_IDS);
        Optional<String> pattern = tableOptions.getOptional(STREAM_PATTERN);

        if (streamIds.isPresent() && pattern.isPresent()) {
            throw new ValidationException(
                    "Option 'stream-ids' and 'stream-pattern' shouldn't be set together.");
        }

        if (!streamIds.isPresent() && !pattern.isPresent()) {
            throw new ValidationException("Either 'stream-ids' or 'stream-pattern' must be set.");
        }

        streamIds.ifPresent(
                ids -> {
                    if (ids.isEmpty()) {
                        throw new ValidationException("Option 'stream-ids' cannot be empty.");
                    }
                });

        pattern.ifPresent(
                value -> {
                    try {
                        Pattern.compile(value);
                    } catch (PatternSyntaxException e) {
                        throw new ValidationException(
                                "Option 'stream-pattern' contains an invalid regular expression.",
                                e);
                    }
                });
    }

    private static void validateMetadataService(ReadableConfig tableOptions) {
        Optional<String> metadataService = tableOptions.getOptional(METADATA_SERVICE);
        if (!metadataService.isPresent()) {
            throw new ValidationException("Option 'metadata-service' must be set.");
        }
        if (isSingleClusterMetadataService(metadataService.get())) {
            if (!tableOptions.getOptional(METADATA_SERVICE_CLUSTER_ID).isPresent()) {
                throw new ValidationException(
                        "Option 'metadata-service.cluster-id' is required for 'single-cluster' metadata service.");
            }
            if (!tableOptions.getOptional(PROPS_BOOTSTRAP_SERVERS).isPresent()) {
                throw new ValidationException(
                        "Option 'properties.bootstrap.servers' is required for 'single-cluster' metadata service.");
            }
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                        ScanStartupMode.TIMESTAMP));
                                    }
                                    break;
                                case SPECIFIC_OFFSETS:
                                    throw new ValidationException(
                                            "Dynamic Kafka source does not support 'specific-offsets' startup mode.");
                            }
                        });
    }

    private static void validateScanBoundedMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_BOUNDED_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(SCAN_BOUNDED_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' bounded mode"
                                                                + " but missing.",
                                                        SCAN_BOUNDED_TIMESTAMP_MILLIS.key(),
                                                        ScanBoundedMode.TIMESTAMP));
                                    }
                                    break;
                                case SPECIFIC_OFFSETS:
                                    throw new ValidationException(
                                            "Dynamic Kafka source does not support 'specific-offsets' bounded mode.");
                            }
                        });
    }

    private static List<String> getStreamIds(ReadableConfig tableOptions) {
        return tableOptions.getOptional(STREAM_IDS).orElse(null);
    }

    private static Pattern getStreamPattern(ReadableConfig tableOptions) {
        return tableOptions.getOptional(STREAM_PATTERN).map(Pattern::compile).orElse(null);
    }

    private static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final StartupOptions options = new StartupOptions();
        final StartupMode startupMode =
                tableOptions
                        .getOptional(SCAN_STARTUP_MODE)
                        .map(DynamicKafkaTableFactory::toStartupMode)
                        .orElse(StartupMode.GROUP_OFFSETS);
        options.startupMode = startupMode;
        options.specificOffsets = Collections.emptyMap();
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static BoundedOptions getBoundedOptions(ReadableConfig tableOptions) {
        final BoundedOptions options = new BoundedOptions();
        final BoundedMode boundedMode = toBoundedMode(tableOptions.get(SCAN_BOUNDED_MODE));
        options.boundedMode = boundedMode;
        options.specificOffsets = Collections.emptyMap();
        if (boundedMode == BoundedMode.TIMESTAMP) {
            options.boundedTimestampMillis = tableOptions.get(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static StartupMode toStartupMode(ScanStartupMode scanStartupMode) {
        switch (scanStartupMode) {
            case EARLIEST_OFFSET:
                return StartupMode.EARLIEST;
            case LATEST_OFFSET:
                return StartupMode.LATEST;
            case GROUP_OFFSETS:
                return StartupMode.GROUP_OFFSETS;
            case SPECIFIC_OFFSETS:
                return StartupMode.SPECIFIC_OFFSETS;
            case TIMESTAMP:
                return StartupMode.TIMESTAMP;
            default:
                throw new ValidationException(
                        "Unsupported startup mode. Validator should have checked that.");
        }
    }

    private static BoundedMode toBoundedMode(ScanBoundedMode scanBoundedMode) {
        switch (scanBoundedMode) {
            case UNBOUNDED:
                return BoundedMode.UNBOUNDED;
            case LATEST_OFFSET:
                return BoundedMode.LATEST;
            case GROUP_OFFSETS:
                return BoundedMode.GROUP_OFFSETS;
            case TIMESTAMP:
                return BoundedMode.TIMESTAMP;
            case SPECIFIC_OFFSETS:
                return BoundedMode.SPECIFIC_OFFSETS;
            default:
                throw new ValidationException(
                        "Unsupported bounded mode. Validator should have checked that.");
        }
    }

    private static KafkaMetadataService createMetadataService(
            ReadableConfig tableOptions, Properties properties, ClassLoader classLoader) {
        String metadataService = tableOptions.get(METADATA_SERVICE);
        if (isSingleClusterMetadataService(metadataService)) {
            String clusterId = tableOptions.get(METADATA_SERVICE_CLUSTER_ID);
            return new SingleClusterTopicMetadataService(clusterId, properties);
        }

        try {
            Class<?> clazz = Class.forName(metadataService, true, classLoader);
            if (!KafkaMetadataService.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Metadata service class '%s' should implement %s",
                                metadataService, KafkaMetadataService.class.getName()));
            }
            KafkaMetadataService withProperties = instantiateWithProperties(clazz, properties);
            if (withProperties != null) {
                return withProperties;
            }
            return InstantiationUtil.instantiate(
                    metadataService, KafkaMetadataService.class, classLoader);
        } catch (ValidationException e) {
            throw e;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format(
                            "Could not find and instantiate metadata service class '%s'",
                            metadataService),
                    e);
        } catch (ReflectiveOperationException e) {
            throw new ValidationException(
                    String.format(
                            "Could not instantiate metadata service class '%s' with properties.",
                            metadataService),
                    e);
        }
    }

    private static @Nullable KafkaMetadataService instantiateWithProperties(
            Class<?> clazz, Properties properties) throws ReflectiveOperationException {
        try {
            Constructor<?> constructor = clazz.getConstructor(Properties.class);
            return (KafkaMetadataService) constructor.newInstance(properties);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static boolean isSingleClusterMetadataService(String metadataService) {
        return METADATA_SERVICE_SINGLE_CLUSTER.equals(metadataService)
                || SingleClusterTopicMetadataService.class.getName().equals(metadataService);
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    private static class StartupOptions {
        private StartupMode startupMode;
        private Map<org.apache.kafka.common.TopicPartition, Long> specificOffsets;
        private long startupTimestampMillis;
    }

    private static class BoundedOptions {
        private BoundedMode boundedMode;
        private Map<org.apache.kafka.common.TopicPartition, Long> specificOffsets;
        private long boundedTimestampMillis;
    }
}
