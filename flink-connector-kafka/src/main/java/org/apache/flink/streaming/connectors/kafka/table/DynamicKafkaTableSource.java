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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaDeserializationSchema.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A {@link ScanTableSource} for {@link DynamicKafkaSource}. */
@Internal
public class DynamicKafkaTableSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {

    private static final String KAFKA_TRANSFORMATION = "kafka";

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    /** Value format metadata keys. */
    protected List<String> valueFormatMetadataKeys;

    /** Indices that determine the physical fields and their positions in the produced row. */
    protected int[][] projectedPhysicalFields;

    /** Watermark strategy that is used to generate per-partition watermark. */
    protected @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String VALUE_METADATA_PREFIX = "value.";

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Optional format for decoding keys from Kafka. */
    protected final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    /** Format for decoding values from Kafka. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the key fields and the target position in the produced row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    protected final @Nullable String keyPrefix;

    // --------------------------------------------------------------------------------------------
    // Dynamic Kafka specific attributes
    // --------------------------------------------------------------------------------------------

    /** The stream ids to consume. */
    protected final @Nullable List<String> streamIds;

    /** The stream pattern to consume. */
    protected final @Nullable Pattern streamPattern;

    /** Metadata service for resolving streams to topics and clusters. */
    protected final KafkaMetadataService kafkaMetadataService;

    /** Properties for the Kafka consumer. */
    protected final Properties properties;

    /**
     * The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}).
     */
    protected final StartupMode startupMode;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link
     * StartupMode#SPECIFIC_OFFSETS}.
     */
    protected final Map<TopicPartition, Long> specificStartupOffsets;

    /**
     * The start timestamp to locate partition offsets; only relevant when startup mode is {@link
     * StartupMode#TIMESTAMP}.
     */
    protected final long startupTimestampMillis;

    /** The bounded mode for the contained consumer (default is an unbounded data stream). */
    protected final BoundedMode boundedMode;

    /**
     * Specific end offsets; only relevant when bounded mode is {@link
     * BoundedMode#SPECIFIC_OFFSETS}.
     */
    protected final Map<TopicPartition, Long> specificBoundedOffsets;

    /**
     * The bounded timestamp to locate partition offsets; only relevant when bounded mode is {@link
     * BoundedMode#TIMESTAMP}.
     */
    protected final long boundedTimestampMillis;

    /** Flag to determine source mode. In upsert mode, it will keep the tombstone message. * */
    protected final boolean upsertMode;

    protected final String tableIdentifier;

    /** Parallelism of the physical Kafka consumer. * */
    protected final @Nullable Integer parallelism;

    public DynamicKafkaTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            @Nullable List<String> streamIds,
            @Nullable Pattern streamPattern,
            KafkaMetadataService kafkaMetadataService,
            Properties properties,
            StartupMode startupMode,
            Map<TopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis,
            BoundedMode boundedMode,
            Map<TopicPartition, Long> specificBoundedOffsets,
            long boundedTimestampMillis,
            boolean upsertMode,
            String tableIdentifier,
            @Nullable Integer parallelism) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");
        this.keyProjection =
                Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;
        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = Collections.emptyList();
        this.valueFormatMetadataKeys = Collections.emptyList();
        this.projectedPhysicalFields =
                Decoder.identityProjection((RowType) physicalDataType.getLogicalType());
        this.watermarkStrategy = null;
        // Dynamic Kafka specific attributes
        Preconditions.checkArgument(
                (streamIds != null && streamPattern == null)
                        || (streamIds == null && streamPattern != null),
                "Either stream ids or stream pattern must be set for source.");
        this.streamIds = streamIds;
        this.streamPattern = streamPattern;
        this.kafkaMetadataService =
                Preconditions.checkNotNull(
                        kafkaMetadataService, "Kafka metadata service must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.startupMode =
                Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
        this.specificStartupOffsets =
                Preconditions.checkNotNull(
                        specificStartupOffsets, "Specific offsets must not be null.");
        this.startupTimestampMillis = startupTimestampMillis;
        this.boundedMode =
                Preconditions.checkNotNull(boundedMode, "Bounded mode must not be null.");
        this.specificBoundedOffsets =
                Preconditions.checkNotNull(
                        specificBoundedOffsets, "Specific bounded offsets must not be null.");
        this.boundedTimestampMillis = boundedTimestampMillis;
        this.upsertMode = upsertMode;
        this.tableIdentifier = tableIdentifier;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final Decoder keyDecoder =
                Decoder.create(
                        context,
                        keyDecodingFormat,
                        physicalDataType,
                        keyProjection,
                        keyPrefix,
                        projectedPhysicalFields,
                        Collections.emptyList(),
                        false);

        final Decoder valueDecoder =
                Decoder.create(
                        context,
                        valueDecodingFormat,
                        physicalDataType,
                        valueProjection,
                        null,
                        projectedPhysicalFields,
                        valueFormatMetadataKeys,
                        false);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final DynamicKafkaSource<RowData> kafkaSource =
                createDynamicKafkaSource(keyDecoder, valueDecoder, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                    watermarkStrategy = WatermarkStrategy.noWatermarks();
                }
                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                kafkaSource,
                                watermarkStrategy,
                                "DynamicKafkaSource-" + tableIdentifier);
                providerContext.generateUid(KAFKA_TRANSFORMATION).ifPresent(sourceStream::uid);
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return kafkaSource.getBoundedness() == Boundedness.BOUNDED;
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.ofNullable(parallelism);
            }
        };
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        valueDecodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.valueFormatMetadataKeys = new ArrayList<>();
        this.metadataKeys = new ArrayList<>();
        for (final String key : metadataKeys) {
            if (key.startsWith(VALUE_METADATA_PREFIX)) {
                final String formatMetadataKey = key.substring(VALUE_METADATA_PREFIX.length());
                this.valueFormatMetadataKeys.add(formatMetadataKey);
            } else {
                this.metadataKeys.add(key);
            }
        }
        this.producedDataType = producedDataType;
    }

    @Override
    public boolean supportsMetadataProjection() {
        return false;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public DynamicTableSource copy() {
        final DynamicKafkaTableSource copy =
                new DynamicKafkaTableSource(
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        streamIds,
                        streamPattern,
                        kafkaMetadataService,
                        properties,
                        startupMode,
                        specificStartupOffsets,
                        startupTimestampMillis,
                        boundedMode,
                        specificBoundedOffsets,
                        boundedTimestampMillis,
                        upsertMode,
                        tableIdentifier,
                        parallelism);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.valueFormatMetadataKeys = valueFormatMetadataKeys;
        copy.projectedPhysicalFields = projectedPhysicalFields;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Dynamic Kafka table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DynamicKafkaTableSource that = (DynamicKafkaTableSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(valueFormatMetadataKeys, that.valueFormatMetadataKeys)
                && Arrays.deepEquals(projectedPhysicalFields, that.projectedPhysicalFields)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(streamIds, that.streamIds)
                && Objects.equals(String.valueOf(streamPattern), String.valueOf(that.streamPattern))
                && Objects.equals(kafkaMetadataService, that.kafkaMetadataService)
                && Objects.equals(properties, that.properties)
                && startupMode == that.startupMode
                && Objects.equals(specificStartupOffsets, that.specificStartupOffsets)
                && startupTimestampMillis == that.startupTimestampMillis
                && boundedMode == that.boundedMode
                && Objects.equals(specificBoundedOffsets, that.specificBoundedOffsets)
                && boundedTimestampMillis == that.boundedTimestampMillis
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                metadataKeys,
                valueFormatMetadataKeys,
                Arrays.deepHashCode(projectedPhysicalFields),
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                Arrays.hashCode(keyProjection),
                Arrays.hashCode(valueProjection),
                keyPrefix,
                streamIds,
                streamPattern,
                kafkaMetadataService,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                boundedMode,
                specificBoundedOffsets,
                boundedTimestampMillis,
                upsertMode,
                tableIdentifier,
                watermarkStrategy,
                parallelism);
    }

    // --------------------------------------------------------------------------------------------

    protected DynamicKafkaSource<RowData> createDynamicKafkaSource(
            Decoder keyDecoder, Decoder valueDecoder, TypeInformation<RowData> producedTypeInfo) {

        final KafkaRecordDeserializationSchema<RowData> kafkaDeserializer =
                createKafkaDeserializationSchema(keyDecoder, valueDecoder, producedTypeInfo);

        final DynamicKafkaSourceBuilder<RowData> dynamicKafkaSourceBuilder =
                DynamicKafkaSource.builder();

        if (streamIds != null) {
            dynamicKafkaSourceBuilder.setStreamIds(new HashSet<>(streamIds));
        } else {
            dynamicKafkaSourceBuilder.setStreamPattern(streamPattern);
        }

        dynamicKafkaSourceBuilder
                .setKafkaMetadataService(kafkaMetadataService)
                .setDeserializer(kafkaDeserializer)
                .setProperties(properties);

        switch (startupMode) {
            case EARLIEST:
                dynamicKafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case LATEST:
                dynamicKafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        properties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                OffsetResetStrategy.NONE.name());
                OffsetResetStrategy offsetResetStrategy = getResetStrategy(offsetResetConfig);
                dynamicKafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.committedOffsets(offsetResetStrategy));
                break;
            case SPECIFIC_OFFSETS:
                dynamicKafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.offsets(specificStartupOffsets));
                break;
            case TIMESTAMP:
                dynamicKafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.timestamp(startupTimestampMillis));
                break;
        }

        switch (boundedMode) {
            case UNBOUNDED:
                break;
            case LATEST:
                dynamicKafkaSourceBuilder.setBounded(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                dynamicKafkaSourceBuilder.setBounded(OffsetsInitializer.committedOffsets());
                break;
            case SPECIFIC_OFFSETS:
                dynamicKafkaSourceBuilder.setBounded(
                        OffsetsInitializer.offsets(specificBoundedOffsets));
                break;
            case TIMESTAMP:
                dynamicKafkaSourceBuilder.setBounded(
                        OffsetsInitializer.timestamp(boundedTimestampMillis));
                break;
        }

        return dynamicKafkaSourceBuilder.build();
    }

    private OffsetResetStrategy getResetStrategy(String offsetResetConfig) {
        return Arrays.stream(OffsetResetStrategy.values())
                .filter(ors -> ors.name().equals(offsetResetConfig.toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "%s can not be set to %s. Valid values: [%s]",
                                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                offsetResetConfig,
                                                Arrays.stream(OffsetResetStrategy.values())
                                                        .map(Enum::name)
                                                        .map(String::toLowerCase)
                                                        .collect(Collectors.joining(",")))));
    }

    private KafkaRecordDeserializationSchema<RowData> createKafkaDeserializationSchema(
            Decoder keyDecoder, Decoder valueDecoder, TypeInformation<RowData> producedTypeInfo) {
        final MetadataConverter[] metadataConverters = new MetadataConverter[metadataKeys.size()];
        final boolean[] clusterMetadataPositions = new boolean[metadataKeys.size()];
        boolean hasClusterMetadata = false;
        for (int i = 0; i < metadataKeys.size(); i++) {
            String key = metadataKeys.get(i);
            ReadableMetadata metadata =
                    Stream.of(ReadableMetadata.values())
                            .filter(rm -> rm.key.equals(key))
                            .findFirst()
                            .orElseThrow(IllegalStateException::new);
            metadataConverters[i] = metadata.converter;
            if (metadata == ReadableMetadata.KAFKA_CLUSTER) {
                clusterMetadataPositions[i] = true;
                hasClusterMetadata = true;
            }
        }
        final boolean[] clusterPositions = hasClusterMetadata ? clusterMetadataPositions : null;

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                DataType.getFieldDataTypes(producedDataType).size() - metadataKeys.size();

        return new DynamicKafkaDeserializationSchema(
                adjustedPhysicalArity,
                keyDecoder.getDeserializationSchema(),
                keyDecoder.getProjector(),
                valueDecoder.getDeserializationSchema(),
                valueDecoder.getProjector(),
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                upsertMode,
                clusterPositions);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        KAFKA_CLUSTER(
                "kafka_cluster",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        throw new IllegalStateException(
                                "Kafka cluster metadata should be populated by dynamic source.");
                    }
                }),

        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.topic());
                    }
                }),

        PARTITION(
                "partition",
                DataTypes.INT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.partition();
                    }
                }),

        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        final Map<StringData, byte[]> map = new HashMap<>();
                        for (Header header : record.headers()) {
                            map.put(StringData.fromString(header.key()), header.value());
                        }
                        return new GenericMapData(map);
                    }
                }),

        LEADER_EPOCH(
                "leader-epoch",
                DataTypes.INT().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.leaderEpoch().orElse(null);
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.offset();
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return TimestampData.fromEpochMillis(record.timestamp());
                    }
                }),

        TIMESTAMP_TYPE(
                "timestamp-type",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.timestampType().toString());
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
