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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** SerializationSchema used by {@link KafkaDynamicSink} to configure a {@link KafkaSink}. */
class DynamicKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<RowData> {

    private final String topic;
    private final FlinkKafkaPartitioner<RowData> partitioner;
    @Nullable private final SerializationSchema<RowData> keySerialization;
    private final SerializationSchema<RowData> valueSerialization;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final RowData.FieldGetter[] valueFieldGetters;
    private final boolean hasMetadata;
    private final int[] metadataPositions;
    private final boolean upsertMode;
    private static final String TOPIC_NAME = "TOPIC_NAME";
    protected static final String IS_KEY = "IS_KEY";

    protected static final String HEADERS = "HEADERS";

    private static Method serializeWithAdditionalPropertiesMethod = null;

    static {
        Class<SerializationSchema> serializationSchemaClass = SerializationSchema.class;
        try {
            serializeWithAdditionalPropertiesMethod =
                    serializationSchemaClass.getDeclaredMethod(
                            "serializeWithAdditionalProperties",
                            Object.class,
                            Map.class,
                            Map.class);

        } catch (NoSuchMethodException e) {
            // do nothing
        }
    }

    DynamicKafkaRecordSerializationSchema(
            String topic,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode) {
        if (upsertMode) {
            Preconditions.checkArgument(
                    keySerialization != null && keyFieldGetters.length > 0,
                    "Key must be set in upsert mode for serialization schema.");
        }
        this.topic = checkNotNull(topic);
        this.partitioner = partitioner;
        this.keySerialization = keySerialization;
        this.valueSerialization = checkNotNull(valueSerialization);
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.upsertMode = upsertMode;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData consumedRow, KafkaSinkContext context, Long timestamp) {

        // keeping the metadataHeaders maps for keys and values separate so these maps
        // are output only for the methods; avoiding changing the inputs.
        // define the input map
        Map<String, Object> inputMap = new HashMap<>();

        // define the output maps, it is 2 levels deep in case we need to add new information in
        // the future
        Map<String, Object> outputKeyMap = new HashMap<>();
        Map<String, Object> outputValueMap = new HashMap<>();

        inputMap.put(TOPIC_NAME, topic);

        // shortcut in case no input projection is required
        if (keySerialization == null && !hasMetadata) {
            byte[] valueSerialized =  getSerialized(consumedRow, inputMap, outputValueMap, false);
            if (serializeWithAdditionalPropertiesMethod == null) {
                return new ProducerRecord<>(
                        topic,
                        extractPartition(
                                consumedRow,
                                null,
                                valueSerialized,
                                context.getPartitionsForTopic(topic)),
                        null,
                        valueSerialized);
            } else {
                List<Header> headers = new ArrayList<>();
                Map<String, Object> headersToAddForValue =
                            (Map<String, Object>) outputValueMap.get(HEADERS);
                if (!MapUtils.isEmpty(headersToAddForValue)) {
                    for (String headerKey : headersToAddForValue.keySet()) {
                        KafkaDynamicSink.KafkaHeader kafkaHeader =
                                new KafkaDynamicSink.KafkaHeader(
                                        headerKey, (byte[]) headersToAddForValue.get(headerKey));
                        headers.add(kafkaHeader);
                    }
                }
                return new ProducerRecord<>(
                        topic,
                        extractPartition(
                                consumedRow,
                                null,
                                valueSerialized,
                                context.getPartitionsForTopic(topic)),
                        null,    // timestamp will be current time
                        null,
                        valueSerialized,
                        headers);
            }
        }
        final byte[] keySerialized;
        if (keySerialization == null) {
            keySerialized = null;
        } else {
            final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            keySerialized = getSerialized(keyRow, inputMap, outputKeyMap, true);
        }

        final byte[] valueSerialized;
        final RowKind kind = consumedRow.getRowKind();
        if (upsertMode) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                // transform the message as the tombstone message
                valueSerialized = null;
            } else {
                // make the message to be INSERT to be compliant with the INSERT-ONLY format
                final RowData valueRow =
                        DynamicKafkaRecordSerializationSchema.createProjectedRow(
                                consumedRow, kind, valueFieldGetters);
                valueRow.setRowKind(RowKind.INSERT);
                valueSerialized = getSerialized(valueRow, inputMap, outputValueMap, false);
            }
        } else {
            final RowData valueRow =
                    DynamicKafkaRecordSerializationSchema.createProjectedRow(
                            consumedRow, kind, valueFieldGetters);
            valueSerialized = getSerialized(valueRow, inputMap, outputValueMap, false);
        }
        List<Header> headers;
        if (serializeWithAdditionalPropertiesMethod != null) {
            // metadataHeaders representing metadata columns
            List<Header> metadataHeaders =
                    readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS);
            // merge any additional Headers to be added from the serialize, using a map to avoid
            // duplicate headers
            // in the List
            Map<String, Object> headersToAddForKey =
                    (Map<String, Object>) outputKeyMap.get(HEADERS);
            Map<String, Object> headersToAddForValue =
                    (Map<String, Object>) outputValueMap.get(HEADERS);
            Map<String, Header> headersMap = new HashMap<>();
            // add the metadata headers

            if (metadataHeaders != null && !metadataHeaders.isEmpty()) {
                for (Header header : metadataHeaders) {
                    headersMap.put(header.key(), header);
                }
            }
            // add the key headers
            if (!MapUtils.isEmpty(headersToAddForKey)) {
                for (String headerKey : headersToAddForKey.keySet()) {
                    KafkaDynamicSink.KafkaHeader kafkaHeader =
                            new KafkaDynamicSink.KafkaHeader(
                                    headerKey, (byte[]) headersToAddForKey.get(headerKey));
                    headersMap.put(headerKey, kafkaHeader);
                }
            }
            // add the value headers
            if (!MapUtils.isEmpty(headersToAddForValue)) {
                for (String headerKey : headersToAddForValue.keySet()) {
                    KafkaDynamicSink.KafkaHeader kafkaHeader =
                            new KafkaDynamicSink.KafkaHeader(
                                    headerKey, (byte[]) headersToAddForValue.get(headerKey));
                    headersMap.put(headerKey, kafkaHeader);
                }
            }

            // convert map to list
            headers = new ArrayList<>();
            headersMap.entrySet().forEach(entry -> headers.add(entry.getValue()));
        } else {
            headers = readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS);
        }
        return new ProducerRecord<>(
                topic,
                extractPartition(
                        consumedRow,
                        keySerialized,
                        valueSerialized,
                        context.getPartitionsForTopic(topic)),
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
                keySerialized,
                valueSerialized,
                headers);
    }

    /**
     * Get the serialized row, depending on whether Flink has the additional parameters serialize method.
     * @param row row to serialize
     * @param inputMap input map to pass to the additional parameters serialize method
     * @param outputMap output map to pass to the additional parameters serialize method
     * @param isKey whether this is a key
     * @return serialized row
     */
    private byte[] getSerialized(RowData row, Map<String, Object> inputMap, Map<String, Object> outputMap, boolean isKey) {
        final byte[] keySerialized;
        SerializationSchema<RowData> serialization = isKey ? keySerialization : valueSerialization;
        if (serializeWithAdditionalPropertiesMethod == null) {
            keySerialized = serialization.serialize(row);
        } else {
            inputMap.put(IS_KEY, isKey);
            try {
                keySerialized =
                        (byte[])
                                serializeWithAdditionalPropertiesMethod.invoke(
                                        serialization, row, inputMap, outputMap);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        return keySerialized;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
        valueSerialization.open(context);
    }

    private Integer extractPartition(
            RowData consumedRow,
            @Nullable byte[] keySerialized,
            byte[] valueSerialized,
            int[] partitions) {
        if (partitioner != null) {
            return partitioner.partition(
                    consumedRow, keySerialized, valueSerialized, topic, partitions);
        }
        return null;
    }

    static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(RowData consumedRow, KafkaDynamicSink.WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }
}
