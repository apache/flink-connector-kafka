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
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource.Context;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Basically just a container class for two objects that work together to decode and project data to
 * the final produced RowData.
 */
@Internal
public class ProjectionHandler {

    /**
     * Can be null. Null is used inside {@link DynamicKafkaDeserializationSchema} to avoid
     * deserializing keys if not required.
     */
    private final @Nullable DeserializationSchema<RowData> deserializationSchema;

    /** Mapping of the physical position in the key to the target position in the RowData. */
    private final Map<Integer, Integer> dataTypePosToRowDataPos;

    private ProjectionHandler(
            final DeserializationSchema<RowData> deserializationSchema,
            final Map<Integer, Integer> dataTypePosToRowDataPos) {
        this.deserializationSchema = deserializationSchema;
        this.dataTypePosToRowDataPos = Map.copyOf(dataTypePosToRowDataPos);
    }

    /**
     * @param decodingFormat Optional format for decoding bytes.
     * @param tableDataType The data type representing the table schema.
     * @param tableProjection Indices indicate the position in the key/value fields Values indicate
     *     the position in the tableSchema
     * @param prefix Optional
     * @param projectedFields Optional
     * @param projectionPushdownIntoDecoderDisabled if true, any {@param projectedFields} will not
     *     be pushed down into the (projectable) decoder. If false, any {@param projectedFields}
     *     will be pushed down into the (projectable) decoder.
     * @return a ProjectionHandler instance.
     */
    public static ProjectionHandler create(
            final Context context,
            final @Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            final DataType tableDataType,
            final int[] tableProjection,
            final @Nullable String prefix,
            final @Nullable int[][] projectedFields,
            final boolean projectionPushdownIntoDecoderDisabled) {
        if (decodingFormat == null) {
            return ProjectionHandler.empty();
        } else if (projectedFields != null) {
            if (decodingFormat instanceof ProjectableDecodingFormat
                    && !projectionPushdownIntoDecoderDisabled) {
                return ProjectionHandler.withProjectionPushdownIntoDecoder(
                        context,
                        (ProjectableDecodingFormat<DeserializationSchema<RowData>>) decodingFormat,
                        tableDataType,
                        tableProjection,
                        prefix,
                        projectedFields);
            } else {
                return ProjectionHandler.withoutProjectionPushdownIntoDecoder(
                        context,
                        decodingFormat,
                        tableDataType,
                        tableProjection,
                        prefix,
                        projectedFields);
            }
        } else {
            return ProjectionHandler.withoutProjection(
                    context, decodingFormat, tableDataType, tableProjection, prefix);
        }
    }

    /** @return a decoder or null. */
    @Nullable
    public DeserializationSchema<RowData> getDeserializationSchema() {
        return deserializationSchema;
    }

    /**
     * @return map containing an entry for each desired field mapping the position in the decoded
     *     RowData to its target position in produced RowData.
     */
    public Map<Integer, Integer> getDataTypePosToRowDataPos() {
        return dataTypePosToRowDataPos;
    }

    /**
     * The returned {@link ProjectionHandler} has the following behaviour.
     * <li>
     *
     *     <ol>
     *       {@link #getDeserializationSchema} will return null.
     * </ol>
     *
     * <ol>
     *   {@link #getDataTypePosToRowDataPos} will return an empty map.
     * </ol>
     */
    private static ProjectionHandler empty() {
        return new ProjectionHandler(null, Map.of());
    }

    private static DataType toDataType(
            final DataType tableDataType,
            final int[] tableProjection,
            final @Nullable String prefix) {
        final DataType temp = Projection.of(tableProjection).project(tableDataType);
        return Optional.ofNullable(prefix)
                .map(s -> DataTypeUtils.stripRowPrefix(temp, s))
                .orElse(temp);
    }

    /**
     * The returned {@link ProjectionHandler} has the following behaviour.
     * <li>
     *
     *     <ol>
     *       {@link #getDeserializationSchema} will decode just the required fields.
     * </ol>
     *
     * <ol>
     *   {@link #getDataTypePosToRowDataPos} will return a map containing an entry for each decoded
     *   field mapping the position in the decoded RowData to it's target position in produced
     *   RowData.
     * </ol>
     */
    private static ProjectionHandler withProjectionPushdownIntoDecoder(
            final Context context,
            final ProjectableDecodingFormat<DeserializationSchema<RowData>>
                    projectableDecodingFormat,
            final DataType tableDataType,
            final int[] tableProjection,
            final @Nullable String prefix,
            final int[][] projectedFields) {
        final HashMap<Integer, Integer> tableToDataTypePos = new HashMap<>();
        for (int i = 0; i < tableProjection.length; i++) {
            tableToDataTypePos.put(tableProjection[i], i);
        }

        // index indicates where it should go in the produced RowData
        // values at each index indicate the position of the field in the physical dataType
        final List<int[]> decoderProjectionList = new ArrayList<>();
        final Map<Integer, Integer> rowDataProjection = new HashMap<>();
        for (int rowDataPos = 0; rowDataPos < projectedFields.length; rowDataPos++) {
            final int tablePos = projectedFields[rowDataPos][0];
            if (tableToDataTypePos.containsKey(tablePos)) {
                final Integer dataTypePos = tableToDataTypePos.get(tablePos);
                rowDataProjection.put(decoderProjectionList.size(), rowDataPos);
                decoderProjectionList.add(new int[] {dataTypePos});
            }
        }

        return new ProjectionHandler(
                projectableDecodingFormat.createRuntimeDecoder(
                        context,
                        toDataType(tableDataType, tableProjection, prefix),
                        decoderProjectionList.toArray(new int[decoderProjectionList.size()][])),
                rowDataProjection);
    }

    /**
     * The returned {@link ProjectionHandler} has the following behaviour.
     * <li>
     *
     *     <ol>
     *       {@link #getDeserializationSchema} will decode all the fields (only a subset of which
     *       may be desired).
     * </ol>
     *
     * <ol>
     *   {@link #getDataTypePosToRowDataPos} will return a map containing an entry for each of the
     *   desired fields mapping the position in the decoded RowData to it's target position in
     *   produced RowData.
     * </ol>
     */
    static ProjectionHandler withoutProjectionPushdownIntoDecoder(
            final Context context,
            final DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            final DataType tableDataType,
            final int[] tableProjection,
            final @Nullable String prefix,
            final int[][] projectedFields) {
        final DataType dataType = toDataType(tableDataType, tableProjection, prefix);

        final HashMap<Integer, Integer> tableToDataTypePos = new HashMap<>();
        for (int i = 0; i < tableProjection.length; i++) {
            tableToDataTypePos.put(tableProjection[i], i);
        }

        final Map<Integer, Integer> dataTypeToRowDataPos = new HashMap<>();
        for (int rowDataPos = 0; rowDataPos < projectedFields.length; rowDataPos++) {
            final int tablePos = projectedFields[rowDataPos][0];
            if (tableToDataTypePos.containsKey(tablePos)) {
                final Integer dataTypePos = tableToDataTypePos.get(tablePos);
                dataTypeToRowDataPos.put(dataTypePos, rowDataPos);
            }
        }

        return new ProjectionHandler(
                decodingFormat.createRuntimeDecoder(context, dataType), dataTypeToRowDataPos);
    }

    /**
     * The returned {@link ProjectionHandler} has the following behaviour.
     * <li>
     *
     *     <ol>
     *       {@link #getDeserializationSchema} will decode all the fields.
     * </ol>
     *
     * <ol>
     *   {@link #getDataTypePosToRowDataPos} will return a map containing an entry for each field
     *   mapping the position in the decoded RowData to it's target position in produced RowData.
     * </ol>
     */
    private static ProjectionHandler withoutProjection(
            final Context context,
            final DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            final DataType tableDataType,
            final int[] tableProjection,
            final @Nullable String prefix) {
        final HashMap<Integer, Integer> dataTypeToTablePos = new HashMap<>();
        for (int i = 0; i < tableProjection.length; i++) {
            dataTypeToTablePos.put(i, tableProjection[i]);
        }

        return new ProjectionHandler(
                decodingFormat.createRuntimeDecoder(
                        context, toDataType(tableDataType, tableProjection, prefix)),
                dataTypeToTablePos);
    }
}
