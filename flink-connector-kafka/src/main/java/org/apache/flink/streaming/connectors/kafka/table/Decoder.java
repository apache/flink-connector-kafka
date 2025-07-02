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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Decoding messages consists of two potential steps:
 *
 * <ol>
 *   <li>Deserialization i.e deserializing the {@code byte[]} into a {@link RowData}. This process
 *       is handled by a {@link DeserializationSchema}.
 *   <li>Projection i.e. projecting any required fields from the deserialized {@link RowData}
 *       (returned by the {@link DeserializationSchema} in the first step) to their positions in the
 *       final produced {@link RowData}. This process is handled by a {@link Projector}.
 * </ol>
 *
 * <p>In order to decode messages correctly, the {@link DeserializationSchema} and the {@link
 * Projector} need to work together. For example, the {@link Projector} needs to know the positions
 * of the required fields in the {@link RowData} returned by the {@link DeserializationSchema} in
 * order to be able to correctly set fields in the final produced {@link RowData}.
 *
 * <p>That's why we have this {@link Decoder} class. This class ensures that the returned {@link
 * DeserializationSchema} and {@link Projector} will work together to decode messages correctly.
 */
@Internal
public class Decoder {

    /**
     * Can be null. Null is used inside {@link DynamicKafkaDeserializationSchema} to avoid
     * deserializing keys if not required.
     */
    private final @Nullable DeserializationSchema<RowData> deserializationSchema;

    /** Mapping of the physical position in the key to the target position in the RowData. */
    private final Projector projector;

    private Decoder(
            final DeserializationSchema<RowData> deserializationSchema, final Projector projector) {
        this.deserializationSchema = deserializationSchema;
        this.projector = projector;
    }

    /**
     * @param decodingFormat Optional format for decoding bytes.
     * @param physicalTableDataType The data type representing the table schema.
     * @param physicalDataTypeProjection Indices indicate the position of the field in the dataType
     *     (key/value). Values indicate the position of the field in the tableSchema.
     * @param prefix Optional field prefix
     * @param projectedPhysicalFields Indices indicate the position of the field in the produced
     *     Row. Values indicate the position of the field in the table schema.
     * @param pushProjectionsIntoDecodingFormat if this is true and the format is a {@link
     *     ProjectableDecodingFormat}, any {@param projectedPhysicalFields} will be pushed down into
     *     the {@link ProjectableDecodingFormat}. Otherwise, projections will be applied after
     *     deserialization.
     * @return a {@link Decoder} instance.
     */
    public static Decoder create(
            final Context context,
            final @Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            final DataType physicalTableDataType,
            final int[] physicalDataTypeProjection,
            final @Nullable String prefix,
            final int[][] projectedPhysicalFields,
            final List<String> metadataKeys,
            final boolean pushProjectionsIntoDecodingFormat) {
        if (decodingFormat == null) {
            return Decoder.noDeserializationOrProjection();
        } else {
            if (decodingFormat instanceof ProjectableDecodingFormat
                    && pushProjectionsIntoDecodingFormat) {
                return Decoder.projectInsideDeserializer(
                        context,
                        (ProjectableDecodingFormat<DeserializationSchema<RowData>>) decodingFormat,
                        physicalTableDataType,
                        physicalDataTypeProjection,
                        prefix,
                        projectedPhysicalFields,
                        metadataKeys);
            } else {
                return Decoder.projectAfterDeserializing(
                        context,
                        decodingFormat,
                        physicalTableDataType,
                        physicalDataTypeProjection,
                        prefix,
                        projectedPhysicalFields,
                        metadataKeys);
            }
        }
    }

    /** @return a {@link DeserializationSchema} or null. */
    @Nullable
    public DeserializationSchema<RowData> getDeserializationSchema() {
        return deserializationSchema;
    }

    /** @return a {@link Projector}. */
    public Projector getProjector() {
        return projector;
    }

    private static Decoder noDeserializationOrProjection() {
        return new Decoder(null, new ProjectorImpl(Collections.emptyMap(), 0, 0));
    }

    private static DataType toPhysicalDataType(
            final DataType physicalTableDataType,
            final int[] physicalDataTypeProjection,
            final @Nullable String prefix) {
        final DataType temp =
                Projection.of(physicalDataTypeProjection).project(physicalTableDataType);
        return Optional.ofNullable(prefix)
                .map(s -> TableDataTypeUtils.stripRowPrefix(temp, s))
                .orElse(temp);
    }

    private static Map<Integer, Integer> tableToDeserializedTopLevelPos(
            final int[] dataTypeProjection) {
        final HashMap<Integer, Integer> tableToDeserializedPos = new HashMap<>();
        for (int i = 0; i < dataTypeProjection.length; i++) {
            tableToDeserializedPos.put(dataTypeProjection[i], i);
        }
        return tableToDeserializedPos;
    }

    private static int[] copyArray(final int[] arr) {
        return Arrays.copyOf(arr, arr.length);
    }

    private static void addMetadataProjections(
            final DecodingFormat<?> decodingFormat,
            final int deserializedSize,
            final int physicalSize,
            final List<String> requestedMetadataKeys,
            final Map<List<Integer>, Integer> deserializedToProducedPos) {

        if (!requestedMetadataKeys.isEmpty()) {
            decodingFormat.applyReadableMetadata(requestedMetadataKeys);

            // project only requested metadata keys
            for (int i = 0; i < requestedMetadataKeys.size(); i++) {
                // metadata is always added to the end of the deserialized row by the DecodingFormat
                final int deserializedPos = deserializedSize + i;
                // we need to always add metadata to the end of the produced row
                final int producePos = physicalSize + i;
                deserializedToProducedPos.put(
                        Collections.singletonList(deserializedPos), producePos);
            }
        }
    }

    /**
     * This method generates a {@link Decoder} which pushes projections down directly into the
     * {@link ProjectableDecodingFormat} which takes care of projecting the fields during the
     * deserialization process itself.
     */
    private static Decoder projectInsideDeserializer(
            final Context context,
            final ProjectableDecodingFormat<DeserializationSchema<RowData>>
                    projectableDecodingFormat,
            final DataType physicalTableDataType,
            final int[] physicalDataTypeProjection,
            final @Nullable String prefix,
            final int[][] projectedPhysicalFields,
            final List<String> metadataKeys) {
        final Map<Integer, Integer> tableToDeserializedTopLevelPos =
                tableToDeserializedTopLevelPos(physicalDataTypeProjection);

        final List<int[]> deserializerProjectedFields = new ArrayList<>();
        final Map<List<Integer>, Integer> deserializedToProducedPos = new HashMap<>();
        for (int producedPos = 0; producedPos < projectedPhysicalFields.length; producedPos++) {
            final int[] tablePos = projectedPhysicalFields[producedPos];
            final int tableTopLevelPos = tablePos[0];

            final Integer dataTypeTopLevelPos =
                    tableToDeserializedTopLevelPos.get(tableTopLevelPos);
            if (dataTypeTopLevelPos != null) {
                final int[] dataTypePos = copyArray(tablePos);
                dataTypePos[0] = dataTypeTopLevelPos;

                deserializerProjectedFields.add(dataTypePos);

                final int deserializedPos = deserializerProjectedFields.size() - 1;
                deserializedToProducedPos.put(
                        Collections.singletonList(deserializedPos), producedPos);
            }
        }

        addMetadataProjections(
                projectableDecodingFormat,
                deserializerProjectedFields.size(),
                projectedPhysicalFields.length,
                metadataKeys,
                deserializedToProducedPos);

        return new Decoder(
                projectableDecodingFormat.createRuntimeDecoder(
                        context,
                        toPhysicalDataType(
                                physicalTableDataType, physicalDataTypeProjection, prefix),
                        deserializerProjectedFields.toArray(
                                new int[deserializerProjectedFields.size()][])),
                new ProjectorImpl(
                        deserializedToProducedPos,
                        deserializerProjectedFields.size(),
                        metadataKeys.size()));
    }

    /**
     * This method generates a {@link Decoder} which deserializes the data fully using the {@link
     * DecodingFormat} and then applies any projections afterward.
     */
    private static Decoder projectAfterDeserializing(
            final Context context,
            final DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            final DataType physicalTableDataType,
            final int[] physicalDataTypeProjection,
            final @Nullable String prefix,
            final int[][] projectedPhysicalFields,
            final List<String> metadataKeys) {
        final DataType physicalDataType =
                toPhysicalDataType(physicalTableDataType, physicalDataTypeProjection, prefix);
        final Map<Integer, Integer> tableToDeserializedTopLevelPos =
                tableToDeserializedTopLevelPos(physicalDataTypeProjection);

        final Map<List<Integer>, Integer> deserializedToProducedPos = new HashMap<>();
        for (int producedPos = 0; producedPos < projectedPhysicalFields.length; producedPos++) {
            final int[] tablePos = projectedPhysicalFields[producedPos];
            int tableTopLevelPos = tablePos[0];

            final Integer deserializedTopLevelPos =
                    tableToDeserializedTopLevelPos.get(tableTopLevelPos);
            if (deserializedTopLevelPos != null) {
                final int[] deserializedPos = copyArray(tablePos);
                deserializedPos[0] = deserializedTopLevelPos;

                deserializedToProducedPos.put(
                        Collections.unmodifiableList(
                                Arrays.stream(deserializedPos)
                                        .boxed()
                                        .collect(Collectors.toList())),
                        producedPos);
            }
        }

        addMetadataProjections(
                decodingFormat,
                physicalDataTypeProjection.length,
                projectedPhysicalFields.length,
                metadataKeys,
                deserializedToProducedPos);

        return new Decoder(
                decodingFormat.createRuntimeDecoder(context, physicalDataType),
                new ProjectorImpl(
                        deserializedToProducedPos,
                        physicalDataTypeProjection.length,
                        metadataKeys.size()));
    }

    /** Projects fields from the deserialized row to their positions in the final produced row. */
    @Internal
    public interface Projector extends Serializable {
        /** Returns true if {@link #project} will not project any fields. */
        boolean isEmptyProjection();

        /**
         * Returns true if projection is needed i.e. if the produced record is different from the
         * deserialized record
         */
        boolean isProjectionNeeded();

        /** Copies fields from the deserialized row to their final positions in the produced row. */
        void project(final RowData deserialized, final GenericRowData producedRow);
    }

    private static class ProjectorImpl implements Projector {

        private final Map<List<Integer>, Integer> deserializedToProducedPos;
        private final boolean isProjectionNeeded;

        ProjectorImpl(
                final Map<List<Integer>, Integer> deserializedToProducedPos,
                final int numDeserializedPhysicalFields,
                final int numMetadataFields) {
            this.deserializedToProducedPos = deserializedToProducedPos;
            this.isProjectionNeeded =
                    !(deserializedToProducedPos.size()
                                    == (numDeserializedPhysicalFields + numMetadataFields)
                            && samePositions(deserializedToProducedPos));
        }

        private static boolean samePositions(
                Map<List<Integer>, Integer> deserializedToProducedPos) {
            return deserializedToProducedPos.entrySet().stream()
                    .allMatch(
                            entry -> {
                                final List<Integer> deserializedPos = entry.getKey();
                                final List<Integer> producedPos =
                                        Collections.singletonList(entry.getValue());
                                return Objects.equals(producedPos, deserializedPos);
                            });
        }

        @Override
        public boolean isEmptyProjection() {
            return deserializedToProducedPos.isEmpty();
        }

        @Override
        public boolean isProjectionNeeded() {
            return isProjectionNeeded;
        }

        @Override
        public void project(final RowData deserialized, final GenericRowData producedRow) {
            this.deserializedToProducedPos.forEach(
                    (deserializedPos, targetPos) -> {
                        Object value = deserialized;
                        for (final Integer i : deserializedPos) {
                            value = ((GenericRowData) value).getField(i);
                        }
                        producedRow.setField(targetPos, value);
                    });
        }
    }
}
