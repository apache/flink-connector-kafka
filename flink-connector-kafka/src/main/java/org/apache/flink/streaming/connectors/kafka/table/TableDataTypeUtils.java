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
 *
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;

/**
 * Utility class for manipulating {@link DataType} objects, particularly for table schemas. This
 * class provides methods that were removed from the Flink API in version 2.1.0. See <a
 * href="https://github.com/apache/flink/pull/26784">Flink PR 26784</a>.
 */
public class TableDataTypeUtils {

    protected static final String STRIP_ROW_NO_ROW_ERROR_MSG = "Row data type expected.";
    protected static final String RENAME_ROW_LENGTH_MISMATCH_ERROR_MSG =
            "Row length and new names must match.";

    /**
     * Removes a string prefix from the fields of the given row data type.
     *
     * @param dataType The row data type to modify.
     * @param prefix The prefix to remove from the field names.
     * @return A new DataType with the modified field names.
     * @throws IllegalArgumentException if the provided dataType is not of ROW type.
     */
    public static DataType stripRowPrefix(DataType dataType, String prefix) {

        if (!dataType.getLogicalType().is(ROW)) {
            throw new IllegalArgumentException(STRIP_ROW_NO_ROW_ERROR_MSG);
        }

        final RowType rowType = (RowType) dataType.getLogicalType();
        final List<String> newFieldNames =
                rowType.getFieldNames().stream()
                        .map(
                                s -> {
                                    if (s.startsWith(prefix)) {
                                        return s.substring(prefix.length());
                                    }
                                    return s;
                                })
                        .collect(Collectors.toList());
        final LogicalType newRowType = renameRowFields(rowType, newFieldNames);
        return new FieldsDataType(
                newRowType, dataType.getConversionClass(), dataType.getChildren());
    }

    /**
     * Renames the fields of the given {@link RowType}.
     *
     * @param rowType The original RowType.
     * @param newFieldNames The new field names to apply.
     * @return A new RowType with the updated field names.
     * @throws IllegalArgumentException if the number of new field names does not match the number
     *     of fields in the original RowType.
     */
    public static RowType renameRowFields(RowType rowType, List<String> newFieldNames) {

        if (!(rowType.getFieldCount() == newFieldNames.size())) {
            throw new IllegalArgumentException(RENAME_ROW_LENGTH_MISMATCH_ERROR_MSG);
        }

        final List<RowType.RowField> newFields =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                pos -> {
                                    final RowType.RowField oldField = rowType.getFields().get(pos);
                                    return new RowType.RowField(
                                            newFieldNames.get(pos),
                                            oldField.getType(),
                                            oldField.getDescription().orElse(null));
                                })
                        .collect(Collectors.toList());
        return new RowType(rowType.isNullable(), newFields);
    }
}
