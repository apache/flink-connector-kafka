package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableDataTypeUtils}. */
public class TableDataTypeUtilsTest {

    @Test
    public void testStripRowPrefix() {
        DataType rowDataType =
                ROW(
                        FIELD("prefix_name", STRING()),
                        FIELD("prefix_age", INT()),
                        FIELD("address", STRING()));

        DataType result = TableDataTypeUtils.stripRowPrefix(rowDataType, "prefix_");

        RowType rowType = (RowType) result.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();

        assertThat(fieldNames).containsExactly("name", "age", "address");
    }

    @Test
    public void testStripRowPrefixWithNoMatch() {
        // Create a test row data type with no matching prefixes
        DataType rowDataType =
                ROW(FIELD("name", STRING()), FIELD("age", INT()), FIELD("address", STRING()));

        DataType result = TableDataTypeUtils.stripRowPrefix(rowDataType, "nonexistent_");

        // Field names should remain unchanged
        RowType rowType = (RowType) result.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();

        assertThat(fieldNames).containsExactly("name", "age", "address");
    }

    @Test
    public void testStripRowPrefixInvalidType() {
        // Create a non-row data type
        DataType nonRowType = STRING();

        // Attempt to strip prefix should throw an exception
        assertThatThrownBy(() -> TableDataTypeUtils.stripRowPrefix(nonRowType, "prefix_"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TableDataTypeUtils.STRIP_ROW_NO_ROW_ERROR_MSG);
    }

    @Test
    public void testRenameRowFields() {
        List<RowType.RowField> fields =
                Arrays.asList(
                        new RowType.RowField("oldName1", new VarCharType(), null),
                        new RowType.RowField("oldName2", new IntType(), "description"));
        RowType rowType = new RowType(false, fields);

        List<String> newFieldNames = Arrays.asList("newName1", "newName2");

        RowType renamedType = TableDataTypeUtils.renameRowFields(rowType, newFieldNames);

        List<String> resultFieldNames = renamedType.getFieldNames();
        assertThat(resultFieldNames).containsExactly("newName1", "newName2");

        assertThat(renamedType.getFields().get(0).getType()).isInstanceOf(VarCharType.class);
        assertThat(renamedType.getFields().get(1).getType()).isInstanceOf(IntType.class);
        assertThat(renamedType.getFields().get(1).getDescription().orElse(null))
                .isEqualTo("description");
    }

    @Test
    public void testRenameRowFieldsInvalidLength() {
        List<RowType.RowField> fields =
                Arrays.asList(
                        new RowType.RowField("oldName1", new VarCharType(), null),
                        new RowType.RowField("oldName2", new IntType(), null));
        RowType rowType = new RowType(false, fields);

        // Incorrect number of new field names
        List<String> newFieldNames = Collections.singletonList("newName1");

        // Rename with incorrect number of fields should throw an exception
        assertThatThrownBy(() -> TableDataTypeUtils.renameRowFields(rowType, newFieldNames))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TableDataTypeUtils.RENAME_ROW_LENGTH_MISMATCH_ERROR_MSG);
    }
}
