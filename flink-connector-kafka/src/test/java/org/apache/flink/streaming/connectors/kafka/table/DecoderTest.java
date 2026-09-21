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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.testutils.SimpleCollector;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Decoder}. */
class DecoderTest {

    private static ProjectableDecodingFormat<DeserializationSchema<RowData>> jsonDecodingFormat() {
        return (ProjectableDecodingFormat<DeserializationSchema<RowData>>)
                new JsonFormatFactory().createDecodingFormat(null, new Configuration());
    }

    private static ProjectableDecodingFormat<DeserializationSchema<RowData>>
            debeziumJsonDecodingFormat() {
        return new DebeziumJsonDecodingFormat(false, false, TimestampFormat.ISO_8601);
    }

    private abstract static class RecordingDecodingFormat<T> implements DecodingFormat<T> {

        final DecodingFormat<T> underlying;

        private List<String> appliedMetadataKeys = null;
        protected int[][] projectedFields = null;

        RecordingDecodingFormat(final DecodingFormat<T> underlying) {
            this.underlying = underlying;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return underlying.getChangelogMode();
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            return underlying.listReadableMetadata();
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys) {
            this.appliedMetadataKeys = metadataKeys;
            underlying.applyReadableMetadata(metadataKeys);
        }

        List<String> getAppliedMetadataKeys() {
            return appliedMetadataKeys;
        }

        /** The projection pushed into the format, or null if none was pushed. */
        int[][] getProjectedFields() {
            return projectedFields;
        }
    }

    private static class RecordingNonProjectableFormat<T> extends RecordingDecodingFormat<T> {

        RecordingNonProjectableFormat(final DecodingFormat<T> underlying) {
            super(underlying);
        }

        @Override
        public T createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {
            return underlying.createRuntimeDecoder(context, physicalDataType);
        }
    }

    private static class RecordingProjectableFormat<T> extends RecordingDecodingFormat<T>
            implements ProjectableDecodingFormat<T> {

        private final boolean supportsNestedProjection;

        RecordingProjectableFormat(
                final ProjectableDecodingFormat<T> underlying,
                final boolean supportsNestedProjection) {
            super(underlying);
            this.supportsNestedProjection = supportsNestedProjection;
        }

        @Override
        public boolean supportsNestedProjection() {
            return supportsNestedProjection;
        }

        @Override
        public T createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {
            return underlying.createRuntimeDecoder(context, physicalDataType);
        }

        @Override
        public T createRuntimeDecoder(
                DynamicTableSource.Context context,
                DataType physicalDataType,
                int[][] projectedFields) {
            this.projectedFields = projectedFields;
            return ((ProjectableDecodingFormat<T>) underlying)
                    .createRuntimeDecoder(context, physicalDataType, projectedFields);
        }
    }

    private static void assertThatIsProjectionNeeded(
            final boolean pushdownEnabled,
            final DecodingFormat<DeserializationSchema<RowData>> format,
            final Decoder decoder,
            final boolean forPushdownDisabled,
            final boolean forNonProjectableDecodingFormat,
            final boolean forProjectableDecodingFormat,
            final boolean forProjectableTopLevelOnlyDecodingFormat) {
        final boolean expected =
                expectedForVariant(
                        pushdownEnabled,
                        format,
                        forPushdownDisabled,
                        forNonProjectableDecodingFormat,
                        forProjectableDecodingFormat,
                        forProjectableTopLevelOnlyDecodingFormat);

        assertThat(decoder.getProjector().isProjectionNeeded()).isEqualTo(expected);
    }

    private static void assertThatPushedProjectionEquals(
            final boolean pushdownEnabled,
            final DecodingFormat<DeserializationSchema<RowData>> format,
            final int[][] forPushdownDisabled,
            final int[][] forNonProjectableDecodingFormat,
            final int[][] forProjectableDecodingFormat,
            final int[][] forProjectableTopLevelOnlyDecodingFormat) {
        final int[][] expected =
                expectedForVariant(
                        pushdownEnabled,
                        format,
                        forPushdownDisabled,
                        forNonProjectableDecodingFormat,
                        forProjectableDecodingFormat,
                        forProjectableTopLevelOnlyDecodingFormat);

        final int[][] result = ((RecordingDecodingFormat<?>) format).getProjectedFields();
        if (expected == null) {
            assertThat(result).isNull();
        } else {
            assertThat(result).isDeepEqualTo(expected);
        }
    }

    private static <T> T expectedForVariant(
            final boolean pushdownEnabled,
            final DecodingFormat<DeserializationSchema<RowData>> format,
            final T forPushdownDisabled,
            final T forNonProjectableDecodingFormat,
            final T forProjectableDecodingFormat,
            final T forProjectableTopLevelOnlyDecodingFormat) {
        if (pushdownEnabled) {
            if (format instanceof ProjectableDecodingFormat) {
                if (((ProjectableDecodingFormat<?>) format).supportsNestedProjection()) {
                    return forProjectableDecodingFormat;
                } else {
                    return forProjectableTopLevelOnlyDecodingFormat;
                }
            } else {
                return forNonProjectableDecodingFormat;
            }
        } else {
            return forPushdownDisabled;
        }
    }

    private static Stream<Arguments> jsonTestCases() {
        return testCases(DecoderTest::jsonDecodingFormat);
    }

    // debezium-json format exposes some metadata fields (unlike json format)
    private static Stream<Arguments> debeziumJsonTestCases() {
        return testCases(DecoderTest::debeziumJsonDecodingFormat);
    }

    private static Stream<Arguments> testCases(
            final Supplier<ProjectableDecodingFormat<DeserializationSchema<RowData>>> baseFormat) {
        return Stream.of(
                testCase(
                        "Non-Projectable (Pushdown Disabled)",
                        () -> new RecordingNonProjectableFormat<>(baseFormat.get()),
                        false),
                testCase(
                        "Projectable (Pushdown Disabled)",
                        () -> new RecordingProjectableFormat<>(baseFormat.get(), true),
                        false),
                testCase(
                        "Projectable Top-Level-Only (Pushdown Disabled)",
                        () -> new RecordingProjectableFormat<>(baseFormat.get(), false),
                        false),
                testCase(
                        "Non-Projectable (Pushdown Enabled)",
                        () -> new RecordingNonProjectableFormat<>(baseFormat.get()),
                        true),
                testCase(
                        "Projectable (Pushdown Enabled)",
                        () -> new RecordingProjectableFormat<>(baseFormat.get(), true),
                        true),
                testCase(
                        "Projectable Top-Level-Only (Pushdown Enabled)",
                        () -> new RecordingProjectableFormat<>(baseFormat.get(), false),
                        true));
    }

    private static Arguments testCase(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> format,
            final boolean pushdownEnabled) {
        return Arguments.of(Named.of(name, format), pushdownEnabled);
    }

    private static final DataType TABLE_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("a", DataTypes.STRING()),
                    DataTypes.FIELD("b", DataTypes.STRING()),
                    DataTypes.FIELD("c", DataTypes.STRING()));

    private static final String TABLE_INPUT = "{\"a\":\"a\", \"b\":\"b\", \"c\":\"c\"}";

    private static final int[] DATA_TYPE_PROJECTION = new int[] {0, 1, 2};

    private static final DataType NESTED_TABLE_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD(
                            "a0",
                            DataTypes.ROW(
                                    DataTypes.FIELD("a1", DataTypes.STRING()),
                                    DataTypes.FIELD("b1", DataTypes.STRING()))),
                    DataTypes.FIELD(
                            "b0",
                            DataTypes.ROW(
                                    DataTypes.FIELD("a1", DataTypes.STRING()),
                                    DataTypes.FIELD("b1", DataTypes.STRING()))));

    private static class MockContext implements DynamicTableSource.Context {
        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
            return InternalTypeInfo.of(producedDataType.getLogicalType());
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
            return InternalTypeInfo.of(producedLogicalType);
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                DataType producedDataType) {
            throw new UnsupportedOperationException();
        }
    }

    private static void produceRow(
            final Decoder decoder, final String input, final GenericRowData producedRow) {
        // decode
        final RowData deserialized;
        try {
            final DeserializationSchema<RowData> deserializationSchema =
                    decoder.getDeserializationSchema();
            deserializationSchema.open(null);
            final SimpleCollector<RowData> collector = new SimpleCollector<>();
            deserializationSchema.deserialize(input.getBytes(), collector);
            deserialized = collector.getList().get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // project
        decoder.getProjector().project(deserialized, producedRow);
    }

    @Test
    void testProjectsNothingWhenFormatIsNull() {
        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        null,
                        TABLE_DATA_TYPE,
                        new int[] {},
                        null,
                        new int[][] {},
                        Collections.emptyList(),
                        false);

        assertThat(decoder.getDeserializationSchema()).isNull();
        assertThat(decoder.getProjector().isEmptyProjection()).isTrue();
        assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
    }

    @Test
    void testProjectsNothingWhenProjectionIsEmpty() {
        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        jsonDecodingFormat(),
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        new int[][] {},
                        Collections.emptyList(),
                        false);

        assertThat(decoder.getDeserializationSchema()).isNotNull();
        assertThat(decoder.getProjector().isEmptyProjection()).isTrue();
        assertThat(decoder.getProjector().isProjectionNeeded()).isTrue();

        final GenericRowData producedRow = new GenericRowData(0);
        produceRow(decoder, TABLE_INPUT, producedRow);

        assertThat(producedRow.toString()).isEqualTo(new GenericRowData(0).toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsAllFields(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0}, new int[] {1}, new int[] {2},
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}, new int[] {2}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}, new int[] {2}
                });

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(1, "b");
        expected.setField(2, "c");

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ false,
                /* forNonProjectableDecodingFormat= */ false,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsFirstField(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields = new int[][] {new int[] {0}};

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {0}});

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 1;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsSecondField(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields = new int[][] {new int[] {1}};

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {1}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {1}});

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 1;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "b");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsAllFieldsInDifferentOrder(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        // Reverse order of table schema
        final int[][] projectedFields =
                new int[][] {
                    new int[] {2}, new int[] {1}, new int[] {0},
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {2}, new int[] {1}, new int[] {0}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {2}, new int[] {1}, new int[] {0}
                });

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "c");
        expected.setField(1, "b");
        expected.setField(2, "a");

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsContiguousSubsetOfFields(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0}, new int[] {1},
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}, new int[] {1}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}
                });

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(1, "b");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsNonContiguousSubsetOfFields(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] valueProjection = new int[] {0, 2};
        final int[][] projectedFields =
                new int[][] {
                    new int[] {0}, new int[] {1}, new int[] {2},
                };

        final Decoder valueDecoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        valueProjection,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}, new int[] {1}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}
                });

        assertThat(valueDecoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                valueDecoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ true,
                /* forProjectableTopLevelOnlyDecodingFormat= */ true);

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(valueDecoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(2, "c");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsContiguousSubsetOfFieldsInDifferentOrder(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields =
                new int[][] {
                    new int[] {1}, new int[] {0},
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        TABLE_DATA_TYPE,
                        DATA_TYPE_PROJECTION,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {1}, new int[] {0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {1}, new int[] {0}
                });

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, TABLE_INPUT, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "b");
        expected.setField(1, "a");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsFieldWithKeyPrefix(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final String prefix = "k_";
        final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD(prefix + "a", DataTypes.STRING()), // key field
                        DataTypes.FIELD(prefix + "b", DataTypes.STRING()), // key field
                        DataTypes.FIELD("a", DataTypes.STRING()), // value field
                        DataTypes.FIELD("b", DataTypes.STRING()) // value field
                        );

        final int[] dataTypeProjection = new int[] {0, 1};
        final String input = "{\"a\":\"a\", \"b\":\"b\"}";

        final int[][] projectedFields = new int[][] {new int[] {1}};

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        dataTypeProjection,
                        prefix,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {1}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {1}});

        final int expectedArity = 1;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "b");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsNestedFields(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] dataTypeProjection = new int[] {0, 1};
        final String input =
                "{"
                        + "\"a0\":{\"a1\":\"a0_a1\", \"b1\":\"a0_b1\"}, "
                        + "\"b0\":{\"a1\":\"b0_a1\", \"b1\":\"b0_b1\"}"
                        + "}";

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0, 1}, // a0.b1
                    new int[] {1, 0} // b0.a1
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        NESTED_TABLE_DATA_TYPE,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        // Each format variant must only ever receive a projection it supports: the nested-capable
        // format gets the full nested paths, a top-level-only projectable format gets just the
        // (deduplicated) top-level parents (never nested paths, which would violate the {@link
        // ProjectableDecodingFormat#supportsNestedProjection()} contract), and the
        // project-after-deserializing variants get nothing pushed in. Any nested sub-fields are
        // extracted afterward.
        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {0, 1}, new int[] {1, 0}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}
                });

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ true);

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a0_b1");
        expected.setField(1, "b0_a1");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsNestedFieldsWithNullParent(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0, 1}, // a0.b1 -> a0 is null
                    new int[] {1, 0} // b0.a1
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        NESTED_TABLE_DATA_TYPE,
                        new int[] {0, 1},
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {0, 1}, new int[] {1, 0}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}
                });

        final String input = "{\"a0\":null, \"b0\":{\"a1\":\"b0_a1\", \"b1\":\"b0_b1\"}}";

        final GenericRowData producedRow = new GenericRowData(2);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, null);
        expected.setField(1, "b0_a1");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsDeeplyNestedFields(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "a0",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "a1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD("b2", DataTypes.STRING()))),
                                        DataTypes.FIELD(
                                                "b1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "b2", DataTypes.STRING()))))),
                        DataTypes.FIELD(
                                "b0",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "a1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD("b2", DataTypes.STRING()))),
                                        DataTypes.FIELD(
                                                "b1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "b2", DataTypes.STRING()))))));

        final int[] dataTypeProjection = new int[] {0, 1};
        final String input =
                "{\n"
                        + "  \"a0\": {\n"
                        + "    \"a1\": {\n"
                        + "      \"a2\": \"a0_a1_a2\",\n"
                        + "      \"b2\": \"a0_a1_b2\"\n"
                        + "    },\n"
                        + "    \"b1\": {\n"
                        + "      \"a2\": \"a0_b1_a2\",\n"
                        + "      \"b2\": \"a0_b1_b2\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"b0\": {\n"
                        + "    \"a1\": {\n"
                        + "      \"a2\": \"b0_a1_a2\",\n"
                        + "      \"b2\": \"b0_a1_b2\"\n"
                        + "    },\n"
                        + "    \"b1\": {\n"
                        + "      \"a2\": \"b0_b1_a2\",\n"
                        + "      \"b2\": \"b0_b1_b2\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0, 1, 1}, // a0.b1.b2
                    new int[] {1, 0, 0} // b0.a1.a2
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {0, 1, 1}, new int[] {1, 0, 0}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}
                });

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ true);

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a0_b1_b2");
        expected.setField(1, "b0_a1_a2");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsDeeplyNestedFieldsWithNullIntermediateParent(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "a0",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "a1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD("b2", DataTypes.STRING()))),
                                        DataTypes.FIELD(
                                                "b1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "b2", DataTypes.STRING()))))),
                        DataTypes.FIELD(
                                "b0",
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "a1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD("b2", DataTypes.STRING()))),
                                        DataTypes.FIELD(
                                                "b1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("a2", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "b2", DataTypes.STRING()))))));

        final int[] dataTypeProjection = new int[] {0, 1};
        // a0 is present but its intermediate child a0.b1 is null, so projecting a0.b1.b2 must
        // null-short-circuit mid-descent (rather than NPE) and yield null.
        final String input =
                "{\n"
                        + "  \"a0\": {\n"
                        + "    \"a1\": {\n"
                        + "      \"a2\": \"a0_a1_a2\",\n"
                        + "      \"b2\": \"a0_a1_b2\"\n"
                        + "    },\n"
                        + "    \"b1\": null\n"
                        + "  },\n"
                        + "  \"b0\": {\n"
                        + "    \"a1\": {\n"
                        + "      \"a2\": \"b0_a1_a2\",\n"
                        + "      \"b2\": \"b0_a1_b2\"\n"
                        + "    },\n"
                        + "    \"b1\": {\n"
                        + "      \"a2\": \"b0_b1_a2\",\n"
                        + "      \"b2\": \"b0_b1_b2\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0, 1, 1}, // a0.b1.b2 -> a0.b1 is null
                    new int[] {1, 0, 0} // b0.a1.a2
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {0, 1, 1}, new int[] {1, 0, 0}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {1}
                });

        final GenericRowData producedRow = new GenericRowData(2);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, null);
        expected.setField(1, "b0_a1_a2");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    /**
     * Two nested fields under the same top-level parent: a top-level-only projectable format must
     * receive that parent exactly once (the pushed-down top-level projection is deduplicated),
     * while a nested-capable format receives both distinct nested paths.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsNestedFieldsUnderSameParent(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[][] projectedFields =
                new int[][] {
                    new int[] {0, 0}, // a0.a1
                    new int[] {0, 1} // a0.b1
                };

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        NESTED_TABLE_DATA_TYPE,
                        new int[] {0, 1},
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {
                    new int[] {0, 0}, new int[] {0, 1}
                },
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {0}});

        final String input =
                "{"
                        + "\"a0\":{\"a1\":\"a0_a1\", \"b1\":\"a0_b1\"}, "
                        + "\"b0\":{\"a1\":\"b0_a1\", \"b1\":\"b0_b1\"}"
                        + "}";

        final GenericRowData producedRow = new GenericRowData(2);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(2);
        expected.setField(0, "a0_a1");
        expected.setField(1, "a0_b1");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("debeziumJsonTestCases")
    void testProjectsOneMetadataField(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final DataType tableDataType = DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()));

        final int[] dataTypeProjection = new int[] {0};
        final String input =
                "{"
                        + "\"before\":null,"
                        + "\"after\":{\"a\":\"a\"},"
                        + "\"op\":\"c\","
                        + "\"ts_ms\":0"
                        + "}";

        final int[][] projectedFields = new int[][] {new int[] {0}};

        final List<String> metadataKeys = Collections.singletonList("ingestion-timestamp");

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        metadataKeys,
                        isPushdownEnabled);

        assertThat(((RecordingDecodingFormat<?>) format).getAppliedMetadataKeys())
                .isEqualTo(metadataKeys);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {0}});

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ false,
                /* forNonProjectableDecodingFormat= */ false,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(1, TimestampData.fromEpochMillis(0));

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("debeziumJsonTestCases")
    void testProjectsTwoMetadataFields(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final DataType tableDataType = DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()));

        final int[] dataTypeProjection = new int[] {0};
        final String input =
                "{"
                        + "\"before\":null,"
                        + "\"after\":{\"a\":\"a\"},"
                        + "\"source\":{\"version\":\"1.1.1.Final\",\"connector\":\"mysql\",\"name\":\"dbserver1\",\"ts_ms\":0,\"snapshot\":\"true\",\"db\":\"inventory\",\"table\":\"products\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":154,\"row\":0,\"thread\":null,\"query\":null},"
                        + "\"op\":\"c\","
                        + "\"ts_ms\":0"
                        + "}";

        final int[][] projectedFields = new int[][] {new int[] {0}};

        final List<String> metadataKeys =
                Collections.unmodifiableList(
                        Arrays.asList("ingestion-timestamp", "source.database"));

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        metadataKeys,
                        isPushdownEnabled);

        assertThat(((RecordingDecodingFormat<?>) format).getAppliedMetadataKeys())
                .isEqualTo(metadataKeys);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {0}});

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ false,
                /* forNonProjectableDecodingFormat= */ false,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(1, TimestampData.fromEpochMillis(0));
        expected.setField(2, "inventory");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("debeziumJsonTestCases")
    void testProjectsReorderedFieldsWithMetadata(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.STRING()),
                        DataTypes.FIELD("b", DataTypes.STRING()));

        final int[] dataTypeProjection = new int[] {0, 1};
        final String input =
                "{"
                        + "\"before\":null,"
                        + "\"after\":{\"a\":\"a\", \"b\":\"b\"},"
                        + "\"op\":\"c\","
                        + "\"ts_ms\":0"
                        + "}";

        // Reorder the physical columns (b, a) and append a metadata field after them.
        final int[][] projectedFields = new int[][] {new int[] {1}, new int[] {0}};

        final List<String> metadataKeys = Collections.singletonList("ingestion-timestamp");

        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        metadataKeys,
                        isPushdownEnabled);

        assertThat(((RecordingDecodingFormat<?>) format).getAppliedMetadataKeys())
                .isEqualTo(metadataKeys);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {1}, new int[] {0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {1}, new int[] {0}
                });

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(
                isPushdownEnabled,
                format,
                decoder,
                /* forPushdownDisabled= */ true,
                /* forNonProjectableDecodingFormat= */ true,
                /* forProjectableDecodingFormat= */ false,
                /* forProjectableTopLevelOnlyDecodingFormat= */ false);

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "b");
        expected.setField(1, "a");
        expected.setField(2, TimestampData.fromEpochMillis(0));

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsKeyAndValueSubsetInDifferentOrder(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        // key fields are prefix
        // fields are in different order in the key/value as compared to the table schema
        // projected fields both reorders and subsets the table schema

        final String keyPrefix = "key_";
        final String valuePrefix = null;

        final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD(keyPrefix + "a", DataTypes.STRING()),
                        DataTypes.FIELD("a", DataTypes.STRING()),
                        DataTypes.FIELD(keyPrefix + "b", DataTypes.INT()),
                        DataTypes.FIELD(keyPrefix + "c", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.STRING()));

        final int[] keyProjection = new int[] {3, 0, 2};
        final String keyInput = "{\"c\":\"k_c\", \"a\":\"k_a\", \"b\":1}";

        final int[] valueProjection = new int[] {1, 4};
        final String valueInput = "{\"a\":\"v_a\", \"c\":\"v_c\"}";

        final int[][] projectedFields = new int[][] {new int[] {2}, new int[] {1}};

        final Decoder keyDecoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        keyProjection,
                        keyPrefix,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {2}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {2}});

        final Decoder valueDecoder =
                Decoder.create(
                        new MockContext(),
                        format,
                        tableDataType,
                        valueProjection,
                        valuePrefix,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                format,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {0}});

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(keyDecoder, keyInput, producedRow);
        produceRow(valueDecoder, valueInput, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, 1);
        expected.setField(1, "v_a");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectsKeyAndValueNestedSubsetInDifferentOrder(
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> keyFormat = formatSupplier.get();
        final DecodingFormat<DeserializationSchema<RowData>> valueFormat = formatSupplier.get();

        // key fields are prefix
        // fields are in different order in the key/value as compared to the table schema
        // projected fields both reorders, subsets, and un-nests the table schema

        final String keyPrefix = "key_";
        final String valuePrefix = null;

        final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD(keyPrefix + "a", DataTypes.STRING()),
                        DataTypes.FIELD("a", DataTypes.STRING()),
                        DataTypes.FIELD(keyPrefix + "b", DataTypes.INT()),
                        DataTypes.FIELD(keyPrefix + "c", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "d",
                                DataTypes.ROW(
                                        DataTypes.FIELD("e", DataTypes.STRING()),
                                        DataTypes.FIELD("f", DataTypes.STRING().nullable()))));

        final int[] keyProjection = new int[] {3, 0, 2};
        final String keyInput = "{\"c\":\"k_c\", \"a\":\"k_a\", \"b\":1}";

        final int[] valueProjection = new int[] {1, 4, 5};
        final String valueInput = "{\"a\":\"v_a\", \"c\":\"v_c\", \"d\":{\"e\":\"v_e\"}}";

        final int[][] projectedFields =
                new int[][] {
                    new int[] {2}, // k_b
                    new int[] {1}, // a
                    new int[] {5, 0} // d.e
                };

        final Decoder keyDecoder =
                Decoder.create(
                        new MockContext(),
                        keyFormat,
                        tableDataType,
                        keyProjection,
                        keyPrefix,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                keyFormat,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {2}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {new int[] {2}});

        final Decoder valueDecoder =
                Decoder.create(
                        new MockContext(),
                        valueFormat,
                        tableDataType,
                        valueProjection,
                        valuePrefix,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThatPushedProjectionEquals(
                isPushdownEnabled,
                valueFormat,
                /* forPushdownDisabled= */ null,
                /* forNonProjectableDecodingFormat= */ null,
                /* forProjectableDecodingFormat= */ new int[][] {new int[] {0}, new int[] {2, 0}},
                /* forProjectableTopLevelOnlyDecodingFormat= */ new int[][] {
                    new int[] {0}, new int[] {2}
                });

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(keyDecoder, keyInput, producedRow);
        produceRow(valueDecoder, valueInput, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, 1);
        expected.setField(1, "v_a");
        expected.setField(2, "v_e");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }
}
