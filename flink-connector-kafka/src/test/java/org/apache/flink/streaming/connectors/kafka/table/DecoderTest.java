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

import org.junit.jupiter.api.Nested;
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

    private static class NonProjectable<T> implements DecodingFormat<T> {

        private final ProjectableDecodingFormat<T> underlying;

        NonProjectable(final ProjectableDecodingFormat<T> underlying) {
            this.underlying = underlying;
        }

        @Override
        public T createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {
            return underlying.createRuntimeDecoder(context, physicalDataType);
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
            underlying.applyReadableMetadata(metadataKeys);
        }
    }

    private static class RecordingFormat<T> implements DecodingFormat<T> {

        private final DecodingFormat<T> underlying;

        private List<String> appliedMetadataKeys = null;

        RecordingFormat(final DecodingFormat<T> underlying) {
            this.underlying = underlying;
        }

        @Override
        public T createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {
            return underlying.createRuntimeDecoder(context, physicalDataType);
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
    }

    private static final String PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_ENABLED =
            "Projectable DecodingFormat with pushdown enabled";
    private static final String PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_DISABLED =
            "Projectable DecodingFormat with pushdown disabled";
    private static final String NON_PROJECTABLE_DECODING_FORMAT = "Non-projectable DecodingFormat";

    private static void assertThatIsProjectionNeeded(
            final String testName,
            final Decoder decoder,
            final boolean forProjectableWithPushdownEnabled,
            final boolean forProjectableWithPushdownDisabled,
            final boolean forNonProjectable) {
        final boolean expected;
        switch (testName) {
            case PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_ENABLED:
                expected = forProjectableWithPushdownEnabled;
                break;
            case PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_DISABLED:
                expected = forProjectableWithPushdownDisabled;
                break;
            case NON_PROJECTABLE_DECODING_FORMAT:
                expected = forNonProjectable;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + testName);
        }

        assertThat(decoder.getProjector().isProjectionNeeded()).isEqualTo(expected);
    }

    private static Stream<Arguments> jsonTestCases() {
        return Stream.of(
                Arguments.of(
                        PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_ENABLED,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                DecoderTest::jsonDecodingFormat,
                        true),
                Arguments.of(
                        PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_DISABLED,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                DecoderTest::jsonDecodingFormat,
                        false),
                Arguments.of(
                        NON_PROJECTABLE_DECODING_FORMAT,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                () -> new NonProjectable<>(jsonDecodingFormat()),
                        false));
    }

    // debezium-json format exposes some metadata fields (unlike json format)
    private static Stream<Arguments> debeziumJsonTestCases() {
        return Stream.of(
                Arguments.of(
                        PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_ENABLED,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                DecoderTest::debeziumJsonDecodingFormat,
                        true),
                Arguments.of(
                        PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_DISABLED,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                DecoderTest::debeziumJsonDecodingFormat,
                        false),
                Arguments.of(
                        NON_PROJECTABLE_DECODING_FORMAT,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                () -> new NonProjectable<>(debeziumJsonDecodingFormat()),
                        false));
    }

    private static final DataType tableDataType =
            DataTypes.ROW(
                    DataTypes.FIELD("a", DataTypes.STRING()),
                    DataTypes.FIELD("b", DataTypes.STRING()),
                    DataTypes.FIELD("c", DataTypes.STRING()),
                    DataTypes.FIELD("d", DataTypes.STRING()),
                    DataTypes.FIELD("e", DataTypes.STRING()),
                    DataTypes.FIELD("f", DataTypes.STRING()));

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
    void testNoDecoder() {
        final Decoder decoder =
                Decoder.create(
                        new MockContext(),
                        null,
                        tableDataType,
                        new int[] {},
                        null,
                        new int[][] {},
                        Collections.emptyList(),
                        false);

        assertThat(decoder.getDeserializationSchema()).isNull();
        assertThat(decoder.getProjector().isEmptyProjection()).isTrue();
        assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectAllFields(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] dataTypeProjection = new int[] {0, 1, 2, 3, 4, 5};
        final int[][] projectedFields =
                new int[][] {
                    new int[] {0},
                    new int[] {1},
                    new int[] {2},
                    new int[] {3},
                    new int[] {4},
                    new int[] {5},
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

        final String input =
                "{\"a\":\"a\", \"b\":\"b\", \"c\":\"c\", \"d\":\"d\", \"e\":\"e\", \"f\":\"f\"}";

        final int expectedArity = 6;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(1, "b");
        expected.setField(2, "c");
        expected.setField(3, "d");
        expected.setField(4, "e");
        expected.setField(5, "f");

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, false, false);

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectFirstFieldOnly(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] dataTypeProjection = new int[] {0, 2, 4};
        final int[][] projectedFields = new int[][] {new int[] {0}};

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

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, true, true);

        final String input = "{\"a\":\"a\", \"c\":\"c\", \"e\":\"e\"}";

        final int expectedArity = 1;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectToReorderedTableSchema(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] dataTypeProjection = new int[] {0, 2, 4};
        // Reverse order of table schema
        final int[][] projectedFields =
                new int[][] {
                    new int[] {5},
                    new int[] {4},
                    new int[] {3},
                    new int[] {2},
                    new int[] {1},
                    new int[] {0},
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

        final String input = "{\"a\":\"a\", \"c\":\"c\", \"e\":\"e\"}";

        final int expectedArity = 6;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(1, "e");
        expected.setField(3, "c");
        expected.setField(5, "a");

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, true, true, true);

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectToSubsetOfTableSchema(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] dataTypeProjection = new int[] {0, 2, 4};
        final int[][] projectedFields =
                new int[][] {
                    new int[] {0}, new int[] {1}, new int[] {2},
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

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, true, true, true);

        final String input = "{\"a\":\"a\", \"c\":\"c\", \"e\":\"e\"}";

        final int expectedArity = 3;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a");
        expected.setField(2, "c");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectToReorderedSubsetOfTableSchema(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final int[] dataTypeProjection = new int[] {0, 2, 4};
        final int[][] projectedFields =
                new int[][] {
                    new int[] {2}, new int[] {0}, new int[] {1},
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

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, true, true);

        final String input = "{\"a\":\"a\", \"c\":\"c\", \"e\":\"e\"}";

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "c");
        expected.setField(1, "a");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testPrefix(
            final String name,
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

        final int expectedArity = 1;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "b");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonTestCases")
    void testProjectNestedFields(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final DecodingFormat<DeserializationSchema<RowData>> format = formatSupplier.get();

        final DataType tableDataType =
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
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        Collections.emptyList(),
                        isPushdownEnabled);

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, true, true);

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
    void testProjectDeeplyNestedFields(
            final String name,
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

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, true, true);

        final int expectedArity = 2;

        final GenericRowData producedRow = new GenericRowData(expectedArity);
        produceRow(decoder, input, producedRow);

        final GenericRowData expected = new GenericRowData(expectedArity);
        expected.setField(0, "a0_b1_b2");
        expected.setField(1, "b0_a1_a2");

        assertThat(producedRow.toString()).isEqualTo(expected.toString());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("debeziumJsonTestCases")
    void testProjectOneMetadataField(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final RecordingFormat<DeserializationSchema<RowData>> recordingFormat =
                new RecordingFormat<>(formatSupplier.get());

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
                        recordingFormat,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        metadataKeys,
                        isPushdownEnabled);

        assertThat(recordingFormat.appliedMetadataKeys).isEqualTo(metadataKeys);

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, false, false);

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
    void testProjectTwoMetadataFields(
            final String name,
            final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier,
            final Boolean isPushdownEnabled) {
        final RecordingFormat<DeserializationSchema<RowData>> recordingFormat =
                new RecordingFormat<>(formatSupplier.get());

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
                        recordingFormat,
                        tableDataType,
                        dataTypeProjection,
                        null,
                        projectedFields,
                        metadataKeys,
                        isPushdownEnabled);

        assertThat(recordingFormat.appliedMetadataKeys).isEqualTo(metadataKeys);

        assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
        assertThatIsProjectionNeeded(name, decoder, false, false, false);

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
    @MethodSource("jsonTestCases")
    void testPrefixProjectToReorderedSubsetOfTableSchema(
            final String name,
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
    void testPrefixProjectToReorderedSubsetOfNestedTableSchema(
            final String name,
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

    private static Stream<Arguments> withoutProjectionPushdownCases() {
        return Stream.of(
                Arguments.of(
                        PROJECTABLE_DECODING_FORMAT_WITH_PUSHDOWN_DISABLED,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                DecoderTest::debeziumJsonDecodingFormat),
                Arguments.of(
                        NON_PROJECTABLE_DECODING_FORMAT,
                        (Supplier<DecodingFormat<DeserializationSchema<RowData>>>)
                                () -> new NonProjectable<>(debeziumJsonDecodingFormat())));
    }

    @Nested
    class IsProjectionNeeded {
        private final DataType tableDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.STRING()),
                        DataTypes.FIELD("b", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.STRING()));
        private final int[] dataTypeProjection = new int[] {0, 1, 2};

        private Decoder createDecoder(
                final Supplier<DecodingFormat<DeserializationSchema<RowData>>> format,
                final int[][] projectedFields,
                final boolean pushProjectionsIntoDecodingFormat) {
            return createDecoder(
                    format,
                    projectedFields,
                    Collections.emptyList(),
                    pushProjectionsIntoDecodingFormat);
        }

        private Decoder createDecoder(
                final Supplier<DecodingFormat<DeserializationSchema<RowData>>> format,
                final int[][] projectedFields,
                final List<String> metadataKeys,
                final boolean pushProjectionsIntoDecodingFormat) {
            return createDecoder(
                    format,
                    tableDataType,
                    dataTypeProjection,
                    projectedFields,
                    metadataKeys,
                    pushProjectionsIntoDecodingFormat);
        }

        private Decoder createDecoder(
                final Supplier<DecodingFormat<DeserializationSchema<RowData>>> format,
                final DataType tableDataType,
                final int[] dataTypeProjection,
                final int[][] projectedFields,
                final List<String> metadataKeys,
                final boolean pushProjectionsIntoDecodingFormat) {
            return Decoder.create(
                    new MockContext(),
                    format.get(),
                    tableDataType,
                    Arrays.copyOf(dataTypeProjection, dataTypeProjection.length),
                    null,
                    projectedFields,
                    metadataKeys,
                    pushProjectionsIntoDecodingFormat);
        }

        @Nested
        class WithoutProjectionPushdown {
            private final boolean dontPushProjectionsIntoDecodingFormat = false;

            @ParameterizedTest(name = "{0}")
            @MethodSource(
                    "org.apache.flink.streaming.connectors.kafka.table.DecoderTest#withoutProjectionPushdownCases")
            void testAllFields(
                    final String name,
                    final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier) {
                final int[][] projectedFields =
                        new int[][] {new int[] {0}, new int[] {1}, new int[] {2}};
                final Decoder decoder =
                        createDecoder(
                                formatSupplier,
                                projectedFields,
                                dontPushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @ParameterizedTest(name = "{0}")
            @MethodSource(
                    "org.apache.flink.streaming.connectors.kafka.table.DecoderTest#withoutProjectionPushdownCases")
            void testAllFieldsWithMetadata(
                    final String name,
                    final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier) {
                final int[][] projectedFields =
                        new int[][] {new int[] {0}, new int[] {1}, new int[] {2}};

                final Decoder decoder =
                        createDecoder(
                                formatSupplier,
                                projectedFields,
                                Collections.singletonList("ingestion-timestamp"),
                                dontPushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @ParameterizedTest(name = "{0}")
            @MethodSource(
                    "org.apache.flink.streaming.connectors.kafka.table.DecoderTest#withoutProjectionPushdownCases")
            void testAllFieldsInDifferentOrder(
                    final String name,
                    final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier) {
                final int[][] projectedFields =
                        new int[][] {new int[] {2}, new int[] {1}, new int[] {0}};
                final Decoder decoder =
                        createDecoder(
                                formatSupplier,
                                projectedFields,
                                dontPushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isTrue();
            }

            @ParameterizedTest(name = "{0}")
            @MethodSource(
                    "org.apache.flink.streaming.connectors.kafka.table.DecoderTest#withoutProjectionPushdownCases")
            void testSubsetOfContiguousFields(
                    final String name,
                    final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier) {
                final int[][] projectedFields =
                        new int[][] {
                            new int[] {0}, new int[] {1},
                        };
                final Decoder decoder =
                        createDecoder(
                                formatSupplier,
                                projectedFields,
                                dontPushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isTrue();
            }

            @ParameterizedTest(name = "{0}")
            @MethodSource(
                    "org.apache.flink.streaming.connectors.kafka.table.DecoderTest#withoutProjectionPushdownCases")
            void testSubsetOfContiguousFieldsInDifferentOrder(
                    final String name,
                    final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier) {
                final int[][] projectedFields =
                        new int[][] {
                            new int[] {1}, new int[] {0},
                        };
                final Decoder decoder =
                        createDecoder(
                                formatSupplier,
                                projectedFields,
                                dontPushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isTrue();
            }

            @ParameterizedTest(name = "{0}")
            @MethodSource(
                    "org.apache.flink.streaming.connectors.kafka.table.DecoderTest#withoutProjectionPushdownCases")
            void testSubsetOfNonContiguousFields(
                    final String name,
                    final Supplier<DecodingFormat<DeserializationSchema<RowData>>> formatSupplier) {
                final DataType tableDataType =
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.STRING()), // value
                                DataTypes.FIELD("b", DataTypes.STRING()), // key
                                DataTypes.FIELD("c", DataTypes.STRING())); // value

                final int[] dataTypeProjection = new int[] {0, 2};

                final int[][] projectedFields =
                        new int[][] {
                            new int[] {0}, // value
                            new int[] {1}, // key
                            new int[] {2} // value
                        };

                final Decoder valueDecoder =
                        createDecoder(
                                formatSupplier,
                                tableDataType,
                                dataTypeProjection,
                                projectedFields,
                                Collections.emptyList(),
                                dontPushProjectionsIntoDecodingFormat);

                assertThat(valueDecoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(valueDecoder.getProjector().isProjectionNeeded()).isTrue();
            }
        }

        @Nested
        class WithProjectionPushdown {
            private final Supplier<DecodingFormat<DeserializationSchema<RowData>>> format =
                    DecoderTest::debeziumJsonDecodingFormat;
            private final boolean pushProjectionsIntoDecodingFormat = true;

            @Test
            void testAllFields() {
                final int[][] projectedFields =
                        new int[][] {new int[] {0}, new int[] {1}, new int[] {2}};
                final Decoder decoder =
                        createDecoder(format, projectedFields, pushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @Test
            void testAllFieldsWithMetadata() {
                final int[][] projectedFields =
                        new int[][] {new int[] {0}, new int[] {1}, new int[] {2}};

                final Decoder decoder =
                        createDecoder(
                                format,
                                projectedFields,
                                Collections.singletonList("ingestion-timestamp"),
                                pushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @Test
            void testAllFieldsInDifferentOrder() {
                final int[][] projectedFields =
                        new int[][] {new int[] {2}, new int[] {1}, new int[] {0}};
                final Decoder decoder =
                        createDecoder(format, projectedFields, pushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @Test
            void testSubsetOfContiguousFields() {
                final int[][] projectedFields =
                        new int[][] {
                            new int[] {0}, new int[] {1},
                        };
                final Decoder decoder =
                        createDecoder(format, projectedFields, pushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @Test
            void testSubsetOfContiguousFieldsInDifferentOrder() {
                final int[][] projectedFields =
                        new int[][] {
                            new int[] {1}, new int[] {0},
                        };
                final Decoder decoder =
                        createDecoder(format, projectedFields, pushProjectionsIntoDecodingFormat);

                assertThat(decoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(decoder.getProjector().isProjectionNeeded()).isFalse();
            }

            @Test
            void testSubsetOfNonContiguousFields() {
                final DataType tableDataType =
                        DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.STRING()), // value
                                DataTypes.FIELD("b", DataTypes.STRING()), // key
                                DataTypes.FIELD("c", DataTypes.STRING())); // value

                final int[] dataTypeProjection = new int[] {0, 2};

                final int[][] projectedFields =
                        new int[][] {
                            new int[] {0}, // value
                            new int[] {1}, // key
                            new int[] {2} // value
                        };

                final Decoder valueDecoder =
                        createDecoder(
                                format,
                                tableDataType,
                                dataTypeProjection,
                                projectedFields,
                                Collections.emptyList(),
                                pushProjectionsIntoDecodingFormat);

                assertThat(valueDecoder.getProjector().isEmptyProjection()).isFalse();
                assertThat(valueDecoder.getProjector().isProjectionNeeded()).isTrue();
            }
        }
    }
}
