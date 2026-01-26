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

import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.TestFormatFactory;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for {@link DynamicKafkaTableFactory}. */
class DynamicKafkaTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("name", DataTypes.STRING().notNull()),
                            Column.physical("count", DataTypes.INT())),
                    Collections.emptyList(),
                    null);

    @Test
    void testTableSourceWithSingleClusterMetadataService() {
        final DynamicTableSource actualSource =
                createTableSource(SCHEMA, getSingleClusterSourceOptions());
        assertThat(actualSource).isInstanceOf(DynamicKafkaTableSource.class);

        final DynamicKafkaTableSource actualKafkaSource = (DynamicKafkaTableSource) actualSource;
        assertThat(actualKafkaSource.streamIds).containsExactly("stream-1", "stream-2");
        assertThat(actualKafkaSource.streamPattern).isNull();
        assertThat(actualKafkaSource.startupMode).isEqualTo(StartupMode.EARLIEST);
        assertThat(actualKafkaSource.boundedMode).isEqualTo(BoundedMode.UNBOUNDED);
        assertThat(actualKafkaSource.properties.getProperty("bootstrap.servers"))
                .isEqualTo("bootstrap-single:9092");
        assertThat(actualKafkaSource.properties.getProperty("partition.discovery.interval.ms"))
                .isEqualTo("1000");
    }

    @Test
    void testInvalidStreamOptions() {
        final Map<String, String> options = getSingleClusterSourceOptions();
        options.put("stream-pattern", "stream-.*");

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSource(SCHEMA, options))
                .withMessageContaining("stream-ids")
                .withMessageContaining("stream-pattern");
    }

    private static Map<String, String> getSingleClusterSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Dynamic Kafka specific options.
        tableOptions.put("connector", DynamicKafkaTableFactory.IDENTIFIER);
        tableOptions.put("stream-ids", "stream-1;stream-2");
        tableOptions.put("metadata-service", "single-cluster");
        tableOptions.put("metadata-service.cluster-id", "cluster-single");
        tableOptions.put("properties.bootstrap.servers", "bootstrap-single:9092");
        tableOptions.put("properties.group.id", "dummy");
        tableOptions.put("scan.startup.mode", "earliest-offset");
        tableOptions.put("scan.topic-partition-discovery.interval", "1000 ms");
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey =
                String.format(
                        "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        final String failOnMissingKey =
                String.format(
                        "%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
        tableOptions.put(formatDelimiterKey, ",");
        tableOptions.put(failOnMissingKey, "true");
        return tableOptions;
    }
}
