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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the Kafka offsets extraction from savepoints. */
public class SavepointKafkaOffsetsTableFunctionTest {
    @Test
    public void testReadKafkaOffsets() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        tEnv.executeSql("LOAD MODULE state");
        TableResult res =
                tEnv.executeSql(
                        "SELECT * FROM savepoint_get_kafka_offsets('src/test/resources/table-state-kafka-offsets', 'kafka-source-uid')");

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = res.collect()) {
            it.forEachRemaining(rows::add);
        }

        rows.sort(Comparator.comparing(a -> ((Integer) a.getField("partition"))));

        assertThat(rows)
                .extracting(Row::toString)
                .containsExactly("+I[test1, 0, -2, null]", "+I[test1, 1, 1, null]");
    }
}
