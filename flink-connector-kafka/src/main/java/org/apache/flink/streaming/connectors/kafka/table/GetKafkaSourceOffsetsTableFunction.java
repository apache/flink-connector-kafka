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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.functions.FunctionKind.TABLE;

/**
 * Table function to extract Kafka source offsets from savepoints.
 *
 * <p>This function reads Kafka partition splits from a savepoint and extracts the offset
 * information for each partition. The function is designed to work with the Flink state processing
 * API and provides visibility into the Kafka source state.
 */
@Internal
@FunctionHint(
        output =
                @DataTypeHint(
                        "ROW<topic STRING NOT NULL, "
                                + "partition INT NOT NULL, "
                                + "starting-offset BIGINT NOT NULL, "
                                + "stopping-offset BIGINT NULL>"))
public class GetKafkaSourceOffsetsTableFunction extends TableFunction<Row> {

    public static final BuiltInFunctionDefinition FUNCTION_DEFINITION =
            BuiltInFunctionDefinition.newBuilder()
                    .name("savepoint_get_kafka_offsets")
                    .kind(TABLE)
                    .runtimeClass(GetKafkaSourceOffsetsTableFunction.class.getName())
                    .outputTypeStrategy(
                            TypeStrategies.explicit(
                                    DataTypes.ROW(
                                            DataTypes.FIELD("topic", DataTypes.STRING().notNull()),
                                            DataTypes.FIELD("partition", DataTypes.INT().notNull()),
                                            DataTypes.FIELD(
                                                    "starting-offset",
                                                    DataTypes.BIGINT().notNull()),
                                            DataTypes.FIELD(
                                                    "stopping-offset",
                                                    DataTypes.BIGINT().nullable()))))
                    .build();

    public GetKafkaSourceOffsetsTableFunction(SpecializedFunction.SpecializedContext context) {}

    public void eval(String savepointPath, String operatorUid) {
        try {
            CheckpointMetadata checkpointMetadata =
                    SavepointLoader.loadSavepointMetadata(savepointPath);

            OperatorIdentifier operatorIdentifier = OperatorIdentifier.forUid(operatorUid);
            OperatorState operatorState =
                    checkpointMetadata.getOperatorStates().stream()
                            .filter(
                                    os ->
                                            os.getOperatorID()
                                                    .equals(operatorIdentifier.getOperatorId()))
                            .findFirst()
                            .orElseThrow();
            List<OperatorStateHandle> stateHandles =
                    operatorState.getStates().stream()
                            .map(OperatorSubtaskState::getManagedOperatorState)
                            .collect(ArrayList::new, List::addAll, List::addAll);
            CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
            try (DefaultOperatorStateBackend stateBackend =
                    new DefaultOperatorStateBackendBuilder(
                                    Thread.currentThread().getContextClassLoader(),
                                    new ExecutionConfig(),
                                    false,
                                    stateHandles,
                                    cancelStreamRegistry)
                            .build()) {
                KafkaPartitionSplitSerializer serializer = new KafkaPartitionSplitSerializer();
                ListState<?> listState =
                        stateBackend.getListState(
                                new ListStateDescriptor<>(
                                        "SourceReaderState", Types.PRIMITIVE_ARRAY(Types.BYTE)));

                StreamSupport.stream(listState.get().spliterator(), false)
                        .forEach(
                                (value) -> {
                                    try (ByteArrayInputStream bais =
                                                    new ByteArrayInputStream((byte[]) value);
                                            DataInputStream in =
                                                    new DataInputViewStreamWrapper(bais)) {
                                        int version = in.readInt();
                                        int numBytes = in.readInt();
                                        byte[] data = new byte[numBytes];
                                        in.readFully(data);
                                        KafkaPartitionSplit split =
                                                serializer.deserialize(version, data);

                                        Row row = Row.withNames();
                                        row.setField("topic", split.getTopic());
                                        row.setField("partition", split.getPartition());
                                        row.setField("starting-offset", split.getStartingOffset());
                                        row.setField(
                                                "stopping-offset",
                                                split.getStoppingOffset().orElse(null));
                                        collect(row);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
