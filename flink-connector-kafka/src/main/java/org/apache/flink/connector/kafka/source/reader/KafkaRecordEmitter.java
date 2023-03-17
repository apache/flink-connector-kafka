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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordEvaluator;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** The {@link RecordEmitter} implementation for {@link KafkaSourceReader}. */
@Internal
public class KafkaRecordEmitter<T>
        implements RecordEmitter<ConsumerRecord<byte[], byte[]>, T, KafkaPartitionSplitState> {

    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper;
    @Nullable private final RecordEvaluator<T> eofRecordEvaluator;
    private final Set<String> finishedSplits;

    public KafkaRecordEmitter(
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            @Nullable RecordEvaluator<T> eofRecordEvaluator) {
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<>(eofRecordEvaluator);
        this.eofRecordEvaluator = eofRecordEvaluator;
        this.finishedSplits = new HashSet<>();
    }

    @Override
    public void emitRecord(
            ConsumerRecord<byte[], byte[]> consumerRecord,
            SourceOutput<T> output,
            KafkaPartitionSplitState splitState)
            throws Exception {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(consumerRecord.timestamp());
            deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper);

            if (sourceOutputWrapper.isEofRecord()) {
                finishedSplits.add(splitState.splitId());
            }
            if (eofRecordEvaluator == null || !finishedSplits.contains(splitState.splitId())) {
                splitState.setCurrentOffset(consumerRecord.offset() + 1);
            }
        } catch (Exception e) {
            throw new IOException("Failed to deserialize consumer record due to", e);
        }
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        @Nullable private final RecordEvaluator<T> eofRecordEvaluator;

        private SourceOutput<T> sourceOutput;
        private long timestamp;
        private boolean isEofRecord = false;

        public SourceOutputWrapper(@Nullable RecordEvaluator<T> eofRecordEvaluator) {
            this.eofRecordEvaluator = eofRecordEvaluator;
        }

        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
            if (eofRecordEvaluator != null) {
                isEofRecord = eofRecordEvaluator.isEndOfStream(record);
            }
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        /** Whether the previous sent record is an eof record. */
        public boolean isEofRecord() {
            return isEofRecord;
        }
    }
}
