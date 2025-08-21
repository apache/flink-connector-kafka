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
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplitState;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Record emitter for Kafka share group records that handles deserialization.
 * 
 * <p>This emitter integrates with Flink's connector architecture to deserialize
 * and emit Kafka records from share group consumers. Unlike regular Kafka emitters,
 * this doesn't track offsets since the share group coordinator handles message delivery state.
 */
@Internal
public class KafkaShareGroupRecordEmitter<T> implements RecordEmitter<ConsumerRecord<byte[], byte[]>, T, KafkaShareGroupSplitState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupRecordEmitter.class);
    
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();
    
    /**
     * Creates a record emitter with the given deserialization schema.
     */
    public KafkaShareGroupRecordEmitter(KafkaRecordDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }
    
    @Override
    public void emitRecord(
            ConsumerRecord<byte[], byte[]> consumerRecord,
            SourceOutput<T> output,
            KafkaShareGroupSplitState splitState) throws Exception {
        
        try {
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(consumerRecord.timestamp());
            deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper);
            
            LOG.trace("Successfully emitted record from share group split: {} (topic: {}, partition: {}, offset: {})",
                    splitState.getSplitId(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                    
        } catch (Exception e) {
            LOG.error("Failed to deserialize record from share group split: {} (topic: {}, partition: {}, offset: {}): {}",
                    splitState.getSplitId(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e.getMessage(), e);
            throw new IOException("Failed to deserialize consumer record from share group", e);
        }
    }
    
    /**
     * Collector adapter that bridges Flink's Collector interface with SourceOutput.
     */
    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;
        private long timestamp;
        
        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
        }
        
        @Override
        public void close() {
            // No-op for SourceOutput
        }
        
        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }
        
        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}