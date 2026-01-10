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
import org.apache.flink.connector.kafka.source.split.ShareGroupSubscriptionState;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Record emitter for Kafka share group records with acknowledgment tracking integration.
 *
 * <h2>Key Responsibilities</h2>
 *
 * <ul>
 *   <li>Deserializes Kafka ConsumerRecords into output type T
 *   <li>Emits records to Flink's data pipeline
 *   <li>Does NOT track offsets (share group coordinator handles state)
 *   <li>Integrates with KafkaShareGroupSourceReader for acknowledgment tracking
 * </ul>
 *
 * <h2>Acknowledgment Flow Integration</h2>
 *
 * <p>This emitter works in conjunction with {@link KafkaShareGroupSourceReader}:
 *
 * <ol>
 *   <li>Emitter receives ConsumerRecord from split reader
 *   <li>Deserializes record using provided schema
 *   <li>Emits record to Flink pipeline
 *   <li>Source reader (not emitter) stores RecordMetadata for acknowledgment
 * </ol>
 *
 * <p><b>Note:</b> Unlike traditional Kafka emitters that track offsets in split state, this emitter
 * doesn't modify split state. The share group coordinator tracks all delivery state on the broker
 * side.
 *
 * @param <T> The type of records produced after deserialization
 * @see KafkaShareGroupSourceReader
 * @see ShareGroupSubscriptionState
 */
@Internal
public class KafkaShareGroupRecordEmitter<T>
        implements RecordEmitter<ConsumerRecord<byte[], byte[]>, T, ShareGroupSubscriptionState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupRecordEmitter.class);

    /**
     * Callback interface for notifying about emitted records. Used by the source reader to track
     * records for acknowledgment.
     */
    @FunctionalInterface
    public interface RecordEmittedCallback {
        void onRecordEmitted(ConsumerRecord<?, ?> record);
    }

    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();
    private final RecordEmittedCallback emittedCallback;

    /**
     * Creates a record emitter with the given deserialization schema and callback.
     *
     * @param deserializationSchema schema for deserializing Kafka records
     * @param emittedCallback callback invoked after each record is emitted (can be null)
     */
    public KafkaShareGroupRecordEmitter(
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            RecordEmittedCallback emittedCallback) {
        this.deserializationSchema = deserializationSchema;
        this.emittedCallback = emittedCallback;
    }

    /**
     * Creates a record emitter with the given deserialization schema (no callback).
     *
     * @param deserializationSchema schema for deserializing Kafka records
     */
    public KafkaShareGroupRecordEmitter(KafkaRecordDeserializationSchema<T> deserializationSchema) {
        this(deserializationSchema, null);
    }

    /**
     * Emits a deserialized record to the Flink data pipeline.
     *
     * <p>This method:
     *
     * <ol>
     *   <li>Deserializes the Kafka ConsumerRecord
     *   <li>Emits to SourceOutput with preserved timestamp
     *   <li>Does NOT modify split state (share groups don't track offsets)
     *   <li>The calling SourceReader handles acknowledgment tracking
     * </ol>
     *
     * @param consumerRecord the Kafka record to emit
     * @param output the Flink source output to emit to
     * @param subscriptionState the subscription state (not modified)
     * @throws Exception if deserialization fails
     */
    @Override
    public void emitRecord(
            ConsumerRecord<byte[], byte[]> consumerRecord,
            SourceOutput<T> output,
            ShareGroupSubscriptionState subscriptionState)
            throws Exception {

        try {
            // Prepare wrapper with output and timestamp
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(consumerRecord.timestamp());

            // Deserialize and emit record
            deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper);

            // Notify callback about emitted record for acknowledgment tracking
            if (emittedCallback != null) {
                emittedCallback.onRecordEmitted(consumerRecord);
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Emitted record from share group '{}' - topic: {}, partition: {}, offset: {}",
                        subscriptionState.getShareGroupId(),
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset());
            }

            // Note: Acknowledgment tracking is handled by KafkaShareGroupSourceReader
            // via the emittedCallback provided during construction

        } catch (Exception e) {
            LOG.error(
                    "Failed to deserialize record from share group '{}' - topic: {}, partition: {}, offset: {}: {}",
                    subscriptionState.getShareGroupId(),
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    e.getMessage(),
                    e);
            throw new IOException(
                    "Failed to deserialize consumer record from share group: "
                            + subscriptionState.getShareGroupId(),
                    e);
        }
    }

    // ===========================================================================================
    // SourceOutput Wrapper
    // ===========================================================================================

    /**
     * Collector adapter that bridges Flink's Collector interface with SourceOutput.
     *
     * <p>This wrapper allows the deserialization schema (which uses Collector) to emit records to
     * Flink's SourceOutput (which requires explicit timestamps).
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
            // No-op for SourceOutput - lifecycle managed by framework
        }

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
