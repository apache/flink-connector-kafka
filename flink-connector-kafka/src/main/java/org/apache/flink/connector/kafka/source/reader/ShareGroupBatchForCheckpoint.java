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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores a batch of Kafka records fetched from share consumer with their processing state.
 * Used for checkpoint persistence to ensure crash recovery and at-least-once processing.
 */
public class ShareGroupBatchForCheckpoint<K, V> implements Serializable {
    
    private long checkpointId;
    private final long batchId;
    private final List<ConsumerRecord<K, V>> records;
    private final Map<String, RecordProcessingState> recordStates;
    private final long batchTimestamp;

    public ShareGroupBatchForCheckpoint(long batchId, List<ConsumerRecord<K, V>> records) {
        this.batchId = batchId;
        this.records = records;
        this.batchTimestamp = System.currentTimeMillis();
        this.recordStates = new HashMap<>();
        
        // Initialize processing state for all records
        for (ConsumerRecord<K, V> record : records) {
            String recordKey = createRecordKey(record);
            recordStates.put(recordKey, new RecordProcessingState());
        }
    }

    /**
     * Processing state for individual records within the batch.
     */
    public static class RecordProcessingState implements Serializable {
        private boolean emittedDownstream = false;
        private boolean reachedSink = false;

        public boolean isEmittedDownstream() {
            return emittedDownstream;
        }

        public void setEmittedDownstream(boolean emittedDownstream) {
            this.emittedDownstream = emittedDownstream;
        }

        public boolean isReachedSink() {
            return reachedSink;
        }

        public void setReachedSink(boolean reachedSink) {
            this.reachedSink = reachedSink;
        }
    }

    private String createRecordKey(ConsumerRecord<K, V> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    public RecordProcessingState getRecordState(ConsumerRecord<K, V> record) {
        return recordStates.get(createRecordKey(record));
    }

    public boolean allRecordsReachedSink() {
        return recordStates.values().stream().allMatch(RecordProcessingState::isReachedSink);
    }

    public void markAllRecordsReachedSink() {
        recordStates.values().forEach(state -> state.setReachedSink(true));
    }

    // Getters and setters
    public long getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getBatchId() {
        return batchId;
    }

    public List<ConsumerRecord<K, V>> getRecords() {
        return records;
    }

    public long getBatchTimestamp() {
        return batchTimestamp;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int size() {
        return records.size();
    }
}