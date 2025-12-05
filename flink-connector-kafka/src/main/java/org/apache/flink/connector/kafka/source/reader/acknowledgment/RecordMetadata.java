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

package org.apache.flink.connector.kafka.source.reader.acknowledgment;

import org.apache.flink.annotation.Internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Objects;

/**
 * Lightweight metadata for a Kafka record used in acknowledgment tracking.
 *
 * <p>This class stores only essential information needed to acknowledge a record to the Kafka share
 * group coordinator.
 *
 * <p><b>Current Implementation (Phase 2.1):</b> Stores a reference to the full ConsumerRecord to
 * work with the {@code ShareConsumer.acknowledge(ConsumerRecord, AcknowledgeType)} API. This
 * temporarily uses more memory (~1KB per record) than the metadata-only approach (~40 bytes).
 *
 * <p><b>Future Optimization (Phase 2.5):</b> Will use the 3-parameter {@code acknowledge(String
 * topic, int partition, long offset, AcknowledgeType)} API once available in the Kafka version,
 * eliminating the need to store full ConsumerRecords.
 *
 * <h2>Thread Safety</h2>
 *
 * This class is immutable and thread-safe.
 */
@Internal
public class RecordMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;

    /**
     * Full ConsumerRecord reference needed for acknowledgment. TODO (Phase 2.5): Remove this when
     * 3-parameter acknowledge() API is available.
     */
    private final transient ConsumerRecord<byte[], byte[]> consumerRecord;

    /**
     * Creates record metadata.
     *
     * @param topic Kafka topic name
     * @param partition partition number
     * @param offset record offset within partition
     * @param timestamp record timestamp
     * @param consumerRecord the full consumer record (for acknowledgment)
     */
    public RecordMetadata(
            String topic,
            int partition,
            long offset,
            long timestamp,
            ConsumerRecord<byte[], byte[]> consumerRecord) {
        this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.consumerRecord =
                Objects.requireNonNull(consumerRecord, "ConsumerRecord cannot be null");
    }

    /**
     * Creates metadata from a {@link ConsumerRecord}.
     *
     * <p>This method accepts ConsumerRecord with any key/value types and performs an unchecked cast
     * to byte[] types. This is safe because Kafka records are always stored as byte arrays
     * internally before deserialization.
     *
     * @param record the Kafka consumer record
     * @return lightweight metadata for the record
     */
    @SuppressWarnings("unchecked")
    public static RecordMetadata from(ConsumerRecord<?, ?> record) {
        // Safe cast: Kafka records are always byte[] before deserialization
        ConsumerRecord<byte[], byte[]> byteRecord = (ConsumerRecord<byte[], byte[]>) record;
        return new RecordMetadata(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                byteRecord // Store full record for acknowledgment
                );
    }

    /**
     * Gets the ConsumerRecord for acknowledgment operations.
     *
     * @return the consumer record
     */
    public ConsumerRecord<byte[], byte[]> getConsumerRecord() {
        return consumerRecord;
    }

    /** Gets the topic name. */
    public String getTopic() {
        return topic;
    }

    /** Gets the partition number. */
    public int getPartition() {
        return partition;
    }

    /** Gets the record offset. */
    public long getOffset() {
        return offset;
    }

    /** Gets the record timestamp. */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Estimates the memory size of this metadata in bytes.
     *
     * <p><b>Note:</b> Current implementation stores full ConsumerRecord reference, so memory usage
     * is approximately 1KB per record (size of ConsumerRecord). Future optimizations (Phase 2.5)
     * will reduce this to ~40 bytes.
     *
     * @return approximate memory size
     */
    public int estimateSize() {
        // Currently stores full ConsumerRecord: ~1KB
        // Future optimization: just metadata (topic string + primitives): ~40 bytes
        return 1024; // Approximate size of ConsumerRecord with typical payload
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecordMetadata)) return false;
        RecordMetadata that = (RecordMetadata) o;
        return partition == that.partition
                && offset == that.offset
                && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
        return String.format(
                "RecordMetadata{topic='%s', partition=%d, offset=%d, timestamp=%d}",
                topic, partition, offset, timestamp);
    }
}
