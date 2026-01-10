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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe buffer for pending acknowledgments using the checkpoint-subsuming pattern.
 *
 * <p>This buffer stores lightweight record metadata (not full records) and follows Flink's
 * checkpoint subsuming contract:
 *
 * <ul>
 *   <li>Checkpoint IDs are strictly increasing
 *   <li>Higher checkpoint ID subsumes all lower IDs
 *   <li>Once checkpoint N completes, checkpoints < N will never complete
 * </ul>
 *
 * <h2>Implementation Pattern</h2>
 *
 * <pre>{@code
 * // On record fetched:
 * buffer.addRecord(currentCheckpointId, record);
 *
 * // On checkpoint complete:
 * Set<RecordMetadata> toAck = buffer.getRecordsUpTo(checkpointId);
 * acknowledgeToKafka(toAck);
 * buffer.removeUpTo(checkpointId);
 * }</pre>
 *
 * <h2>Memory Management</h2>
 *
 * Stores only metadata (~40 bytes per record) instead of full ConsumerRecords. For 100,000 records
 * at 1KB each:
 *
 * <ul>
 *   <li>Full records: ~100 MB
 *   <li>Metadata only: ~4 MB (25x savings)
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * Uses {@link ConcurrentSkipListMap} for lock-free concurrent access. All public methods are
 * thread-safe.
 */
@Internal
public class AcknowledgmentBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgmentBuffer.class);

    // Checkpoint ID → Set of record metadata
    private final ConcurrentNavigableMap<Long, Set<RecordMetadata>> pendingAcknowledgments;

    // Statistics for monitoring
    private final AtomicInteger totalRecordsBuffered;
    private final AtomicLong oldestCheckpointId;
    private final AtomicLong newestCheckpointId;

    /** Creates a new acknowledgment buffer. */
    public AcknowledgmentBuffer() {
        this.pendingAcknowledgments = new ConcurrentSkipListMap<>();
        this.totalRecordsBuffered = new AtomicInteger(0);
        this.oldestCheckpointId = new AtomicLong(-1);
        this.newestCheckpointId = new AtomicLong(-1);
    }

    /**
     * Adds a record to the buffer for the given checkpoint.
     *
     * <p>This should be called immediately after fetching a record from Kafka, using the current
     * checkpoint ID.
     *
     * @param checkpointId the checkpoint ID to associate with this record
     * @param record the Kafka consumer record
     */
    public void addRecord(long checkpointId, ConsumerRecord<?, ?> record) {
        RecordMetadata metadata = RecordMetadata.from(record);

        pendingAcknowledgments
                .computeIfAbsent(checkpointId, k -> Collections.synchronizedSet(new HashSet<>()))
                .add(metadata);

        totalRecordsBuffered.incrementAndGet();

        // Update checkpoint bounds
        oldestCheckpointId.compareAndSet(-1, checkpointId);
        newestCheckpointId.updateAndGet(current -> Math.max(current, checkpointId));

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Added record to buffer: checkpoint={}, topic={}, partition={}, offset={}",
                    checkpointId,
                    record.topic(),
                    record.partition(),
                    record.offset());
        }
    }

    /**
     * Gets all record metadata up to and including the given checkpoint ID.
     *
     * <p>This implements the checkpoint subsuming pattern: when checkpoint N completes, we
     * acknowledge all records from checkpoints ≤ N.
     *
     * @param checkpointId the checkpoint ID (inclusive upper bound)
     * @return set of all record metadata up to this checkpoint
     */
    public Set<RecordMetadata> getRecordsUpTo(long checkpointId) {
        Set<RecordMetadata> result = new HashSet<>();

        // Get all checkpoints <= checkpointId
        for (Map.Entry<Long, Set<RecordMetadata>> entry :
                pendingAcknowledgments.headMap(checkpointId, true).entrySet()) {
            result.addAll(entry.getValue());
        }

        LOG.debug(
                "Retrieved {} records for acknowledgment up to checkpoint {}",
                result.size(),
                checkpointId);

        return result;
    }

    /**
     * Removes all record metadata up to and including the given checkpoint ID.
     *
     * <p>This should be called after successfully acknowledging records to Kafka.
     *
     * @param checkpointId the checkpoint ID (inclusive upper bound)
     * @return number of records removed
     */
    public int removeUpTo(long checkpointId) {
        // Get submap of checkpoints <= checkpointId
        Map<Long, Set<RecordMetadata>> removed =
                new HashMap<>(pendingAcknowledgments.headMap(checkpointId, true));

        // Count records before removal
        int removedCount = 0;
        for (Set<RecordMetadata> records : removed.values()) {
            removedCount += records.size();
        }

        // Remove from the concurrent map
        pendingAcknowledgments.headMap(checkpointId, true).clear();

        // Update statistics
        totalRecordsBuffered.addAndGet(-removedCount);

        // Update oldest checkpoint
        if (pendingAcknowledgments.isEmpty()) {
            oldestCheckpointId.set(-1);
            newestCheckpointId.set(-1);
        } else {
            oldestCheckpointId.set(pendingAcknowledgments.firstKey());
        }

        LOG.debug("Removed {} records from buffer up to checkpoint {}", removedCount, checkpointId);

        return removedCount;
    }

    /**
     * Gets the total number of buffered records across all checkpoints.
     *
     * @return total buffered record count
     */
    public int size() {
        return totalRecordsBuffered.get();
    }

    /**
     * Gets the number of checkpoints currently buffered.
     *
     * @return number of distinct checkpoints with pending records
     */
    public int checkpointCount() {
        return pendingAcknowledgments.size();
    }

    /**
     * Gets the oldest checkpoint ID in the buffer.
     *
     * @return oldest checkpoint ID, or -1 if buffer is empty
     */
    public long getOldestCheckpointId() {
        return oldestCheckpointId.get();
    }

    /**
     * Gets the newest checkpoint ID in the buffer.
     *
     * @return newest checkpoint ID, or -1 if buffer is empty
     */
    public long getNewestCheckpointId() {
        return newestCheckpointId.get();
    }

    /**
     * Gets the estimated memory usage in bytes.
     *
     * <p>This is an approximation based on record metadata size.
     *
     * @return estimated memory usage in bytes
     */
    public long estimateMemoryUsage() {
        long totalBytes = 0;
        for (Set<RecordMetadata> records : pendingAcknowledgments.values()) {
            for (RecordMetadata metadata : records) {
                totalBytes += metadata.estimateSize();
            }
        }
        return totalBytes;
    }

    /**
     * Clears all buffered records.
     *
     * <p>This should only be called when closing the reader or resetting state.
     */
    public void clear() {
        pendingAcknowledgments.clear();
        totalRecordsBuffered.set(0);
        oldestCheckpointId.set(-1);
        newestCheckpointId.set(-1);

        LOG.info("Cleared acknowledgment buffer");
    }

    /**
     * Gets buffer statistics for monitoring.
     *
     * @return statistics snapshot
     */
    public BufferStatistics getStatistics() {
        return new BufferStatistics(
                totalRecordsBuffered.get(),
                pendingAcknowledgments.size(),
                oldestCheckpointId.get(),
                newestCheckpointId.get(),
                estimateMemoryUsage());
    }

    /** Snapshot of buffer statistics. */
    public static class BufferStatistics {
        private final int totalRecords;
        private final int checkpointCount;
        private final long oldestCheckpointId;
        private final long newestCheckpointId;
        private final long memoryUsageBytes;

        public BufferStatistics(
                int totalRecords,
                int checkpointCount,
                long oldestCheckpointId,
                long newestCheckpointId,
                long memoryUsageBytes) {
            this.totalRecords = totalRecords;
            this.checkpointCount = checkpointCount;
            this.oldestCheckpointId = oldestCheckpointId;
            this.newestCheckpointId = newestCheckpointId;
            this.memoryUsageBytes = memoryUsageBytes;
        }

        public int getTotalRecords() {
            return totalRecords;
        }

        public int getCheckpointCount() {
            return checkpointCount;
        }

        public long getOldestCheckpointId() {
            return oldestCheckpointId;
        }

        public long getNewestCheckpointId() {
            return newestCheckpointId;
        }

        public long getMemoryUsageBytes() {
            return memoryUsageBytes;
        }

        @Override
        public String toString() {
            return String.format(
                    "BufferStatistics{records=%d, checkpoints=%d, oldestCp=%d, newestCp=%d, memory=%d bytes}",
                    totalRecords,
                    checkpointCount,
                    oldestCheckpointId,
                    newestCheckpointId,
                    memoryUsageBytes);
        }
    }
}
