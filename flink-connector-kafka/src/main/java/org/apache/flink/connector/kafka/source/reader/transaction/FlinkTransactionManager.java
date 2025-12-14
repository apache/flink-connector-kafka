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

package org.apache.flink.connector.kafka.source.reader.transaction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.source.reader.acknowledgment.RecordMetadata;

import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates acknowledgments with Flink checkpoint lifecycle for at-least-once semantics.
 *
 * Two-phase commit coordinated with Flink checkpoints:
 * - Phase 1 (snapshotState): Buffer acks locally, records stay locked at broker
 * - Phase 2 (notifyCheckpointComplete): Send acks via commitSync(), uses Kafka's built-in 2PC
 *
 * At-least-once guarantee:
 * - Records stay IN_FLIGHT (locked) at broker until checkpoint completes
 * - If checkpoint fails: locks timeout → records automatically redelivered
 * - If checkpoint succeeds: commitSync() atomically acknowledges records
 *
 * Note: Kafka's built-in commitSync() handles PREPARED→COMMITTED atomically (milliseconds).
 * This manager coordinates the TIMING of commitSync() with Flink's checkpoint lifecycle.
 */
@Internal
public class FlinkTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkTransactionManager.class);

    private final String shareGroupId;
    private ShareConsumer<byte[], byte[]> shareConsumer;
    private final Map<Long, TransactionState> checkpointTransactions;
    private final Map<Long, Set<RecordMetadata>> readyForAcknowledgment;

    public FlinkTransactionManager(String shareGroupId, ShareConsumer<byte[], byte[]> shareConsumer) {
        this.shareGroupId = shareGroupId;
        this.shareConsumer = shareConsumer;
        this.checkpointTransactions = new ConcurrentHashMap<>();
        this.readyForAcknowledgment = new ConcurrentHashMap<>();
    }

    /**
     * Update share consumer reference (for lazy initialization).
     */
    public void setShareConsumer(ShareConsumer<byte[], byte[]> shareConsumer) {
        this.shareConsumer = shareConsumer;
    }

    /**
     * Mark acknowledgments ready (Phase 1).
     * Stores records locally - does NOT send to broker yet.
     * Records remain locked (IN_FLIGHT) at broker until commitTransaction().
     */
    public void markReadyForAcknowledgment(long checkpointId, Set<RecordMetadata> records) {
        if (records.isEmpty()) {
            LOG.debug("Share group '{}': No records to mark for checkpoint {}",
                shareGroupId, checkpointId);
            return;
        }

        LOG.info("Share group '{}': Marking {} records ready for checkpoint {} (NOT sending to broker yet)",
            shareGroupId, records.size(), checkpointId);

        readyForAcknowledgment.put(checkpointId, records);
        checkpointTransactions.put(checkpointId, TransactionState.READY);
    }

    /**
     * Commit transaction (Phase 2).
     * Sends acks to broker using Kafka's built-in atomic commitSync().
     * Kafka internally: acknowledge() marks PREPARED, commitSync() applies atomically.
     */
    public void commitTransaction(long checkpointId) throws Exception {
        Set<RecordMetadata> records = readyForAcknowledgment.remove(checkpointId);

        if (records == null || records.isEmpty()) {
            LOG.debug("Share group '{}': No records to commit for checkpoint {}",
                shareGroupId, checkpointId);
            checkpointTransactions.remove(checkpointId);
            return;
        }

        TransactionState state = checkpointTransactions.get(checkpointId);
        if (state != TransactionState.READY) {
            LOG.warn("Share group '{}': Cannot commit checkpoint {} in state {}",
                shareGroupId, checkpointId, state);
            return;
        }

        LOG.info("Share group '{}': Committing {} records for checkpoint {}",
            shareGroupId, records.size(), checkpointId);

        try {
            // Send acknowledgments using Kafka's built-in atomic commit
            Map<TopicPartition, java.util.List<RecordMetadata>> byPartition = new ConcurrentHashMap<>();
            for (RecordMetadata meta : records) {
                TopicPartition tp = new TopicPartition(meta.getTopic(), meta.getPartition());
                byPartition.computeIfAbsent(tp, k -> new java.util.ArrayList<>()).add(meta);
            }

            for (Map.Entry<TopicPartition, java.util.List<RecordMetadata>> entry : byPartition.entrySet()) {
                for (RecordMetadata meta : entry.getValue()) {
                    shareConsumer.acknowledge(
                        meta.getConsumerRecord(),
                        org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT
                    );
                }
            }

            // commitSync() atomically applies all acknowledgments at broker
            shareConsumer.commitSync(Duration.ofSeconds(30));

            checkpointTransactions.put(checkpointId, TransactionState.COMMITTED);
            cleanupOldTransactions(checkpointId);

            LOG.info("Share group '{}': Successfully committed checkpoint {}",
                shareGroupId, checkpointId);

        } catch (Exception e) {
            LOG.error("Share group '{}': Failed to commit checkpoint {}",
                shareGroupId, checkpointId, e);
            checkpointTransactions.put(checkpointId, TransactionState.FAILED);
            throw e;
        }
    }

    /**
     * Abort transaction - releases record locks for redelivery.
     */
    public void abortTransaction(long checkpointId, Set<RecordMetadata> records) {
        LOG.info("Share group '{}': Aborting checkpoint {}", shareGroupId, checkpointId);

        try {
            for (RecordMetadata meta : records) {
                shareConsumer.acknowledge(
                    meta.getConsumerRecord(),
                    org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE
                );
            }

            shareConsumer.commitSync(Duration.ofSeconds(10));
            checkpointTransactions.put(checkpointId, TransactionState.ABORTED);

        } catch (Exception e) {
            LOG.error("Share group '{}': Failed to abort checkpoint {}",
                shareGroupId, checkpointId, e);
        }

        cleanupOldTransactions(checkpointId);
    }

    /**
     * Recovery is handled automatically by Kafka's lock timeout mechanism.
     * If task fails, locks expire and records are redelivered - no explicit action needed.
     */
    public void recoverFromCheckpoint(long restoredCheckpointId) {
        LOG.info("Share group '{}': Recovering from checkpoint {} - relying on Kafka lock timeout for redelivery",
            shareGroupId, restoredCheckpointId);
    }

    private void cleanupOldTransactions(long completedCheckpointId) {
        // Remove transactions older than completed checkpoint
        checkpointTransactions.entrySet().removeIf(entry ->
            entry.getKey() < completedCheckpointId);
    }

    private enum TransactionState {
        READY,
        COMMITTED,
        ABORTED,
        FAILED
    }
}
