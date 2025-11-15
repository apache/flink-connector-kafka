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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.reader.acknowledgment.AcknowledgmentBuffer;
import org.apache.flink.connector.kafka.source.reader.acknowledgment.RecordMetadata;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaShareGroupFetcherManager;
import org.apache.flink.connector.kafka.source.reader.transaction.FlinkTransactionManager;
import org.apache.flink.connector.kafka.source.split.ShareGroupSubscriptionState;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Source reader for Kafka share groups implementing the CheckpointListener pattern.
 *
 * <h2>Architecture Overview</h2>
 *
 * <p>This reader implements a fundamentally different pattern than traditional Kafka
 * partition-based sources. Instead of managing partition assignments and offsets, it leverages
 * Kafka 4.1's share groups which provide message-level distribution managed by the broker's share
 * group coordinator.
 *
 * <h2>Key Differences from Traditional Kafka Source</h2>
 *
 * <table border="1">
 * <tr>
 *   <th>Aspect</th>
 *   <th>Traditional KafkaSourceReader</th>
 *   <th>KafkaShareGroupSourceReader</th>
 * </tr>
 * <tr>
 *   <td>Split Type</td>
 *   <td>Partition-based splits (KafkaPartitionSplit)</td>
 *   <td>Subscription-based (ShareGroupSubscriptionState)</td>
 * </tr>
 * <tr>
 *   <td>Assignment</td>
 *   <td>Enumerator assigns partitions to readers</td>
 *   <td>Broker coordinator distributes messages</td>
 * </tr>
 * <tr>
 *   <td>Checkpoint Storage</td>
 *   <td>Partition offsets</td>
 *   <td>Minimal subscription state only</td>
 * </tr>
 * <tr>
 *   <td>Acknowledgment</td>
 *   <td>Offset commits (implicit)</td>
 *   <td>Explicit per-message acknowledgments</td>
 * </tr>
 * <tr>
 *   <td>Memory Usage</td>
 *   <td>No buffering needed (offsets only)</td>
 *   <td>Metadata-only buffer (~40 bytes/record)</td>
 * </tr>
 * </table>
 *
 * <h2>Checkpoint-Acknowledgment Flow</h2>
 *
 * <pre>{@code
 * 1. poll() → Fetch records from Kafka share consumer
 * 2. emit() → Emit records to Flink pipeline
 * 3. addRecord() → Store RecordMetadata in AcknowledgmentBuffer[currentCheckpointId]
 * 4. snapshotState(N) → Return minimal ShareGroupSubscriptionState
 * 5. notifyCheckpointComplete(N) →
 *    a. Get all records from buffer up to checkpoint N (checkpoint subsuming)
 *    b. Call shareConsumer.acknowledge() for each record
 *    c. Call shareConsumer.commitSync() to commit to broker
 *    d. Remove acknowledged records from buffer
 * }</pre>
 *
 * <h2>At-Least-Once Guarantee</h2>
 *
 * <p>The at-least-once guarantee is provided through:
 *
 * <ul>
 *   <li>Records are only acknowledged to Kafka AFTER checkpoint completes successfully
 *   <li>If checkpoint fails, acquisition lock expires (default 30s) → broker redelivers messages
 *   <li>If task fails before acknowledgment, messages are redelivered to any available consumer
 *   <li>Checkpoint subsuming ensures no acknowledgment is lost even if notifications are missed
 * </ul>
 *
 * <h2>Memory Management</h2>
 *
 * <p>Uses {@link AcknowledgmentBuffer} to store only lightweight {@link RecordMetadata} (~40 bytes)
 * instead of full {@link ConsumerRecord} objects (typically 1KB+). For 100,000 pending records:
 *
 * <ul>
 *   <li>Full records: ~100 MB memory
 *   <li>Metadata only: ~4 MB memory (25x reduction)
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This reader runs in Flink's source reader thread. The {@link AcknowledgmentBuffer} is
 * thread-safe for concurrent access, but typically only accessed from the reader thread.
 *
 * @param <T> The type of records produced by this source reader after deserialization
 * @see CheckpointListener
 * @see AcknowledgmentBuffer
 * @see ShareGroupSubscriptionState
 */
@Internal
public class KafkaShareGroupSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                ConsumerRecord<byte[], byte[]>,
                T,
                ShareGroupSubscriptionState,
                ShareGroupSubscriptionState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSourceReader.class);

    /** Default timeout for commitSync operations */
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(30);

    /** Deserialization schema for transforming Kafka records into output type T */
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;

    /** Metrics collector for share group operations */
    private final KafkaShareGroupSourceMetrics shareGroupMetrics;

    /** Share group ID for this consumer */
    private final String shareGroupId;

    /**
     * Buffer storing lightweight RecordMetadata organized by checkpoint ID. Implements the
     * checkpoint-subsuming pattern for reliable acknowledgment.
     */
    private final AcknowledgmentBuffer acknowledgmentBuffer;

    /** Transaction manager for 2PC acknowledgments (Phase 1: prepare, Phase 2: commit) */
    private final FlinkTransactionManager transactionManager;

    /**
     * Reference to the Kafka 4.1 ShareConsumer for acknowledgment operations. Obtained from the
     * fetcher manager.
     */
    private final AtomicReference<ShareConsumer<byte[], byte[]>> shareConsumerRef;

    /** Current checkpoint ID being processed */
    private final AtomicLong currentCheckpointId;

    /** Tracks if this reader has been initialized with a subscription */
    private volatile boolean subscriptionInitialized = false;

    /**
     * Creates a share group source reader implementing the CheckpointListener pattern.
     *
     * @param consumerProps consumer properties configured for share groups (must include group.id)
     * @param deserializationSchema schema for deserializing Kafka records
     * @param context source reader context from Flink
     * @param shareGroupMetrics metrics collector for share group operations
     */
    public KafkaShareGroupSourceReader(
            Properties consumerProps,
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            SourceReaderContext context,
            KafkaShareGroupSourceMetrics shareGroupMetrics) {

        // Create fields before super() so lambda can capture them
        this(
                consumerProps,
                deserializationSchema,
                context,
                shareGroupMetrics,
                new AcknowledgmentBuffer(),
                new AtomicLong(-1L));
    }

    /** Private constructor with pre-created buffer and checkpoint ID for lambda capture. */
    private KafkaShareGroupSourceReader(
            Properties consumerProps,
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            SourceReaderContext context,
            KafkaShareGroupSourceMetrics shareGroupMetrics,
            AcknowledgmentBuffer ackBuffer,
            AtomicLong checkpointIdRef) {

        super(
                new KafkaShareGroupFetcherManager(consumerProps, context, shareGroupMetrics),
                new KafkaShareGroupRecordEmitter<>(
                        deserializationSchema,
                        record -> {
                            // Lambda captures buffer and checkpoint ID from constructor parameters
                            long checkpointId = checkpointIdRef.get();
                            if (checkpointId < 0) {
                                checkpointId = 0; // Use 0 if no checkpoint yet
                            }
                            ackBuffer.addRecord(checkpointId, record);
                        }),
                new Configuration(),
                context);

        // Initialize final fields from constructor parameters
        this.acknowledgmentBuffer = ackBuffer;
        this.currentCheckpointId = checkpointIdRef;
        this.shareConsumerRef = new AtomicReference<>();
        this.deserializationSchema = deserializationSchema;
        this.shareGroupId = consumerProps.getProperty("group.id", "unknown-share-group");
        this.shareGroupMetrics = shareGroupMetrics;

        // Initialize transaction manager for 2PC
        this.transactionManager = new FlinkTransactionManager(
            this.shareGroupId,
            null // ShareConsumer will be set after fetcher manager starts
        );

        LOG.info(
                "Created KafkaShareGroupSourceReader for share group '{}' on subtask {} with transactional 2PC",
                shareGroupId,
                context.getIndexOfSubtask());
    }

    // ===========================================================================================
    // Lifecycle Management
    // ===========================================================================================

    @Override
    public void start() {
        // Initialize deserialization schema
        try {
            deserializationSchema.open(
                    new DeserializationSchema.InitializationContext() {
                        @Override
                        public org.apache.flink.metrics.MetricGroup getMetricGroup() {
                            return context.metricGroup();
                        }

                        @Override
                        public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
                            // Simple wrapper for Thread's context classloader
                            final ClassLoader classLoader =
                                    Thread.currentThread().getContextClassLoader();
                            return new org.apache.flink.util.UserCodeClassLoader() {
                                @Override
                                public ClassLoader asClassLoader() {
                                    return classLoader;
                                }

                                @Override
                                public void registerReleaseHookIfAbsent(
                                        String releaseHookName, Runnable releaseHook) {
                                    // No-op - we don't manage classloader lifecycle
                                }
                            };
                        }
                    });

            LOG.info(
                    "Share group '{}': Initialized deserialization schema for subtask {}",
                    shareGroupId,
                    context.getIndexOfSubtask());

        } catch (Exception e) {
            LOG.error(
                    "Share group '{}': Failed to initialize deserialization schema",
                    shareGroupId,
                    e);
            throw new RuntimeException("Failed to initialize deserialization schema", e);
        }

        // Call parent start
        super.start();

        // Set share consumer reference in transaction manager after fetcher starts
        ShareConsumer<byte[], byte[]> consumer = getShareConsumer();
        if (consumer != null) {
            transactionManager.setShareConsumer(consumer);
            LOG.info(
                    "Share group '{}': Transaction manager initialized with ShareConsumer",
                    shareGroupId);
        } else {
            LOG.warn(
                    "Share group '{}': ShareConsumer not available yet - will retry on first checkpoint",
                    shareGroupId);
        }
    }

    // ===========================================================================================
    // Split Management (Simplified for Share Groups)
    // ===========================================================================================

    @Override
    protected void onSplitFinished(Map<String, ShareGroupSubscriptionState> finishedSplitIds) {
        // For share groups, "splits" don't really finish - the subscription is ongoing
        // This method is required by the base class but is effectively a no-op
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Share group '{}': onSplitFinished called (no-op for share groups)",
                    shareGroupId);
        }
    }

    @Override
    protected ShareGroupSubscriptionState initializedState(ShareGroupSubscriptionState split) {
        // Share group splits are minimal - just return the state as-is
        subscriptionInitialized = true;

        LOG.info(
                "Share group '{}': Initialized subscription state for topics: {}",
                shareGroupId,
                split.getSubscribedTopics());

        return split;
    }

    @Override
    protected ShareGroupSubscriptionState toSplitType(
            String splitId, ShareGroupSubscriptionState splitState) {
        // State and split are the same for share groups - no conversion needed
        return splitState;
    }

    // ===========================================================================================
    // Checkpoint Integration
    // ===========================================================================================

    @Override
    public List<ShareGroupSubscriptionState> snapshotState(long checkpointId) {
        // Update current checkpoint ID for record association
        currentCheckpointId.set(checkpointId);

        // Ensure share consumer is set in transaction manager
        ShareConsumer<byte[], byte[]> consumer = getShareConsumer();
        if (consumer != null && transactionManager != null) {
            transactionManager.setShareConsumer(consumer);
        }

        // Get records for this checkpoint (checkpoint subsuming)
        Set<RecordMetadata> recordsToAck = acknowledgmentBuffer.getRecordsUpTo(checkpointId);

        // Phase 1 of 2PC: Prepare acknowledgments
        if (!recordsToAck.isEmpty()) {
            try {
                transactionManager.prepareAcknowledgments(checkpointId, recordsToAck);
                LOG.info(
                        "Share group '{}': CHECKPOINT {} PREPARED - {} records marked for acknowledgment",
                        shareGroupId,
                        checkpointId,
                        recordsToAck.size());
            } catch (Exception e) {
                LOG.error(
                        "Share group '{}': CHECKPOINT {} PREPARE FAILED - transaction will be aborted",
                        shareGroupId,
                        checkpointId,
                        e);
                throw new RuntimeException("Failed to prepare checkpoint " + checkpointId, e);
            }
        } else {
            LOG.debug(
                    "Share group '{}': CHECKPOINT {} SNAPSHOT - No records to prepare",
                    shareGroupId,
                    checkpointId);
        }

        // Get the current subscription state from parent
        List<ShareGroupSubscriptionState> states = super.snapshotState(checkpointId);

        // Log checkpoint snapshot statistics
        AcknowledgmentBuffer.BufferStatistics stats = acknowledgmentBuffer.getStatistics();
        LOG.info(
                "Share group '{}': CHECKPOINT {} SNAPSHOT - {} records buffered across {} checkpoints (memory: {} bytes)",
                shareGroupId,
                checkpointId,
                stats.getTotalRecords(),
                stats.getCheckpointCount(),
                stats.getMemoryUsageBytes());

        // Return minimal subscription state - no offset tracking needed
        return states;
    }

    /**
     * Callback when a checkpoint completes successfully.
     *
     * Phase 2 of 2PC: Commit transaction.
     * The broker applies acknowledgments atomically when checkpoint completes.
     * This ensures no data loss - records remain locked until checkpoint succeeds.
     *
     * @param checkpointId the ID of the checkpoint that completed
     * @throws Exception if commit fails
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        final long startTime = System.currentTimeMillis();

        // Get all records up to this checkpoint for statistics
        Set<RecordMetadata> processedRecords = acknowledgmentBuffer.getRecordsUpTo(checkpointId);

        if (processedRecords.isEmpty()) {
            LOG.debug(
                    "Share group '{}': CHECKPOINT {} COMPLETE - No records processed",
                    shareGroupId,
                    checkpointId);
            super.notifyCheckpointComplete(checkpointId);
            return;
        }

        LOG.info(
                "Share group '{}': CHECKPOINT {} COMPLETE - Committing transaction for {} records",
                shareGroupId,
                checkpointId,
                processedRecords.size());

        try {
            // Phase 2 of 2PC: Commit transaction
            // Broker applies prepared acknowledgments atomically
            transactionManager.commitTransaction(checkpointId);

            // Update metrics
            final long duration = System.currentTimeMillis() - startTime;
            if (shareGroupMetrics != null) {
                shareGroupMetrics.recordSuccessfulCommit();
                for (int i = 0; i < processedRecords.size(); i++) {
                    shareGroupMetrics.recordMessageAcknowledged(
                            duration / Math.max(1, processedRecords.size()));
                }
            }

            // Clean up buffer - remove processed record metadata
            int removedCount = acknowledgmentBuffer.removeUpTo(checkpointId);

            LOG.info(
                    "Share group '{}': CHECKPOINT {} SUCCESS - Committed {} records, cleaned up {} metadata entries in {}ms",
                    shareGroupId,
                    checkpointId,
                    processedRecords.size(),
                    removedCount,
                    duration);

        } catch (Exception e) {
            LOG.error(
                    "Share group '{}': CHECKPOINT {} COMMIT FAILED",
                    shareGroupId,
                    checkpointId,
                    e);
            if (shareGroupMetrics != null) {
                shareGroupMetrics.recordFailedCommit();
            }
            throw e;
        }

        // Call parent implementation
        super.notifyCheckpointComplete(checkpointId);
    }

    /**
     * Callback when a checkpoint is aborted.
     *
     * Abort transaction and release records back to share group for redelivery.
     * Following checkpoint subsuming pattern - next successful checkpoint will handle these records.
     *
     * @param checkpointId the ID of the checkpoint that was aborted
     * @throws Exception if abort operation fails
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // Get records for this checkpoint
        Set<RecordMetadata> recordsToRelease = acknowledgmentBuffer.getRecordsUpTo(checkpointId);

        if (!recordsToRelease.isEmpty()) {
            LOG.info(
                    "Share group '{}': CHECKPOINT {} ABORTED - Releasing {} records for redelivery",
                    shareGroupId,
                    checkpointId,
                    recordsToRelease.size());

            try {
                // Abort transaction - releases record locks for redelivery
                transactionManager.abortTransaction(checkpointId, recordsToRelease);

                LOG.info(
                        "Share group '{}': CHECKPOINT {} ABORTED - Released {} records",
                        shareGroupId,
                        checkpointId,
                        recordsToRelease.size());

            } catch (Exception e) {
                LOG.warn(
                        "Share group '{}': Failed to abort checkpoint {} - records will timeout and be redelivered",
                        shareGroupId,
                        checkpointId,
                        e);
                // Non-fatal - records will timeout and be redelivered automatically
            }
        } else {
            LOG.debug(
                    "Share group '{}': CHECKPOINT {} ABORTED - No records to release",
                    shareGroupId,
                    checkpointId);
        }

        // Following Checkpoint Subsuming Contract: next successful checkpoint will handle these records
        super.notifyCheckpointAborted(checkpointId);
    }

    // ===========================================================================================
    // Record Processing
    // ===========================================================================================

    /**
     * Adds a record to the acknowledgment buffer.
     *
     * <p>This should be called after emitting each record to the Flink pipeline. The record
     * metadata is associated with the current checkpoint ID.
     *
     * @param record the Kafka consumer record to buffer for acknowledgment
     */
    public void addRecordForAcknowledgment(ConsumerRecord<byte[], byte[]> record) {
        long checkpointId = currentCheckpointId.get();
        if (checkpointId < 0) {
            LOG.warn(
                    "Share group '{}': Received record before first checkpoint - using checkpoint ID 0",
                    shareGroupId);
            checkpointId = 0;
        }

        acknowledgmentBuffer.addRecord(checkpointId, record);

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Share group '{}': Buffered record for checkpoint {} - topic={}, partition={}, offset={}",
                    shareGroupId,
                    checkpointId,
                    record.topic(),
                    record.partition(),
                    record.offset());
        }
    }

    // ===========================================================================================
    // Lifecycle Management
    // ===========================================================================================

    @Override
    public void close() throws Exception {
        LOG.info("Closing KafkaShareGroupSourceReader for share group '{}'", shareGroupId);

        try {
            // Get any remaining records from buffer
            AcknowledgmentBuffer.BufferStatistics stats = acknowledgmentBuffer.getStatistics();
            if (stats.getTotalRecords() > 0) {
                LOG.warn(
                        "Share group '{}': Closing with {} unacknowledged records in buffer - "
                                + "these will be redelivered after lock expiration",
                        shareGroupId,
                        stats.getTotalRecords());
            }

            // Clear buffer (records will be redelivered by broker after lock expiration)
            acknowledgmentBuffer.clear();

            // Close parent (closes fetcher manager and share consumer)
            super.close();

            if (shareGroupMetrics != null) {
                shareGroupMetrics.reset();
            }

            LOG.info(
                    "KafkaShareGroupSourceReader for share group '{}' closed successfully",
                    shareGroupId);

        } catch (Exception e) {
            LOG.error(
                    "Error closing KafkaShareGroupSourceReader for share group '{}'",
                    shareGroupId,
                    e);
            throw e;
        }
    }

    // ===========================================================================================
    // Helper Methods
    // ===========================================================================================

    /**
     * Gets the ShareConsumer from the fetcher manager for acknowledgment operations.
     *
     * <p>This method accesses the parent's protected {@code splitFetcherManager} field, casts it to
     * {@link KafkaShareGroupFetcherManager}, and retrieves the ShareConsumer.
     *
     * <p><b>Thread Safety:</b> The ShareConsumer itself is NOT thread-safe. However, this method
     * can be called safely from the reader thread. Actual ShareConsumer operations should be
     * performed carefully to avoid threading issues.
     *
     * @return the ShareConsumer instance, or null if not yet initialized
     */
    private ShareConsumer<byte[], byte[]> getShareConsumer() {
        try {
            // Access parent's protected splitFetcherManager field
            // Cast to KafkaShareGroupFetcherManager to access getShareConsumer()
            if (splitFetcherManager instanceof KafkaShareGroupFetcherManager) {
                KafkaShareGroupFetcherManager fetcherManager =
                        (KafkaShareGroupFetcherManager) splitFetcherManager;
                return fetcherManager.getShareConsumer();
            } else {
                LOG.error(
                        "splitFetcherManager is not KafkaShareGroupFetcherManager: {}",
                        splitFetcherManager.getClass().getName());
                return null;
            }
        } catch (Exception e) {
            LOG.error("Failed to get ShareConsumer from fetcher manager", e);
            return null;
        }
    }

    /**
     * Gets the share group ID for this reader.
     *
     * @return the share group identifier
     */
    public String getShareGroupId() {
        return shareGroupId;
    }

    /**
     * Gets the acknowledgment buffer (for testing/monitoring).
     *
     * @return the acknowledgment buffer instance
     */
    public AcknowledgmentBuffer getAcknowledgmentBuffer() {
        return acknowledgmentBuffer;
    }

    /**
     * Gets buffer statistics (for monitoring).
     *
     * @return current buffer statistics snapshot
     */
    public AcknowledgmentBuffer.BufferStatistics getBufferStatistics() {
        return acknowledgmentBuffer.getStatistics();
    }

    /**
     * Gets the share group metrics collector.
     *
     * @return the metrics collector
     */
    public KafkaShareGroupSourceMetrics getShareGroupMetrics() {
        return shareGroupMetrics;
    }

    /**
     * Checks if the subscription has been initialized.
     *
     * @return true if subscription is active
     */
    public boolean isSubscriptionInitialized() {
        return subscriptionInitialized;
    }
}
