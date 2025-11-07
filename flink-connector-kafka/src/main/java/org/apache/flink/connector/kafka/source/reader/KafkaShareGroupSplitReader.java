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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.split.ShareGroupSubscriptionState;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Simplified SplitReader for Kafka Share Groups using direct subscription pattern.
 *
 * <h2>Key Design Changes from Traditional Implementation</h2>
 *
 * <p>This simplified reader eliminates the complex split-based architecture and batch management in
 * favor of a direct subscription model that aligns with how Kafka share groups actually work:
 *
 * <h3>What Was Removed</h3>
 *
 * <ul>
 *   <li><b>ShareGroupBatchManager</b> - Replaced by AcknowledgmentBuffer in SourceReader
 *   <li><b>Split assignment tracking</b> - No partition assignment needed
 *   <li><b>Batch storage</b> - Records stored as metadata only (40 bytes vs 1KB+)
 *   <li><b>Complex state management</b> - State handled at reader level
 * </ul>
 *
 * <h3>New Simplified Flow</h3>
 *
 * <pre>{@code
 * 1. Subscribe to topics (one-time, not per-split)
 * 2. Poll records from ShareConsumer
 * 3. Return records to SourceReader
 * 4. SourceReader stores metadata in AcknowledgmentBuffer
 * 5. On checkpoint complete, SourceReader acknowledges via exposed ShareConsumer
 * }</pre>
 *
 * <h2>ShareConsumer Exposure</h2>
 *
 * <p>This reader exposes the {@link ShareConsumer} instance to the {@link
 * KafkaShareGroupSourceReader} via the FetcherManager. This enables the reader to directly call
 * {@code acknowledge()} and {@code commitSync()} during checkpoint completion, implementing the
 * proper acknowledgment flow.
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This reader runs in Flink's split fetcher thread. The ShareConsumer is not thread-safe, so all
 * operations must be performed from the same thread. Access from the SourceReader happens via the
 * fetcher manager which ensures thread safety.
 *
 * @see ShareGroupSubscriptionState
 * @see KafkaShareGroupSourceReader
 */
@Internal
public class KafkaShareGroupSplitReader
        implements SplitReader<ConsumerRecord<byte[], byte[]>, ShareGroupSubscriptionState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSplitReader.class);

    /** Poll timeout for ShareConsumer.poll() calls */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    /** Kafka 4.1+ ShareConsumer for share group consumption */
    private final ShareConsumer<byte[], byte[]> shareConsumer;

    /** Share group ID for this consumer */
    private final String shareGroupId;

    /** Reader ID (subtask index) */
    private final int readerId;

    /** Current subscription state (topics being consumed) */
    private ShareGroupSubscriptionState currentSubscription;

    /** Metrics collector for monitoring */
    private final KafkaShareGroupSourceMetrics metrics;

    /** Flag indicating if consumer has been closed */
    private volatile boolean closed = false;

    /**
     * Creates a simplified share group split reader with direct subscription.
     *
     * @param props consumer properties configured for share groups
     * @param context the source reader context
     * @param metrics metrics collector for share group operations (can be null)
     */
    public KafkaShareGroupSplitReader(
            Properties props,
            SourceReaderContext context,
            @Nullable KafkaShareGroupSourceMetrics metrics) {

        this.readerId = context.getIndexOfSubtask();
        this.metrics = metrics;

        // Configure ShareConsumer properties
        Properties shareConsumerProps = new Properties();
        shareConsumerProps.putAll(props);

        // Force explicit acknowledgment mode
        shareConsumerProps.setProperty("share.acknowledgement.mode", "explicit");
        shareConsumerProps.setProperty("group.type", "share");

        this.shareGroupId = shareConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if (shareGroupId == null) {
            throw new IllegalArgumentException("Share group ID (group.id) must be specified");
        }

        // Configure client ID with reader index
        String baseClientId =
                shareConsumerProps.getProperty(
                        ConsumerConfig.CLIENT_ID_CONFIG, "flink-share-consumer");
        shareConsumerProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG,
                String.format("%s-%s-reader-%d", baseClientId, shareGroupId, readerId));

        // Remove unsupported properties for share consumers
        shareConsumerProps.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        shareConsumerProps.remove(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);

        // Create ShareConsumer (Kafka 4.1+)
        this.shareConsumer =
                new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(shareConsumerProps);

        LOG.info(
                "Created simplified KafkaShareGroupSplitReader for share group '{}' reader {} "
                        + "with direct subscription pattern",
                shareGroupId,
                readerId);
    }

    // ===========================================================================================
    // Core Fetch Operation
    // ===========================================================================================

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        // Check if closed
        if (closed) {
            LOG.debug(
                    "Share group '{}' reader {} is closed, returning empty",
                    shareGroupId,
                    readerId);
            return ShareGroupRecordsWithSplitIds.empty();
        }

        // Check if subscribed
        if (currentSubscription == null) {
            LOG.debug(
                    "Share group '{}' reader {} waiting for subscription", shareGroupId, readerId);
            return ShareGroupRecordsWithSplitIds.empty();
        }

        try {
            // Poll records from ShareConsumer
            ConsumerRecords<byte[], byte[]> consumerRecords = shareConsumer.poll(POLL_TIMEOUT);

            if (consumerRecords.isEmpty()) {
                return ShareGroupRecordsWithSplitIds.empty();
            }

            // Convert ConsumerRecords to list and acknowledge immediately
            //
            // IMPORTANT SEMANTIC GUARANTEE: AT-MOST-ONCE
            //
            // ShareConsumer requires acknowledgment before next poll() call.
            // This creates a fundamental incompatibility with Flink's checkpoint model:
            //
            //   Flink needs: poll() → checkpoint → acknowledge()
            //   ShareConsumer requires: poll() → acknowledge() → poll()
            //
            // We must ACCEPT immediately to allow continuous polling, which means:
            // - Records acknowledged BEFORE checkpoint completes
            // - If job fails after ACCEPT but before checkpoint: DATA IS LOST
            // - This provides AT-MOST-ONCE semantics (not exactly-once or at-least-once)
            //
            // Users requiring stronger guarantees should use KafkaSource with partition assignment.
            //
            List<ConsumerRecord<byte[], byte[]>> recordList =
                    new ArrayList<>(consumerRecords.count());
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                recordList.add(record);

                // Acknowledge as ACCEPT to satisfy ShareConsumer requirement
                // This tells Kafka: "I successfully processed this record"
                // Note: We haven't actually processed it yet - just polled it
                shareConsumer.acknowledge(
                        record, org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT);

                // Update metrics
                if (metrics != null) {
                    metrics.recordMessageReceived();
                }
            }

            // Commit acknowledgments to Kafka coordinator
            shareConsumer.commitSync(java.time.Duration.ofSeconds(5));

            LOG.debug(
                    "Share group '{}' reader {} fetched and acknowledged {} records (at-most-once semantics)",
                    shareGroupId,
                    readerId,
                    recordList.size());

            // Return records wrapped with split ID
            // Warning: Records are already marked as processed in Kafka
            return new ShareGroupRecordsWithSplitIds(
                    recordList.iterator(), currentSubscription.splitId());

        } catch (WakeupException e) {
            LOG.info("Share group '{}' reader {} woken up during fetch", shareGroupId, readerId);
            return ShareGroupRecordsWithSplitIds.empty();

        } catch (Exception e) {
            LOG.error(
                    "Share group '{}' reader {} failed to fetch records",
                    shareGroupId,
                    readerId,
                    e);
            throw new IOException("Failed to fetch records from share group: " + shareGroupId, e);
        }
    }

    // ===========================================================================================
    // Subscription Management
    // ===========================================================================================

    @Override
    public void handleSplitsChanges(SplitsChange<ShareGroupSubscriptionState> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition) {
            handleSubscriptionChange((SplitsAddition<ShareGroupSubscriptionState>) splitsChanges);
        }
        // SplitsRemoval is ignored - share group subscription is persistent until close
    }

    /**
     * Handles subscription changes by subscribing to topics.
     *
     * <p>For share groups, we don't have traditional "splits" - instead we have a subscription
     * state that tells us which topics to subscribe to. The broker's share group coordinator
     * handles distribution.
     */
    private void handleSubscriptionChange(SplitsAddition<ShareGroupSubscriptionState> addition) {
        if (addition.splits().isEmpty()) {
            return;
        }

        // Get first subscription state (there should only be one)
        ShareGroupSubscriptionState newSubscription = addition.splits().get(0);
        this.currentSubscription = newSubscription;

        Set<String> topics = newSubscription.getSubscribedTopics();

        try {
            shareConsumer.subscribe(topics);
            LOG.info(
                    "Share group '{}' reader {} subscribed to topics: {}",
                    shareGroupId,
                    readerId,
                    topics);

        } catch (Exception e) {
            LOG.error(
                    "Share group '{}' reader {} failed to subscribe to topics: {}",
                    shareGroupId,
                    readerId,
                    topics,
                    e);
            throw new RuntimeException("Failed to subscribe to topics", e);
        }
    }

    // ===========================================================================================
    // ShareConsumer Access (for SourceReader acknowledgment)
    // ===========================================================================================

    /**
     * Gets the ShareConsumer instance for acknowledgment operations.
     *
     * <p>This method is called by {@link KafkaShareGroupSourceReader} via the {@link
     * org.apache.flink.connector.kafka.source.reader.fetcher.KafkaShareGroupFetcherManager} to
     * access the ShareConsumer for calling {@code acknowledge()} and {@code commitSync()} during
     * checkpoint completion.
     *
     * <p><b>Thread Safety:</b> The ShareConsumer is NOT thread-safe. The caller must ensure all
     * operations happen from the split fetcher thread.
     *
     * @return the ShareConsumer instance
     */
    public ShareConsumer<byte[], byte[]> getShareConsumer() {
        return shareConsumer;
    }

    // ===========================================================================================
    // Lifecycle Management
    // ===========================================================================================

    @Override
    public void wakeUp() {
        if (!closed) {
            shareConsumer.wakeup();
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

        try {
            // Unsubscribe from topics
            shareConsumer.unsubscribe();

            // Close ShareConsumer (this will release all acquisition locks)
            shareConsumer.close(Duration.ofSeconds(5));

            LOG.info("Share group '{}' reader {} closed successfully", shareGroupId, readerId);

        } catch (Exception e) {
            LOG.warn(
                    "Share group '{}' reader {} encountered error during close: {}",
                    shareGroupId,
                    readerId,
                    e.getMessage());
            throw e;
        }
    }

    // ===========================================================================================
    // Getters (for monitoring and testing)
    // ===========================================================================================

    /** Gets the share group ID. */
    public String getShareGroupId() {
        return shareGroupId;
    }

    /** Gets the reader ID (subtask index). */
    public int getReaderId() {
        return readerId;
    }

    /** Gets the current subscription state. */
    @Nullable
    public ShareGroupSubscriptionState getCurrentSubscription() {
        return currentSubscription;
    }

    /** Gets the subscribed topics. */
    public Set<String> getSubscribedTopics() {
        return currentSubscription != null
                ? currentSubscription.getSubscribedTopics()
                : Collections.emptySet();
    }

    /** Checks if this reader is closed. */
    public boolean isClosed() {
        return closed;
    }

    // ===========================================================================================
    // Inner Classes
    // ===========================================================================================

    /**
     * Simple implementation of RecordsWithSplitIds for share group records.
     *
     * <p>For share groups, the "split ID" is just an identifier - it doesn't represent a partition
     * assignment since message distribution is handled by the broker.
     */
    private static class ShareGroupRecordsWithSplitIds
            implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {

        private static final ShareGroupRecordsWithSplitIds EMPTY =
                new ShareGroupRecordsWithSplitIds(Collections.emptyIterator(), null);

        private final Iterator<ConsumerRecord<byte[], byte[]>> recordIterator;
        private final String splitId;
        private boolean hasReturnedSplit = false;

        private ShareGroupRecordsWithSplitIds(
                Iterator<ConsumerRecord<byte[], byte[]>> recordIterator, String splitId) {
            this.recordIterator = recordIterator;
            this.splitId = splitId;
        }

        public static ShareGroupRecordsWithSplitIds empty() {
            return EMPTY;
        }

        @Override
        public String nextSplit() {
            if (!hasReturnedSplit && recordIterator.hasNext() && splitId != null) {
                hasReturnedSplit = true;
                return splitId;
            }
            return null;
        }

        @Override
        public ConsumerRecord<byte[], byte[]> nextRecordFromSplit() {
            return recordIterator.hasNext() ? recordIterator.next() : null;
        }

        @Override
        public void recycle() {
            // No recycling needed for share group records
        }

        @Override
        public Set<String> finishedSplits() {
            // Share group subscriptions don't "finish" like partition-based splits
            return Collections.emptySet();
        }
    }
}
