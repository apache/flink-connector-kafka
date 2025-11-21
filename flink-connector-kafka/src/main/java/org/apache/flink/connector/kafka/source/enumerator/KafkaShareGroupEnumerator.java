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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.split.ShareGroupSubscriptionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Minimal enumerator for Kafka Share Group sources.
 *
 * <h2>Simplified Architecture for Share Groups</h2>
 *
 * <p>This enumerator implements a fundamentally different pattern than traditional Kafka
 * partition-based sources. Instead of managing complex partition assignments, it simply assigns a
 * subscription state to each reader containing all topics. The Kafka share group coordinator
 * handles all message distribution dynamically.
 *
 * <h2>Key Differences from Traditional Kafka Enumerator</h2>
 *
 * <table border="1">
 * <tr>
 *   <th>Aspect</th>
 *   <th>Traditional KafkaSourceEnumerator</th>
 *   <th>KafkaShareGroupEnumerator</th>
 * </tr>
 * <tr>
 *   <td>Split Type</td>
 *   <td>KafkaPartitionSplit (per partition)</td>
 *   <td>ShareGroupSubscriptionState (per reader)</td>
 * </tr>
 * <tr>
 *   <td>Assignment Logic</td>
 *   <td>Complex partition distribution algorithm</td>
 *   <td>Same subscription to all readers</td>
 * </tr>
 * <tr>
 *   <td>State Tracking</td>
 *   <td>Partition metadata, offsets, assignments</td>
 *   <td>Just topics and share group ID</td>
 * </tr>
 * <tr>
 *   <td>Partition Discovery</td>
 *   <td>Periodic discovery of new partitions</td>
 *   <td>Not needed (broker handles distribution)</td>
 * </tr>
 * <tr>
 *   <td>Rebalancing</td>
 *   <td>Manual partition redistribution on topology changes</td>
 *   <td>Automatic by share group coordinator</td>
 * </tr>
 * </table>
 *
 * <h2>How Share Group Enumerator Works</h2>
 *
 * <pre>{@code
 * 1. Enumerator knows: Topics [topic1, topic2] + Share Group ID "my-group"
 * 2. Reader 1 registers → Enumerator assigns ShareGroupSubscriptionState([topic1, topic2], "my-group")
 * 3. Reader 2 registers → Enumerator assigns ShareGroupSubscriptionState([topic1, topic2], "my-group")
 * 4. Reader N registers → Enumerator assigns ShareGroupSubscriptionState([topic1, topic2], "my-group")
 *
 * All readers get the SAME subscription, but the broker's share group coordinator
 * distributes DIFFERENT messages to each consumer dynamically.
 * }</pre>
 *
 * <h2>Memory & Complexity Benefits</h2>
 *
 * <ul>
 *   <li>No partition metadata storage (0 bytes vs KBs per partition)
 *   <li>No partition discovery overhead
 *   <li>No rebalancing logic (100+ lines eliminated)
 *   <li>Simple assignment: O(1) per reader vs O(partitions * readers)
 * </ul>
 *
 * @see ShareGroupSubscriptionState
 * @see KafkaShareGroupEnumeratorState
 */
@Internal
public class KafkaShareGroupEnumerator
        implements SplitEnumerator<ShareGroupSubscriptionState, KafkaShareGroupEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupEnumerator.class);

    private final SplitEnumeratorContext<ShareGroupSubscriptionState> context;
    private final Set<String> topics;
    private final String shareGroupId;
    private final KafkaShareGroupEnumeratorState state;

    /**
     * Creates a minimal share group enumerator.
     *
     * @param topics the set of topics to subscribe to
     * @param shareGroupId the share group identifier
     * @param state the enumerator state (for checkpointing)
     * @param context the enumerator context
     */
    public KafkaShareGroupEnumerator(
            Set<String> topics,
            String shareGroupId,
            @Nullable KafkaShareGroupEnumeratorState state,
            SplitEnumeratorContext<ShareGroupSubscriptionState> context) {

        this.topics = topics;
        this.shareGroupId = shareGroupId;
        this.context = context;
        this.state =
                state != null ? state : new KafkaShareGroupEnumeratorState(topics, shareGroupId);

        LOG.info(
                "Created KafkaShareGroupEnumerator for share group '{}' with {} topics: {} - {} reader(s) expected",
                shareGroupId,
                topics.size(),
                topics,
                context.currentParallelism());

        if (topics.isEmpty()) {
            LOG.warn(
                    "No topics configured for share group '{}' - no splits will be assigned",
                    shareGroupId);
        }
    }

    // ===========================================================================================
    // Lifecycle Methods
    // ===========================================================================================

    @Override
    public void start() {
        LOG.info(
                "Starting KafkaShareGroupEnumerator for share group '{}' with {} registered readers",
                shareGroupId,
                context.registeredReaders().size());

        if (topics.isEmpty()) {
            LOG.error(
                    "Cannot start enumerator with empty topics for share group '{}'", shareGroupId);
            return;
        }

        // Assign subscription to all currently registered readers
        assignSubscriptionToAllReaders();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing KafkaShareGroupEnumerator for share group '{}'", shareGroupId);
    }

    // ===========================================================================================
    // Split Assignment (Simplified for Share Groups)
    // ===========================================================================================

    @Override
    public void addReader(int subtaskId) {
        LOG.info(
                "Adding reader {} to share group '{}' - assigning subscription to topics: {}",
                subtaskId,
                shareGroupId,
                topics);
        assignSubscriptionToReader(subtaskId);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug(
                "Received split request from subtask {} (host: {}) for share group '{}'",
                subtaskId,
                requesterHostname,
                shareGroupId);

        // For share groups, we assign the subscription immediately when reader is added
        // This request is typically sent by readers as a fallback, so just re-assign
        assignSubscriptionToReader(subtaskId);
    }

    /**
     * Handles splits returned from a failed reader.
     *
     * <p>For share groups, when a reader fails:
     *
     * <ul>
     *   <li>Any messages it had acquired (but not acknowledged) will be automatically released by
     *       the broker after the acquisition lock timeout (default 30s)
     *   <li>Those messages become available to other consumers in the share group
     *   <li>No explicit split reassignment is needed from the enumerator
     * </ul>
     *
     * <p>This is fundamentally different from partition-based sources where the enumerator must
     * explicitly reassign partitions to other readers.
     *
     * @param splits the splits being returned (subscription states from failed reader)
     * @param subtaskId the ID of the failed subtask
     */
    @Override
    public void addSplitsBack(List<ShareGroupSubscriptionState> splits, int subtaskId) {
        LOG.info(
                "Received {} subscription state(s) back from failed reader {} in share group '{}' - "
                        + "no reassignment needed (broker will auto-rebalance)",
                splits.size(),
                subtaskId,
                shareGroupId);

        // For share groups, splits don't need explicit reassignment
        // The share group coordinator handles message redistribution automatically:
        // 1. Acquisition locks from failed consumer expire (default 30s)
        // 2. Messages become available to remaining consumers
        // 3. No enumerator action required
    }

    // ===========================================================================================
    // Checkpointing
    // ===========================================================================================

    @Override
    public KafkaShareGroupEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug(
                "Snapshotting enumerator state for share group '{}' at checkpoint {}",
                shareGroupId,
                checkpointId);
        return state;
    }

    // ===========================================================================================
    // Internal Assignment Logic
    // ===========================================================================================

    /** Assigns subscription to all currently registered readers. */
    private void assignSubscriptionToAllReaders() {
        LOG.debug(
                "Assigning subscription to all {} registered readers",
                context.registeredReaders().size());

        for (int readerId : context.registeredReaders().keySet()) {
            assignSubscriptionToReader(readerId);
        }
    }

    /**
     * Assigns subscription state to a specific reader.
     *
     * <p>Each reader gets the SAME subscription (all topics), but the Kafka share group coordinator
     * ensures different messages are delivered to each consumer. This enables:
     *
     * <ul>
     *   <li>Parallelism > partitions (multiple readers per partition)
     *   <li>Dynamic load balancing by broker
     *   <li>Automatic message redistribution on reader failure
     * </ul>
     *
     * @param readerId the reader (subtask) ID to assign to
     */
    private void assignSubscriptionToReader(int readerId) {
        if (topics.isEmpty()) {
            LOG.warn("Cannot assign subscription to reader {} - no topics configured", readerId);
            return;
        }

        // Create ONE subscription state containing ALL topics
        // This is the key simplification: no per-topic or per-partition splits
        ShareGroupSubscriptionState subscription =
                new ShareGroupSubscriptionState(shareGroupId, topics);

        LOG.info(
                "Assigning subscription to reader {}: share group '{}' with {} topics: {}",
                readerId,
                shareGroupId,
                topics.size(),
                topics);

        // Assign to reader through Flink's split assignment API
        context.assignSplits(
                new SplitsAssignment<>(
                        Collections.singletonMap(
                                readerId, Collections.singletonList(subscription))));

        LOG.debug(
                "Successfully assigned subscription '{}' to reader {}",
                subscription.splitId(),
                readerId);
    }

    // ===========================================================================================
    // Getters (for monitoring and testing)
    // ===========================================================================================

    /** Gets the subscribed topics. */
    public Set<String> getTopics() {
        return topics;
    }

    /** Gets the share group ID. */
    public String getShareGroupId() {
        return shareGroupId;
    }

    /** Gets the current enumerator state. */
    public KafkaShareGroupEnumeratorState getState() {
        return state;
    }
}
