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

package org.apache.flink.connector.kafka.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Minimal split representation for Kafka share group subscriptions.
 *
 * <p>Unlike traditional Kafka splits that represent partitions with specific offsets, this
 * represents the subscription state for a share group consumer. This is a lightweight container
 * that exists primarily to satisfy Flink's Source API requirements.
 *
 * <h2>Key Differences from Traditional Kafka Splits</h2>
 *
 * <table border="1">
 * <tr>
 *   <th>Aspect</th>
 *   <th>Traditional KafkaPartitionSplit</th>
 *   <th>ShareGroupSubscriptionState</th>
 * </tr>
 * <tr>
 *   <td>Represents</td>
 *   <td>Single partition with offset range</td>
 *   <td>Set of subscribed topics</td>
 * </tr>
 * <tr>
 *   <td>Assignment</td>
 *   <td>One split per partition per reader</td>
 *   <td>One split per reader (all topics)</td>
 * </tr>
 * <tr>
 *   <td>State Tracking</td>
 *   <td>Current offset, stopping offset</td>
 *   <td>No offset tracking (broker-managed)</td>
 * </tr>
 * <tr>
 *   <td>Distribution</td>
 *   <td>Enumerator assigns partitions</td>
 *   <td>Broker coordinator distributes messages</td>
 * </tr>
 * </table>
 *
 * <p>In share groups, the Kafka broker's share group coordinator handles message distribution at
 * the message level, not the partition level. Therefore, this "split" is purely a subscription
 * state holder, not a true split in the traditional sense.
 *
 * <h2>Thread Safety</h2>
 *
 * This class is immutable and thread-safe.
 *
 * @see org.apache.flink.connector.kafka.source.KafkaShareGroupSource
 */
@Internal
public class ShareGroupSubscriptionState implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final Set<String> subscribedTopics;
    private final String shareGroupId;

    /**
     * Creates a subscription state for a share group.
     *
     * @param shareGroupId the share group identifier
     * @param subscribedTopics set of topics to subscribe to
     */
    public ShareGroupSubscriptionState(String shareGroupId, Set<String> subscribedTopics) {
        this.shareGroupId = Objects.requireNonNull(shareGroupId, "Share group ID cannot be null");
        this.subscribedTopics =
                Collections.unmodifiableSet(
                        new HashSet<>(
                                Objects.requireNonNull(
                                        subscribedTopics, "Subscribed topics cannot be null")));
        this.splitId = "share-group-" + shareGroupId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    /**
     * Gets the immutable set of subscribed topics.
     *
     * @return set of topic names
     */
    public Set<String> getSubscribedTopics() {
        return subscribedTopics;
    }

    /**
     * Gets the share group identifier.
     *
     * @return share group ID
     */
    public String getShareGroupId() {
        return shareGroupId;
    }

    /**
     * Checks if this subscription includes the given topic.
     *
     * @param topic topic name to check
     * @return true if subscribed to this topic
     */
    public boolean isSubscribedTo(String topic) {
        return subscribedTopics.contains(topic);
    }

    /**
     * Gets the number of subscribed topics.
     *
     * @return number of topics
     */
    public int getTopicCount() {
        return subscribedTopics.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShareGroupSubscriptionState)) return false;
        ShareGroupSubscriptionState that = (ShareGroupSubscriptionState) o;
        return Objects.equals(shareGroupId, that.shareGroupId)
                && Objects.equals(subscribedTopics, that.subscribedTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shareGroupId, subscribedTopics);
    }

    @Override
    public String toString() {
        return String.format(
                "ShareGroupSubscriptionState{shareGroupId='%s', topics=%s}",
                shareGroupId, subscribedTopics);
    }
}
