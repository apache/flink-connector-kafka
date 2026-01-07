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

package org.apache.flink.connector.kafka.source.enumerator.subscriber;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Kafka consumer allows a few different ways to consume from the topics, including:
 *
 * <ol>
 *   <li>Subscribe from a collection of topics.
 *   <li>Subscribe to a topic pattern using Java {@code Regex}.
 *   <li>Assign specific partitions.
 * </ol>
 *
 * <p>The KafkaSubscriber provides a unified interface for the Kafka source to support all these
 * three types of subscribing mode.
 *
 * <p>When implementing a subscriber, {@link
 * org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider} can be implemented to
 * provide lineage metadata with source topics.
 */
@PublicEvolving
public interface KafkaSubscriber extends Serializable {

    /**
     * Opens the subscriber. This lifecycle method will be called before {@link
     * #getSubscribedTopicPartitions(AdminClient)} calls are made.
     *
     * <p>Implementations may override this method to initialize any additional resources (beyond
     * the Kafka {@link AdminClient}) required for discovering topic partitions.
     *
     * @param initializationContext initialization context for the subscriber.
     */
    default void open(InitializationContext initializationContext) {}

    /**
     * Get a set of subscribed {@link TopicPartition}s.
     *
     * @param adminClient The admin client used to retrieve subscribed topic partitions.
     * @return A set of subscribed {@link TopicPartition}s
     */
    Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient);

    /**
     * Closes the subscriber. This lifecycle method will be called after this {@link
     * KafkaSubscriber} will no longer be used.
     *
     * <p>Any resources created in the {@link #open(InitializationContext)} method should be cleaned
     * up here.
     */
    default void close() {}

    /** Initialization context for the {@link KafkaSubscriber}. */
    @PublicEvolving
    interface InitializationContext {}

    // ----------------- factory methods --------------

    static KafkaSubscriber getTopicListSubscriber(List<String> topics) {
        return new TopicListSubscriber(topics);
    }

    static KafkaSubscriber getTopicPatternSubscriber(Pattern topicPattern) {
        return new TopicPatternSubscriber(topicPattern);
    }

    static KafkaSubscriber getPartitionSetSubscriber(Set<TopicPartition> partitions) {
        return new PartitionSetSubscriber(partitions);
    }
}
