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

import org.apache.flink.connector.kafka.integrity.TopicIntegrityAware;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifierProvider;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.kafka.util.AdminUtils.getTopicMetadata;

/**
 * A subscriber to a fixed list of topics. The subscribed topics must have existed in the Kafka
 * cluster, otherwise an exception will be thrown.
 */
class TopicListSubscriber
        implements KafkaSubscriber, KafkaDatasetIdentifierProvider, TopicIntegrityAware {
    private static final long serialVersionUID = -6917603843104947866L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicListSubscriber.class);
    private final List<String> topicNames;
    private final TopicIntegrityProvider topicIntegrityProvider;
    private volatile boolean topicIntegrityCheckEnabled = false;

    TopicListSubscriber(List<String> topics) {
        this.topicNames = topics;
        this.topicIntegrityProvider = new TopicIntegrityProvider();
    }

    @Override
    public void open(InitializationContext initializationContext) {
        topicIntegrityCheckEnabled = initializationContext.topicIntegrityCheckEnabled();
        topicIntegrityProvider.open(initializationContext.topicIntegrityMapping());
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
        LOG.debug("Fetching descriptions for topics: {}", topicNames);
        final Map<String, TopicDescription> topicMetadata =
                topicIntegrityCheckEnabled
                        ? topicIntegrityProvider.getVerifiedTopicMetadata(adminClient, topicNames)
                        : getTopicMetadata(adminClient, topicNames);
        Set<TopicPartition> subscribedPartitions = new HashSet<>();
        for (TopicDescription topic : topicMetadata.values()) {
            for (TopicPartitionInfo partition : topic.partitions()) {
                subscribedPartitions.add(new TopicPartition(topic.name(), partition.partition()));
            }
        }

        return subscribedPartitions;
    }

    @Override
    public Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier() {
        return Optional.of(DefaultKafkaDatasetIdentifier.ofTopics(topicNames));
    }

    @Override
    public Map<String, String> getTopicIntegrityMapping() {
        return topicIntegrityCheckEnabled
                ? topicIntegrityProvider.getTopicIntegrityMapping()
                : Collections.emptyMap();
    }
}
