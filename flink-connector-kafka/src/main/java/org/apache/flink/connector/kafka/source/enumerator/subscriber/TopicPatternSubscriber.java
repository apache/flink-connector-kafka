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

import org.apache.flink.connector.kafka.lineage.LineageFacetProvider;
import org.apache.flink.connector.kafka.lineage.facets.KafkaTopicPatternFacet;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

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
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.getTopicMetadata;

/** A subscriber to a topic pattern. */
class TopicPatternSubscriber implements KafkaSubscriber, LineageFacetProvider {
    private static final long serialVersionUID = -7471048577725467797L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicPatternSubscriber.class);
    private final Pattern topicPattern;

    TopicPatternSubscriber(Pattern topicPattern) {
        this.topicPattern = topicPattern;
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
        LOG.debug("Fetching descriptions for {} topics on Kafka cluster", topicPattern.pattern());
        final Map<String, TopicDescription> matchedTopicMetadata =
                getTopicMetadata(adminClient, topicPattern);

        Set<TopicPartition> subscribedTopicPartitions = new HashSet<>();

        matchedTopicMetadata.forEach(
                (topicName, topicDescription) -> {
                    for (TopicPartitionInfo partition : topicDescription.partitions()) {
                        subscribedTopicPartitions.add(
                                new TopicPartition(topicDescription.name(), partition.partition()));
                    }
                });

        return subscribedTopicPartitions;
    }

    @Override
    public List<LineageDatasetFacet> getDatasetFacets() {
        return Collections.singletonList(new KafkaTopicPatternFacet(topicPattern));
    }
}
