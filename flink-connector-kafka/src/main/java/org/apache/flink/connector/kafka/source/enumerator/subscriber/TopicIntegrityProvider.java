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

import org.apache.flink.connector.kafka.integrity.TopicIntegrityException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.util.AdminUtils.getTopicMetadata;
import static org.apache.flink.connector.kafka.util.AdminUtils.getTopicsByPattern;

/** Provider of topic integrity related functionalities for {@link KafkaSourceEnumerator}. */
class TopicIntegrityProvider implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(TopicIntegrityProvider.class);
    private final Map<String, String> topicIntegrityMapping = new ConcurrentHashMap<>();

    TopicIntegrityProvider() {}

    public void open(Map<String, String> topicIntegrityMappingFromContext) {
        topicIntegrityMapping.putAll(topicIntegrityMappingFromContext);
    }

    public Map<String, TopicDescription> getVerifiedTopicMetadata(
            AdminClient adminClient, Pattern pattern) {
        final Collection<String> topicsToVerifyInPatternMode =
                topicIntegrityMapping.keySet().stream()
                        .filter(pattern.asPredicate())
                        .collect(Collectors.toCollection(HashSet::new));
        topicsToVerifyInPatternMode.addAll(getTopicsByPattern(adminClient, pattern));
        return getVerifiedTopicMetadata(adminClient, topicsToVerifyInPatternMode);
    }

    public Map<String, TopicDescription> getVerifiedTopicMetadata(
            AdminClient adminClient, Collection<String> subscribedTopicNames) {
        Map<String, TopicDescription> topicMetadata = new HashMap<>();
        try {
            topicMetadata = getTopicMetadata(adminClient, subscribedTopicNames);
            failIfRecreated(subscribedTopicNames, topicMetadata);
        } catch (RuntimeException original) {
            if (ExceptionUtils.getRootCause(original) instanceof UnknownTopicOrPartitionException) {
                // UnknownTopicOrPartitionException can be transient due to broker timeout
                // or permanent due to topic/partition loss.
                // Determine if the exception is caused by a missing topic
                // and if yes, trigger a TopicIntegrity failure instead
                try {
                    failIfMissing(subscribedTopicNames, adminClient.listTopics().names().get());
                } catch (TopicIntegrityException missingTopicException) {
                    throw missingTopicException;
                } catch (Exception ignored) {
                    // ignored so we fallback to the original error
                }
            }
            throw original;
        }
        trackNewIdsInMapping(subscribedTopicNames, topicMetadata);
        return topicMetadata;
    }

    private void trackNewIdsInMapping(
            Collection<String> subscribedTopicNames, Map<String, TopicDescription> topicMetadata) {

        // Add new subscribed topic to mapping
        for (String subscribedTopicName : subscribedTopicNames) {
            if (!topicIntegrityMapping.keySet().contains(subscribedTopicName)) {
                topicIntegrityMapping.put(
                        subscribedTopicName,
                        topicMetadata.get(subscribedTopicName).topicId().toString());
            }
        }
        // Remove outdated topics from mapping
        for (String topicNameFromMapping : topicIntegrityMapping.keySet()) {
            if (!subscribedTopicNames.contains(topicNameFromMapping)) {
                topicIntegrityMapping.remove(topicNameFromMapping);
            }
        }
    }

    public Map<String, String> getTopicIntegrityMapping() {
        return new HashMap<>(topicIntegrityMapping);
    }

    private void failIfRecreated(
            Collection<String> subscribedTopicNames, Map<String, TopicDescription> metadataTopics)
            throws RuntimeException {
        for (String subscribedTopicName : subscribedTopicNames) {
            final TopicDescription topicDescription = metadataTopics.get(subscribedTopicName);
            if (topicDescription == null) {
                LOG.error("Topic {} found missing during recreation check", subscribedTopicName);
                throw new TopicIntegrityException("Topic " + subscribedTopicName + " is missing");
            }
            final String topicIdFromState = topicIntegrityMapping.get(subscribedTopicName);
            final Uuid topicIdFromMetadata = topicDescription.topicId();
            if (topicIdFromState == null || topicIdFromMetadata == null) {
                // we skip topic integrity check for null topicId
                // due to broker configuration, or topic not yet stored on topicIntegrityMapping
                LOG.warn(
                        "Topic integrity check skipped due to a null topicId: topic name: {},"
                                + " topic id passed from initial config: {}"
                                + " current topic id on kafka server: {}",
                        subscribedTopicName,
                        topicIdFromState,
                        topicIdFromMetadata);
                return;
            }
            if (!topicIdFromState.equals(topicIdFromMetadata.toString())) {
                LOG.error(
                        "Topic integrity mismatch: expected topic Id of {} to be {}, got {}",
                        subscribedTopicName,
                        topicIdFromState,
                        topicIdFromMetadata);
                throw new TopicIntegrityException(
                        "Topic " + subscribedTopicName + " was recreated");
            }
        }
    }

    private static void failIfMissing(
            Collection<String> subscribedTopicNames, Collection<String> existingTopicNames)
            throws RuntimeException {
        for (String subscribedTopicName : subscribedTopicNames) {
            if (!existingTopicNames.contains(subscribedTopicName)) {
                LOG.error(
                        "Topic {} is missing in current topics {}",
                        subscribedTopicName,
                        existingTopicNames);
                throw new TopicIntegrityException("Topic " + subscribedTopicName + " is missing");
            }
        }
    }
}
