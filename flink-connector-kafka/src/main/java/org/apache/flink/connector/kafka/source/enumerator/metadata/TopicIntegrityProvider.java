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

package org.apache.flink.connector.kafka.source.enumerator.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.util.AdminUtils;
import org.apache.flink.util.ExceptionUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Provider of topic integrity related functionalities for {@link
 * org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator}.
 */
@Internal
public class TopicIntegrityProvider implements TopicMetadataProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TopicIntegrityProvider.class);
    private final Map<String, String> trackedTopicIdsByName;

    public TopicIntegrityProvider(Map<String, String> trackedTopicIdsByNameFromContext) {
        trackedTopicIdsByName = new ConcurrentHashMap<>(trackedTopicIdsByNameFromContext);
    }

    @Override
    public Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Pattern pattern) {
        final Collection<String> topicsToVerifyInPatternMode =
                trackedTopicIdsByName.keySet().stream()
                        .filter(name -> pattern.matcher(name).matches())
                        .collect(Collectors.toCollection(HashSet::new));
        topicsToVerifyInPatternMode.addAll(AdminUtils.getTopicsByPattern(adminClient, pattern));
        return getTopicMetadata(adminClient, topicsToVerifyInPatternMode);
    }

    @Override
    public Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Collection<String> subscribedTopicNames) {
        Map<String, TopicDescription> topicMetadata;
        try {
            topicMetadata = AdminUtils.getTopicMetadata(adminClient, subscribedTopicNames);
            failIfRecreated(subscribedTopicNames, topicMetadata);
        } catch (RuntimeException original) {
            if (ExceptionUtils.findThrowable(original, UnknownTopicOrPartitionException.class)
                    .isPresent()) {
                // UnknownTopicOrPartitionException can be transient due to broker timeout
                // or permanent due to topic/partition loss.
                // Determine if the exception is caused by a missing topic
                // and if yes, trigger a TopicIntegrity failure instead
                try {
                    failIfMissing(subscribedTopicNames, adminClient.listTopics().names().get());
                } catch (TopicIntegrityException missingTopicException) {
                    throw missingTopicException;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception ignored) {
                    // ignored so we fallback to the original error
                }
            }
            throw original;
        }
        refreshTrackedTopicIds(subscribedTopicNames, topicMetadata);
        return topicMetadata;
    }

    private void refreshTrackedTopicIds(
            Collection<String> subscribedTopicNames, Map<String, TopicDescription> topicMetadata) {

        // Add new subscribed topic to trackedTopicIdsByName
        for (String subscribedTopicName : subscribedTopicNames) {
            if (!trackedTopicIdsByName.containsKey(subscribedTopicName)) {
                final Uuid topicId = topicMetadata.get(subscribedTopicName).topicId();
                if (topicId == null || topicId.equals(Uuid.ZERO_UUID)) {
                    continue;
                }
                trackedTopicIdsByName.put(subscribedTopicName, topicId.toString());
            }
        }
        // Remove outdated topics from trackedTopicIdsByName
        for (String topicNameFromMapping : trackedTopicIdsByName.keySet()) {
            if (!subscribedTopicNames.contains(topicNameFromMapping)) {
                trackedTopicIdsByName.remove(topicNameFromMapping);
            }
        }
    }

    public Map<String, String> getTrackedTopicIdsByName() {
        return new HashMap<>(trackedTopicIdsByName);
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
            final String topicIdFromState = trackedTopicIdsByName.get(subscribedTopicName);
            final Uuid topicIdFromMetadata = topicDescription.topicId();
            if (topicIdFromState == null
                    || topicIdFromMetadata == null
                    || topicIdFromMetadata.equals(Uuid.ZERO_UUID)) {
                // we skip topic integrity check for null topicId
                // due to broker configuration, or topic not yet stored on trackedTopicIdsByName
                LOG.warn(
                        "Topic integrity check skipped due to a null topicId: topic name: {},"
                                + " topic id passed from initial config: {}"
                                + " current topic id on kafka server: {}",
                        subscribedTopicName,
                        topicIdFromState,
                        topicIdFromMetadata);
                continue;
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
