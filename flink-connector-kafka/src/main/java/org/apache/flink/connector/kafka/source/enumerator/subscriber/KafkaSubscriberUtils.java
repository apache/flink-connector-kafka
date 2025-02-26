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

import org.apache.flink.connector.kafka.source.KafkaSourceOptions;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** The base implementations of {@link KafkaSubscriber}. */
class KafkaSubscriberUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSubscriberUtils.class);

    private KafkaSubscriberUtils() {}

    static Map<String, TopicDescription> getAllTopicMetadata(
            AdminClient adminClient, Properties properties) {
        try {
            Set<String> allTopicNames = adminClient.listTopics().names().get();
            return getTopicMetadata(adminClient, allTopicNames, properties);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get metadata for all topics.", e);
        }
    }

    static Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Pattern topicPattern, Properties properties) {
        try {
            Set<String> allTopicNames = adminClient.listTopics().names().get();
            Set<String> matchedTopicNames =
                    allTopicNames.stream()
                            .filter(name -> topicPattern.matcher(name).matches())
                            .collect(Collectors.toSet());
            return getTopicMetadata(adminClient, matchedTopicNames, properties);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to get metadata for %s topics.", topicPattern.pattern()),
                    e);
        }
    }

    static Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Set<String> topicNames, Properties properties) {
        int maxRetries =
                KafkaSourceOptions.getOption(
                        properties,
                        KafkaSourceOptions.TOPIC_METADATA_REQUEST_MAX_RETRY,
                        Integer::parseInt);
        long retryDelay =
                KafkaSourceOptions.getOption(
                        properties,
                        KafkaSourceOptions.TOPIC_METADATA_REQUEST_RETRY_INTERVAL_MS,
                        Long::parseLong);
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return adminClient.describeTopics(topicNames).allTopicNames().get();
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw new RuntimeException(
                            String.format("Failed to get metadata for topics %s.", topicNames), e);
                } else {
                    LOG.warn(
                            "Attempt {} to get metadata for topics {} failed. Retrying in {} ms...",
                            attempt,
                            topicNames,
                            retryDelay);
                    try {
                        TimeUnit.MILLISECONDS.sleep(retryDelay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); // Restore interrupted state
                        LOG.error("Thread was interrupted during metadata fetch retry delay.", ie);
                    }
                }
            }
        }
        // This statement will never be reached because either a valid result is returned or an
        // exception is thrown.
        throw new IllegalStateException(
                "Unexpected error in getTopicMetadata: reached unreachable code.");
    }
}
