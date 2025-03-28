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

package org.apache.flink.connector.kafka.util;

import org.apache.flink.annotation.Internal;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utility methods for Kafka admin operations. */
@Internal
public class AdminUtils {

    private AdminUtils() {}

    public static Map<String, TopicDescription> getAllTopicMetadata(AdminClient adminClient) {
        try {
            Set<String> allTopicNames = adminClient.listTopics().names().get();
            return getTopicMetadata(adminClient, allTopicNames);
        } catch (Exception e) {
            checkIfInterrupted(e);
            throw new RuntimeException("Failed to get metadata for all topics.", e);
        }
    }

    public static Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Pattern topicPattern) {
        try {
            Set<String> matchedTopicNames = getTopicsByPattern(adminClient, topicPattern);
            return getTopicMetadata(adminClient, matchedTopicNames);
        } catch (Exception e) {
            checkIfInterrupted(e);
            throw new RuntimeException(
                    String.format("Failed to get metadata for %s topics.", topicPattern.pattern()),
                    e);
        }
    }

    public static Set<String> getTopicsByPattern(AdminClient adminClient, Pattern topicPattern) {
        try {
            Set<String> allTopicNames = adminClient.listTopics().names().get();
            return allTopicNames.stream()
                    .filter(name -> topicPattern.matcher(name).matches())
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            checkIfInterrupted(e);
            throw new RuntimeException(
                    String.format("Failed to get metadata for %s topics.", topicPattern.pattern()),
                    e);
        }
    }

    public static Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Collection<String> topicNames) {
        try {
            return adminClient.describeTopics(topicNames).allTopicNames().get();
        } catch (Exception e) {
            checkIfInterrupted(e);
            throw new RuntimeException(
                    String.format("Failed to get metadata for topics %s.", topicNames), e);
        }
    }

    public static Map<TopicPartition, DescribeProducersResult.PartitionProducerState>
            getProducerStates(AdminClient adminClient, Collection<String> topicNames) {
        try {
            return adminClient
                    .describeProducers(getTopicPartitions(adminClient, topicNames))
                    .all()
                    .get();
        } catch (Exception e) {
            checkIfInterrupted(e);
            throw new RuntimeException(
                    String.format("Failed to get producers for topics %s.", topicNames), e);
        }
    }

    public static Collection<Long> getProducerIds(
            AdminClient adminClient, Collection<String> topicNames) {
        return getProducerStates(adminClient, topicNames).values().stream()
                .flatMap(
                        producerState ->
                                producerState.activeProducers().stream()
                                        .map(ProducerState::producerId))
                .collect(Collectors.toList());
    }

    public static Collection<TransactionListing> getOpenTransactionsForTopics(
            AdminClient adminClient, Collection<String> topicNames) {
        try {
            return adminClient
                    .listTransactions(
                            new ListTransactionsOptions()
                                    .filterProducerIds(getProducerIds(adminClient, topicNames))
                                    .filterStates(List.of(TransactionState.ONGOING)))
                    .all()
                    .get();
        } catch (Exception e) {
            checkIfInterrupted(e);
            throw new RuntimeException(
                    String.format("Failed to get open transactions for topics %s.", topicNames), e);
        }
    }

    private static void checkIfInterrupted(Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    public static List<TopicPartition> getTopicPartitions(
            AdminClient adminClient, Collection<String> topicNames) {
        return getTopicMetadata(adminClient, topicNames).values().stream()
                .flatMap(
                        t ->
                                t.partitions().stream()
                                        .map(p -> new TopicPartition(t.name(), p.partition())))
                .collect(Collectors.toList());
    }
}
