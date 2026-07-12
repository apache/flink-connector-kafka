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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Unit tests for {@link TopicIntegrityProvider}.
 *
 * <p>{@link TopicIntegrityProvider#getVerifiedTopicMetadata} talks to a real {@link
 * org.apache.kafka.clients.admin.AdminClient}, so these tests drive it through {@link
 * TestAdminClient}, a thin wrapper around Kafka's own {@link MockAdminClient} test double.
 */
class TopicIntegrityProviderTest {

    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static MockAdminClient mockAdmin;

    @BeforeEach
    public void setup() {
        mockAdmin = new MockAdminClient();
    }

    @Test
    void testReturnsVerifiedTopics() throws Exception {
        String id = addTopic(TOPIC1);

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, id));

        Map<String, TopicDescription> result =
                provider.getVerifiedTopicMetadata(mockAdmin, Collections.singletonList(TOPIC1));

        assertThat(result).containsOnlyKeys(TOPIC1);
        assertThat(result.get(TOPIC1).topicId().toString()).isEqualTo(id);
    }

    @Test
    void testAddsNewlySubscribedTopicWithoutFailingIntegrityCheck() throws Exception {
        String id = addTopic(TOPIC1);

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(new HashMap<>());

        provider.getVerifiedTopicMetadata(mockAdmin, Collections.singletonList(TOPIC1));

        assertThat(provider.getTopicIntegrityMapping()).containsExactly(entry(TOPIC1, id));
    }

    @Test
    void testFailsIfTopicIsMissing() {
        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, Uuid.randomUuid().toString()));

        assertThatThrownBy(
                        () ->
                                provider.getVerifiedTopicMetadata(
                                        mockAdmin, Collections.singletonList(TOPIC1)))
                .isInstanceOf(TopicIntegrityException.class)
                .hasMessageContaining("Topic " + TOPIC1 + " is missing");
    }

    @Test
    void testFailsIfTopicWasRecreated() throws Exception {
        String originalId = addTopic(TOPIC1);

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, originalId));

        // Simulate recreation: delete and re-add under the same name, yielding a new id.
        mockAdmin.deleteTopics(Collections.singletonList(TOPIC1)).all().get();
        addTopic(TOPIC1);

        assertThatThrownBy(
                        () ->
                                provider.getVerifiedTopicMetadata(
                                        mockAdmin, Collections.singletonList(TOPIC1)))
                .isInstanceOf(TopicIntegrityException.class)
                .hasMessageContaining("Topic " + TOPIC1 + " was recreated");
    }

    @Test
    void testThrowsOriginalErrorWhenUnknownTopicExceptionIsNotDueToMissingTopic() throws Exception {
        String id = addTopic(TOPIC1);
        // Marked-for-deletion topics fail describeTopics with UnknownTopicOrPartitionException,
        // but MockAdminClient's listTopics still reports them - a stand-in for a topic that is
        // transiently unavailable rather than truly gone.
        mockAdmin.markTopicForDeletion(TOPIC1);

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, id));

        assertThatThrownBy(
                        () ->
                                provider.getVerifiedTopicMetadata(
                                        mockAdmin, Collections.singletonList(TOPIC1)))
                .isNotInstanceOf(TopicIntegrityException.class)
                .hasRootCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void testThrowsOriginalErrorForUnrelatedException() throws Exception {
        String id = addTopic(TOPIC1);
        mockAdmin.timeoutNextRequest(1);

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, id));

        assertThatThrownBy(
                        () ->
                                provider.getVerifiedTopicMetadata(
                                        mockAdmin, Collections.singletonList(TOPIC1)))
                .isNotInstanceOf(TopicIntegrityException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void testRemovesOutdatedTopicFromMapping() throws Exception {
        // TestAdminClient adminClient = new TestAdminClient();
        String id1 = addTopic(TOPIC1);

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        Map<String, String> tracked = new HashMap<>();
        tracked.put(TOPIC1, id1);
        tracked.put(TOPIC2, Uuid.randomUuid().toString());
        provider.open(tracked);

        // Only TOPIC1 is subscribed to anymore; TOPIC2 must be dropped from the tracked mapping.
        provider.getVerifiedTopicMetadata(mockAdmin, Collections.singletonList(TOPIC1));

        assertThat(provider.getTopicIntegrityMapping()).containsExactly(entry(TOPIC1, id1));
    }

    @Test
    void testPatternModeStillChecksTopicsThatDisappearedFromLiveMatch() {
        // TOPIC1 was tracked in a previous run but no longer exists on the cluster at all, so a
        // pattern-driven re-query alone would silently drop it from the set of names to verify.
        // MockAdminClient adminClient = new MockAdminClient();

        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, Uuid.randomUuid().toString()));

        assertThatThrownBy(
                        () -> provider.getVerifiedTopicMetadata(mockAdmin, Pattern.compile(".*")))
                .isInstanceOf(TopicIntegrityException.class)
                .hasMessageContaining("Topic " + TOPIC1 + " is missing");
    }

    @Test
    void testGetTopicIntegrityMappingReturnsDefensiveCopy() {
        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(Map.of(TOPIC1, Uuid.randomUuid().toString()));

        Map<String, String> mapping = provider.getTopicIntegrityMapping();
        mapping.put(TOPIC2, Uuid.randomUuid().toString());

        assertThat(provider.getTopicIntegrityMapping()).doesNotContainKey(TOPIC2);
    }

    @Test
    void testEmptySubscriptionReturnsEmptyMetadataWithoutError() {
        TopicIntegrityProvider provider = new TopicIntegrityProvider();
        provider.open(new HashMap<>());

        Map<String, TopicDescription> result =
                provider.getVerifiedTopicMetadata(mockAdmin, Collections.emptyList());

        assertThat(result).isEmpty();
        assertThat(provider.getTopicIntegrityMapping()).isEmpty();
    }

    private static final String addTopic(String name) throws Exception {
        mockAdmin.addTopic(false, name, Collections.emptyList(), Collections.emptyMap());
        return mockAdmin
                .describeTopics(Collections.singletonList(name))
                .allTopicNames()
                .get()
                .get(name)
                .topicId()
                .toString();
    }

    /** Adds {@code name} with a fresh topic id and returns that id. */
    // private static final class TestAdminClient extends MockAdminClient {
    //     String addTopic(String name) throws Exception {
    //         addTopic(false, name, Collections.emptyList(), Collections.emptyMap());
    //         return describeTopics(Collections.singletonList(name))
    //                 .allTopicNames()
    //                 .get()
    //                 .get(name)
    //                 .topicId()
    //                 .toString();
    //     }
    // }
}
