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

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Holds a former owner's cleanup until the new owner has emitted records in the real job. */
final class RetainedCleanupBarrier {
    private final String cluster;
    private final TopicPartition partition;
    private final CountDownLatch cleanupReached = new CountDownLatch(1);
    private final CountDownLatch releaseCleanup = new CountDownLatch(1);
    private volatile DynamicKafkaSourceSplit heldShadow;
    private volatile List<DynamicKafkaSourceSplit> restoredState = List.of();

    RetainedCleanupBarrier(String cluster, TopicPartition partition) {
        this.cluster = cluster;
        this.partition = partition;
    }

    void beforeCleanup(List<DynamicKafkaSourceSplit> readerState) {
        heldShadow =
                readerState.stream()
                        .filter(
                                split ->
                                        split.isRetained()
                                                && split.getKafkaClusterId().equals(cluster)
                                                && split.getTopicPartition().equals(partition))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing former-owner shadow"));
        cleanupReached.countDown();
        try {
            releaseCleanup.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while holding former-owner cleanup", interrupted);
        }
    }

    void awaitNewOwnerProgress(Supplier<Map<Integer, Integer>> observedOwners, int start, int end)
            throws Exception {
        assertThat(cleanupReached.await(30, TimeUnit.SECONDS))
                .as("former owner reached cleanup while retaining its checkpoint shadow")
                .isTrue();
        CommonTestUtils.waitUtil(
                () -> {
                    Map<Integer, Integer> owners = observedOwners.get();
                    return IntStream.range(start, end)
                            .allMatch(record -> Integer.valueOf(1).equals(owners.get(record)));
                },
                Duration.ofSeconds(30),
                "New owner did not emit records while former-owner cleanup was held");
    }

    void releaseCleanup() {
        releaseCleanup.countDown();
    }

    void recordRestoredState(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context, int subtaskId) {
        if (subtaskId == 0) {
            restoredState =
                    List.copyOf(
                            context.registeredReaders()
                                    .get(subtaskId)
                                    .getReportedSplitsOnRegistration());
        }
    }

    static boolean isCleanupEvent(SourceEvent event, String cluster) {
        if (!(event instanceof MetadataUpdateEvent)) {
            return false;
        }
        MetadataUpdateEvent metadata = (MetadataUpdateEvent) event;
        return !metadata.getRetainedClusterDeadlines().containsKey(cluster)
                && metadata.getKafkaStreams().stream()
                        .anyMatch(stream -> stream.getClusterMetadataMap().containsKey(cluster));
    }

    void assertRestoredShadow() {
        assertThat(restoredState)
                .as(
                        "local failure restored the retained shadow from the checkpoint before"
                                + " transfer")
                .contains(heldShadow);
    }
}
