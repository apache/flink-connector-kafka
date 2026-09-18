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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Offline regressions for physical-partition ownership during returned-split reconciliation. */
class KafkaSourceEnumeratorPendingOwnershipRegressionTest {

    @Test
    void sameOwnerSameOffsetIsIdempotentControl() throws Exception {
        assertSinglePendingPhysicalSplit(0, 0, 100L, 100L);
    }

    @Test
    void changedOwnerMustNotRetainPreviousPendingOwner() throws Exception {
        assertSinglePendingPhysicalSplit(0, 1, 100L, 100L);
    }

    @Test
    void changedOwnerAndOffsetMustNotRetainPreviousPendingCopy() throws Exception {
        assertSinglePendingPhysicalSplit(0, 1, 100L, 200L);
    }

    @Test
    void changedOffsetMustNotRetainTwoCopiesOnSameOwner() throws Exception {
        assertSinglePendingPhysicalSplit(0, 0, 100L, 200L);
    }

    @Test
    void lowerCheckpointOffsetReplacesHigherPendingOffset() throws Exception {
        assertSinglePendingPhysicalSplit(0, 1, 200L, 100L);
    }

    @ParameterizedTest
    @CsvSource({"100, 200", "200, 100"})
    void sameBatchKeepsLastPhysicalSplitAndPreservesOtherPartitions(
            long firstOffset, long lastOffset) throws Exception {
        TopicPartition partition = new TopicPartition("topic", 0);
        KafkaPartitionSplit unrelated =
                new KafkaPartitionSplit(new TopicPartition("topic", 1), 500L);
        KafkaPartitionSplit expected = new KafkaPartitionSplit(partition, lastOffset);
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(1);
                KafkaSourceEnumerator enumerator =
                        new KafkaSourceEnumerator(
                                client -> Collections.emptySet(),
                                OffsetsInitializer.earliest(),
                                OffsetsInitializer.earliest(),
                                new Properties(),
                                context,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new KafkaSourceEnumState(
                                        Collections.emptyList(), Collections.emptyList(), true))) {
            enumerator.addSplitsBack(List.of(unrelated), 0);
            enumerator.addSplitsBack(
                    List.of(new KafkaPartitionSplit(partition, firstOffset), expected), 0);
            assertThat(enumerator.snapshotState(1L).unassignedSplits())
                    .containsExactlyInAnyOrder(unrelated, expected);

            context.registerReader(new ReaderInfo(0, "reader-0"));
            enumerator.addReader(0);

            assertThat(context.getSplitsAssignmentSequence()).hasSize(1);
            assertThat(context.getSplitsAssignmentSequence().get(0).assignment().get(0))
                    .containsExactlyInAnyOrder(unrelated, expected);
            assertThat(enumerator.snapshotState(2L).assignedSplits())
                    .containsExactlyInAnyOrder(unrelated, expected);
        }
    }

    private void assertSinglePendingPhysicalSplit(
            int firstOwner, int secondOwner, long firstOffset, long secondOffset) throws Exception {
        AtomicInteger owner = new AtomicInteger(firstOwner);
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                new MockSplitEnumeratorContext<>(2)) {
            KafkaSubscriber subscriber = client -> Collections.emptySet();
            KafkaSourceEnumerator enumerator =
                    new KafkaSourceEnumerator(
                            subscriber,
                            OffsetsInitializer.earliest(),
                            OffsetsInitializer.earliest(),
                            new Properties(),
                            context,
                            Boundedness.CONTINUOUS_UNBOUNDED,
                            new KafkaSourceEnumState(
                                    Collections.emptySet(), Collections.emptySet(), true),
                            (split, readers) -> owner.get());
            TopicPartition partition = new TopicPartition("topic", 2);
            enumerator.addSplitsBack(
                    Collections.singletonList(new KafkaPartitionSplit(partition, firstOffset)),
                    firstOwner);
            owner.set(secondOwner);
            enumerator.addSplitsBack(
                    Collections.singletonList(new KafkaPartitionSplit(partition, secondOffset)),
                    secondOwner);
            for (int reader = 0; reader < 2; reader++) {
                context.registerReader(new ReaderInfo(reader, "reader-" + reader));
                enumerator.addReader(reader);
            }
            List<String> emitted =
                    context.getSplitsAssignmentSequence().stream()
                            .flatMap(a -> a.assignment().entrySet().stream())
                            .flatMap(e -> e.getValue().stream().map(s -> e.getKey() + ":" + s))
                            .collect(Collectors.toList());
            assertThat(emitted)
                    .as("one pending assignment for physical topic partition %s", partition)
                    .hasSize(1);
            assertThat(context.getSplitsAssignmentSequence().get(0).assignment())
                    .containsOnlyKeys(secondOwner);
            assertThat(context.getSplitsAssignmentSequence().get(0).assignment().get(secondOwner))
                    .containsExactly(new KafkaPartitionSplit(partition, secondOffset));
        }
    }
}
