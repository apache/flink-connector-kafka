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
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.mock.Whitebox;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Replays delayed initialization callbacks against newer authoritative checkpoint splits. */
class KafkaSourceEnumeratorRecoveryDiscoveryRaceTest {
    private static final TopicPartition PARTITION = new TopicPartition("topic", 0);
    private static final long CHECKPOINT_STOP = 1500L;
    private static final long DISCOVERY_STOP = 2000L;

    @ParameterizedTest
    @CsvSource({
        "120, 900, false",
        "900, 120, false",
        "120, 900, true",
        "900, 120, true",
        "-2, 900, false",
        "-3, 900, false"
    })
    void checkpointSplitWinsOverDelayedDiscoveryRegardlessOfOffsetOrder(
            long checkpointOffset, long discoveryOffset, boolean alreadyRegistered)
            throws Throwable {
        KafkaPartitionSplit checkpoint =
                new KafkaPartitionSplit(PARTITION, checkpointOffset, CHECKPOINT_STOP);
        try (HoldingCompletionContext context = new HoldingCompletionContext();
                KafkaSourceEnumerator enumerator =
                        newEnumerator(context, Collections.emptyList(), discoveryOffset)) {
            discover(enumerator);
            // Compute the real worker result first, but hold its real completion callback.
            context.runNextOneTimeCallable();
            assertThat(context.completions).hasSize(1);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            if (alreadyRegistered) {
                registerReader(context, enumerator);
            }
            enumerator.addSplitsBack(Collections.singletonList(checkpoint), 0);
            KafkaSourceEnumState before = enumerator.snapshotState(1L);
            assertThat(alreadyRegistered ? before.assignedSplits() : before.unassignedSplits())
                    .containsExactly(checkpoint);
            assertThat(before.initialDiscoveryFinished()).isFalse();

            context.deliverCompletion();
            KafkaSourceEnumState afterCallback = enumerator.snapshotState(2L);
            List<KafkaPartitionSplit> pendingAfterCallback =
                    enumerator.getPendingPartitionSplitAssignment().values().stream()
                            .flatMap(Set::stream)
                            .collect(Collectors.toList());
            if (!alreadyRegistered) {
                registerReader(context, enumerator);
            }
            KafkaSourceEnumState afterRegistration = enumerator.snapshotState(3L);
            // Collect both persisted-state and emitted-assignment failures in the red baseline.
            assertSoftly(
                    softly -> {
                        softly.assertThat(afterCallback.initialDiscoveryFinished()).isTrue();
                        softly.assertThat(
                                        alreadyRegistered
                                                ? afterCallback.assignedSplits()
                                                : afterCallback.unassignedSplits())
                                .as(
                                        "checkpoint split survives the delayed callback in snapshot state")
                                .containsExactly(checkpoint);
                        softly.assertThat(
                                        alreadyRegistered
                                                ? afterCallback.unassignedSplits()
                                                : afterCallback.assignedSplits())
                                .isEmpty();
                        if (alreadyRegistered) {
                            softly.assertThat(pendingAfterCallback).isEmpty();
                        } else {
                            softly.assertThat(pendingAfterCallback).containsExactly(checkpoint);
                        }
                        softly.assertThat(emittedSplits(context))
                                .as(
                                        "exactly one checkpoint assignment; never the stale discovery copy")
                                .containsExactly(checkpoint);
                        softly.assertThat(afterRegistration.assignedSplits())
                                .containsExactly(checkpoint);
                        softly.assertThat(afterRegistration.unassignedSplits()).isEmpty();
                    });
            assertCompletionBookkeeping(context, enumerator);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void absentOrUnresolvedMigratedPartitionStillAcceptsInitialization(boolean migrated)
            throws Throwable {
        List<KafkaPartitionSplit> restored =
                migrated
                        ? Collections.singletonList(
                                new KafkaPartitionSplit(PARTITION, KafkaPartitionSplit.MIGRATED))
                        : Collections.emptyList();
        KafkaPartitionSplit initialized = new KafkaPartitionSplit(PARTITION, 900L, DISCOVERY_STOP);
        try (HoldingCompletionContext context = new HoldingCompletionContext();
                KafkaSourceEnumerator enumerator = newEnumerator(context, restored, 900L)) {
            discover(enumerator);
            context.runNextOneTimeCallable();
            assertThat(context.completions).hasSize(1);
            context.deliverCompletion();
            KafkaSourceEnumState pending = enumerator.snapshotState(1L);
            assertThat(pending.unassignedSplits()).containsExactly(initialized);
            assertThat(pending.assignedSplits()).isEmpty();
            registerReader(context, enumerator);
            assertThat(emittedSplits(context)).containsExactly(initialized);
            KafkaSourceEnumState assigned = enumerator.snapshotState(2L);
            assertThat(assigned.assignedSplits()).containsExactly(initialized);
            assertThat(assigned.unassignedSplits()).isEmpty();
            assertCompletionBookkeeping(context, enumerator);
        }
    }

    private static KafkaSourceEnumerator newEnumerator(
            HoldingCompletionContext context,
            List<KafkaPartitionSplit> restored,
            long discoveryOffset) {
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        KafkaSubscriber subscriber = client -> Collections.singleton(PARTITION);
        KafkaSourceEnumerator enumerator =
                new KafkaSourceEnumerator(
                        subscriber,
                        OffsetsInitializer.offsets(
                                Collections.singletonMap(PARTITION, discoveryOffset)),
                        OffsetsInitializer.offsets(
                                Collections.singletonMap(PARTITION, DISCOVERY_STOP)),
                        properties,
                        context,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        new KafkaSourceEnumState(Collections.emptyList(), restored, false));
        // No broker is involved; the production initializer resolves the specified offsets.
        Whitebox.setInternalState(enumerator, "adminClient", new MockAdminClient());
        return enumerator;
    }

    private static void discover(KafkaSourceEnumerator enumerator) throws Exception {
        Method discover =
                KafkaSourceEnumerator.class.getDeclaredMethod(
                        "checkPartitionChanges", Set.class, Throwable.class);
        discover.setAccessible(true);
        discover.invoke(enumerator, Collections.singleton(PARTITION), null);
    }

    private static void registerReader(
            HoldingCompletionContext context, KafkaSourceEnumerator enumerator) {
        context.registerReader(new ReaderInfo(0, "reader-0"));
        enumerator.addReader(0);
    }

    private static List<KafkaPartitionSplit> emittedSplits(HoldingCompletionContext context) {
        return context.getSplitsAssignmentSequence().stream()
                .flatMap(assignment -> assignment.assignment().values().stream())
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private static void assertCompletionBookkeeping(
            HoldingCompletionContext context, KafkaSourceEnumerator enumerator) throws Exception {
        assertThat(enumerator.snapshotState(4L).initialDiscoveryFinished()).isTrue();
        discover(enumerator);
        assertThat(context.getOneTimeCallables()).isEmpty();
        assertThat(context.completions).isEmpty();
    }

    /** Holds only delivery of the actual initializer result, not its computation or split state. */
    private static class HoldingCompletionContext
            extends MockSplitEnumeratorContext<KafkaPartitionSplit> {
        private final Deque<Runnable> completions = new ArrayDeque<>();

        private HoldingCompletionContext() {
            super(1);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            super.callAsync(
                    callable,
                    (result, error) -> completions.addLast(() -> handler.accept(result, error)));
        }

        private void deliverCompletion() {
            completions.removeFirst().run();
        }
    }
}
