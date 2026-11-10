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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReaderRecoveryGate}. */
class ReaderRecoveryGateTest {

    @Test
    void testFreshStartHasNoPendingRecovery() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(false);

        assertThat(gate.hasPendingRecovery()).isFalse();
        assertThat(gate.shouldDeferMetadataUpdateEvents()).isFalse();
        assertThat(gate.hasReportedSplits()).isFalse();
    }

    @Test
    void testRestoredStartGatesUntilInitialRegistrationCompletes() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);

        assertThat(gate.hasPendingRecovery()).isTrue();
        assertThat(gate.shouldDeferMetadataUpdateEvents()).isTrue();

        gate.markInitialRegistrationComplete();

        assertThat(gate.hasPendingRecovery()).isFalse();
        assertThat(gate.shouldDeferMetadataUpdateEvents()).isFalse();
    }

    @Test
    void testDrainingReportsDoesNotReleaseInitialAssignmentGate() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        gate.recordReportedSplits(1, Collections.singletonList(split("topic", 0)));
        gate.startReaderRegistration(1);

        gate.drainReportedSplits();

        assertThat(gate.hasPendingRecovery()).isTrue();
        assertThat(gate.isReaderReadyForAssignment(1)).isFalse();
        assertThat(gate.shouldDeferMetadataUpdateEvents()).isTrue();

        gate.markInitialRegistrationComplete();

        assertThat(gate.hasPendingRecovery()).isFalse();
        assertThat(gate.isReaderReadyForAssignment(1)).isTrue();
        assertThat(gate.isReconciliationPending()).isFalse();
    }

    @Test
    void testLocalRegistrationOnlyBlocksTheRecoveringReader() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(false);
        gate.startReaderRegistration(1);
        gate.recordReportedSplits(1, Collections.singletonList(split("topic", 0)));

        assertThat(gate.hasPendingRecovery()).isFalse();
        assertThat(gate.hasReportedSplits()).isFalse();
        assertThat(gate.isReaderReadyForAssignment(0)).isTrue();
        assertThat(gate.isReaderReadyForAssignment(1)).isFalse();
        assertThat(gate.shouldDeferMetadataUpdateEvents()).isTrue();

        gate.completeReaderRegistration(1);

        assertThat(gate.isReaderReadyForAssignment(1)).isTrue();
        assertThat(gate.isReconciliationPending()).isFalse();
        assertThat(gate.shouldDeferMetadataUpdateEvents()).isFalse();
    }

    @Test
    void testEmptyReportDuringInitialRecovery() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        gate.recordReportedSplits(1, Collections.emptyList());

        assertThat(gate.hasPendingRecovery()).isTrue();
        assertThat(gate.hasReportedSplits()).isFalse();
        assertThat(gate.drainReportedSplits()).isEmpty();
        assertThat(gate.hasPendingRecovery()).isTrue();
    }

    @Test
    void testRepeatedReportMergesSplitsAndPrefersLatestOffsets() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        DynamicKafkaSourceSplit firstReport = split("topic", 0);
        DynamicKafkaSourceSplit secondReport = split("topic", 1);
        DynamicKafkaSourceSplit updatedSplit =
                new DynamicKafkaSourceSplit(
                        "cluster0", new KafkaPartitionSplit(new TopicPartition("topic", 0), 42L));
        DynamicKafkaSourceSplit previousOnlySplit = split("topic", 2);
        gate.recordReportedSplits(1, Arrays.asList(firstReport, previousOnlySplit));
        gate.recordReportedSplits(1, Arrays.asList(updatedSplit, secondReport));

        NavigableMap<Integer, List<DynamicKafkaSourceSplit>> drained = gate.drainReportedSplits();

        assertThat(drained.keySet()).containsExactly(1);
        assertThat(drained.get(1))
                .containsExactlyInAnyOrder(updatedSplit, previousOnlySplit, secondReport);
    }

    @Test
    void testSnapshotReportedSplitsIsIndependentAndDoesNotDrain() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        DynamicKafkaSourceSplit firstReport = split("topic", 0);
        DynamicKafkaSourceSplit secondReport = split("topic", 1);
        gate.recordReportedSplits(1, Collections.singletonList(firstReport));

        Map<Integer, List<DynamicKafkaSourceSplit>> snapshot = gate.snapshotReportedSplits();
        assertThat(gate.hasReportedSplits()).isTrue();
        snapshot.get(1).clear();
        snapshot.clear();
        assertThat(gate.snapshotReportedSplits().get(1)).containsExactly(firstReport);

        snapshot = gate.snapshotReportedSplits();
        gate.recordReportedSplits(1, Collections.singletonList(secondReport));
        assertThat(gate.drainReportedSplits().get(1))
                .containsExactlyInAnyOrder(firstReport, secondReport);
        assertThat(gate.snapshotReportedSplits()).isEmpty();
        assertThat(snapshot).containsOnlyKeys(1);
        assertThat(snapshot.get(1)).containsExactly(firstReport);
    }

    @Test
    void testEmptyRepeatedReportPreservesPendingSplits() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        DynamicKafkaSourceSplit firstReport = split("topic", 0);
        gate.recordReportedSplits(1, Collections.singletonList(firstReport));
        gate.recordReportedSplits(1, Collections.emptyList());

        assertThat(gate.drainReportedSplits().get(1)).containsExactly(firstReport);
    }

    @Test
    void testReturnedSplitsRemainPendingWithoutInitialRecoveryGate() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(false);
        DynamicKafkaSourceSplit firstReturnedSplit = split("topic", 0);
        DynamicKafkaSourceSplit secondReturnedSplit = split("topic", 1);
        DynamicKafkaSourceSplit updatedSplit =
                new DynamicKafkaSourceSplit(
                        "cluster0", new KafkaPartitionSplit(new TopicPartition("topic", 0), 42L));
        gate.recordReturnedSplits(1, Collections.singletonList(firstReturnedSplit));
        gate.recordReturnedSplits(1, Arrays.asList(updatedSplit, secondReturnedSplit));

        assertThat(gate.hasPendingRecovery()).isFalse();
        assertThat(gate.hasReportedSplits()).isTrue();
        assertThat(gate.snapshotReportedSplits().get(1))
                .containsExactlyInAnyOrder(updatedSplit, secondReturnedSplit);
        assertThat(gate.snapshotReportedSplits().get(1).get(0).getStartingOffset()).isEqualTo(42L);
    }

    @Test
    void testDrainReportedSplitsForOneReaderPreservesOtherReaders() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(false);
        DynamicKafkaSourceSplit splitReader1 = split("topic", 1);
        DynamicKafkaSourceSplit splitReader2 = split("topic", 2);
        gate.recordReturnedSplits(1, Collections.singletonList(splitReader1));
        gate.recordReturnedSplits(2, Collections.singletonList(splitReader2));

        assertThat(gate.drainReportedSplits(1)).containsExactly(splitReader1);
        assertThat(gate.drainReportedSplits(1)).isEmpty();
        assertThat(gate.snapshotReportedSplits()).containsOnlyKeys(2);
        assertThat(gate.snapshotReportedSplits().get(2)).containsExactly(splitReader2);
    }

    @Test
    void testEmptyReturnedSplitsAreIgnored() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(false);
        gate.recordReturnedSplits(1, Collections.emptyList());

        assertThat(gate.hasPendingRecovery()).isFalse();
        assertThat(gate.hasReportedSplits()).isFalse();
        assertThat(gate.snapshotReportedSplits()).isEmpty();
        assertThat(gate.drainReportedSplits(1)).isEmpty();
    }

    @Test
    void testMergeKeepsDistinctPartitionsWithAmbiguousClusterAndTopicIds() {
        // Cluster "a" topic "b-c" and cluster "a-b" topic "c" are the same partition name once the
        // cluster id and the partition id are joined, so the split ids have to stay distinct.
        DynamicKafkaSourceSplit firstSplit =
                new DynamicKafkaSourceSplit(
                        "a", new KafkaPartitionSplit(new TopicPartition("b-c", 0), 123L));
        DynamicKafkaSourceSplit secondSplit =
                new DynamicKafkaSourceSplit(
                        "a-b", new KafkaPartitionSplit(new TopicPartition("c", 0), 456L));
        DynamicKafkaSourceSplit returnedSplit =
                new DynamicKafkaSourceSplit(
                        "a-b", new KafkaPartitionSplit(new TopicPartition("c", 0), 789L));
        assertThat(firstSplit.splitId()).isNotEqualTo(secondSplit.splitId());

        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        gate.recordReportedSplits(0, Collections.singletonList(firstSplit));
        gate.recordReportedSplits(0, Collections.singletonList(secondSplit));
        assertThat(gate.snapshotReportedSplits().get(0)).containsExactly(firstSplit, secondSplit);

        gate.recordReturnedSplits(0, Collections.singletonList(returnedSplit));
        assertThat(gate.snapshotReportedSplits().get(0)).containsExactly(firstSplit, returnedSplit);
    }

    @Test
    void testDrainReportedSplitsReturnsReaderOrderAndClears() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        DynamicKafkaSourceSplit splitReader2 = split("topic", 2);
        DynamicKafkaSourceSplit splitReader0 = split("topic", 0);
        gate.recordReportedSplits(2, Collections.singletonList(splitReader2));
        gate.recordReportedSplits(0, Collections.singletonList(splitReader0));

        NavigableMap<Integer, List<DynamicKafkaSourceSplit>> drained = gate.drainReportedSplits();

        assertThat(drained.keySet()).containsExactly(0, 2);
        assertThat(drained.get(0)).containsExactly(splitReader0);
        assertThat(drained.get(2)).containsExactly(splitReader2);
        assertThat(gate.hasReportedSplits()).isFalse();
        assertThat(gate.drainReportedSplits()).isEmpty();
    }

    @Test
    void testDrainDeferredMetadataUpdateReadersReturnsSortedAndClears() {
        ReaderRecoveryGate gate = new ReaderRecoveryGate(true);
        gate.deferMetadataUpdate(3);
        gate.deferMetadataUpdates(Arrays.asList(1, 2, 3));

        assertThat(gate.drainDeferredMetadataUpdateReaders()).containsExactly(1, 2, 3);
        assertThat(gate.drainDeferredMetadataUpdateReaders()).isEmpty();
    }

    private static DynamicKafkaSourceSplit split(String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                "cluster0", new KafkaPartitionSplit(new TopicPartition(topic, partition), 0L));
    }
}
