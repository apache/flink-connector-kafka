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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;

import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Tracks the recovery-time reader registration state of the {@link DynamicKafkaSourceEnumerator}.
 *
 * <p>After enumerator restore, assignments and metadata updates wait for the complete reader cohort
 * and the first metadata discovery. A local reader restart only waits for that reader's checkpoint
 * report to be reconciled with the surviving enumerator's current ownership.
 *
 * <p>This class owns that gating state; the enumerator remains responsible for acting on it.
 *
 * <p>This class is not thread-safe and must only be used from the coordinator thread: the
 * enumerator methods that touch it are called by the {@code SourceCoordinator}, and metadata
 * discovery results are handed back through {@code runInCoordinatorThread}.
 */
@Internal
class ReaderRecoveryGate {

    /** Set on restore; cleared once all readers have registered after the first discovery. */
    private boolean initialReaderRegistrationPending;

    /** Reported or returned splits awaiting recovery reassignment. */
    private final Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader =
            new HashMap<>();

    /** Readers whose checkpoint reports are being reconciled before pending assignments flush. */
    private final Set<Integer> readersAwaitingSplitReconciliation = new HashSet<>();

    /** Readers whose metadata update events were deferred during recovery. */
    private final Set<Integer> pendingMetadataUpdateReaders = new HashSet<>();

    ReaderRecoveryGate(boolean restoredFromCheckpoint) {
        this.initialReaderRegistrationPending = restoredFromCheckpoint;
    }

    /** Merges non-empty initial recovery reports with pending splits; empty reports are ignored. */
    void recordReportedSplits(int subtaskId, List<DynamicKafkaSourceSplit> reportedSplits) {
        if (initialReaderRegistrationPending && !reportedSplits.isEmpty()) {
            pendingReportedSplitsByReader.put(
                    subtaskId,
                    mergeReportedSplits(
                            pendingReportedSplitsByReader.get(subtaskId), reportedSplits));
        }
    }

    /** Retains splits returned while their reader is inactive until recovery reassignment. */
    void recordReturnedSplits(int readerId, List<DynamicKafkaSourceSplit> returnedSplits) {
        if (!returnedSplits.isEmpty()) {
            pendingReportedSplitsByReader.put(
                    readerId,
                    mergeReportedSplits(
                            pendingReportedSplitsByReader.get(readerId), returnedSplits));
        }
    }

    /** Returns a copy of the pending reports without draining them. */
    Map<Integer, List<DynamicKafkaSourceSplit>> snapshotReportedSplits() {
        Map<Integer, List<DynamicKafkaSourceSplit>> snapshot = new HashMap<>();
        pendingReportedSplitsByReader.forEach(
                (readerId, splits) -> snapshot.put(readerId, new ArrayList<>(splits)));
        return snapshot;
    }

    /**
     * Sub-enumerators can assign newly discovered splits while reported splits await reassignment.
     * A checkpoint may therefore contain both pending reports and reader-held splits, which must be
     * merged on restore. Preferring the current report for the same physical partition is
     * defensive.
     */
    private static List<DynamicKafkaSourceSplit> mergeReportedSplits(
            @Nullable List<DynamicKafkaSourceSplit> previousReportedSplits,
            List<DynamicKafkaSourceSplit> reportedSplits) {
        if (previousReportedSplits == null || previousReportedSplits.isEmpty()) {
            return new ArrayList<>(reportedSplits);
        }
        Map<Tuple2<String, TopicPartition>, DynamicKafkaSourceSplit> mergedByPartition =
                new LinkedHashMap<>();
        for (DynamicKafkaSourceSplit split : previousReportedSplits) {
            mergedByPartition.put(DynamicKafkaSourceEnumerator.toPartitionKey(split), split);
        }
        for (DynamicKafkaSourceSplit split : reportedSplits) {
            mergedByPartition.put(DynamicKafkaSourceEnumerator.toPartitionKey(split), split);
        }
        return new ArrayList<>(mergedByPartition.values());
    }

    /** Whether recovery gating is active and registrations must be deferred. */
    boolean hasPendingRecovery() {
        return initialReaderRegistrationPending;
    }

    /** Whether metadata updates must wait for checkpoint report reconciliation. */
    boolean shouldDeferMetadataUpdateEvents() {
        return initialReaderRegistrationPending || !readersAwaitingSplitReconciliation.isEmpty();
    }

    void deferMetadataUpdate(int readerId) {
        pendingMetadataUpdateReaders.add(readerId);
    }

    void deferMetadataUpdates(Collection<Integer> readerIds) {
        pendingMetadataUpdateReaders.addAll(readerIds);
    }

    /** Returns the deferred metadata update readers in ascending order and clears them. */
    List<Integer> drainDeferredMetadataUpdateReaders() {
        List<Integer> readers = new ArrayList<>(pendingMetadataUpdateReaders);
        Collections.sort(readers);
        pendingMetadataUpdateReaders.clear();
        return readers;
    }

    void startReaderRegistration(int readerId) {
        readersAwaitingSplitReconciliation.add(readerId);
    }

    void completeReaderRegistration(int readerId) {
        readersAwaitingSplitReconciliation.remove(readerId);
    }

    boolean isReaderReadyForAssignment(int readerId) {
        return !initialReaderRegistrationPending
                && !readersAwaitingSplitReconciliation.contains(readerId);
    }

    boolean isReconciliationPending() {
        return initialReaderRegistrationPending || !readersAwaitingSplitReconciliation.isEmpty();
    }

    void markInitialRegistrationComplete() {
        initialReaderRegistrationPending = false;
        readersAwaitingSplitReconciliation.clear();
    }

    boolean hasReportedSplits() {
        return !pendingReportedSplitsByReader.isEmpty();
    }

    /** Returns and removes only the pending splits for the recovering reader. */
    List<DynamicKafkaSourceSplit> drainReportedSplits(int readerId) {
        List<DynamicKafkaSourceSplit> reportedSplits =
                pendingReportedSplitsByReader.remove(readerId);
        return reportedSplits == null ? Collections.emptyList() : reportedSplits;
    }

    /**
     * Returns the reported splits ordered by reader id and clears the pending state.
     *
     * <p>The initial registration gate remains closed while these reports are reassigned.
     */
    NavigableMap<Integer, List<DynamicKafkaSourceSplit>> drainReportedSplits() {
        NavigableMap<Integer, List<DynamicKafkaSourceSplit>> reportedSplitsByReader =
                new TreeMap<>(pendingReportedSplitsByReader);
        pendingReportedSplitsByReader.clear();
        return reportedSplitsByReader;
    }
}
