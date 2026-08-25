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
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;

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
 * <p>The gate is armed by two recovery triggers:
 *
 * <ul>
 *   <li><b>Enumerator restore from checkpoint</b>: armed at construction ({@code
 *       restoredFromCheckpoint = true}). Split assignment and metadata update events must be
 *       deferred until the first metadata discovery has completed and every reader has
 *       (re-)registered, so that restored reader splits can be redistributed consistently.
 *   <li><b>Reader re-registration after partial failover</b>: armed on a running enumerator when a
 *       re-registering reader reports checkpointed splits ({@link #recordReportedSplits} with
 *       non-empty splits while initial reader registration is already complete). Metadata update
 *       events are deferred until all readers have registered again and reported splits are
 *       redistributed.
 * </ul>
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

    /** Splits reported by readers on registration, pending redistribution. */
    private final Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader =
            new HashMap<>();

    /** Readers whose metadata update events were deferred during recovery. */
    private final Set<Integer> pendingMetadataUpdateReaders = new HashSet<>();

    ReaderRecoveryGate(boolean restoredFromCheckpoint) {
        this.initialReaderRegistrationPending = restoredFromCheckpoint;
    }

    /** Merges a reader's registration report with pending splits; an empty report is ignored. */
    void recordReportedSplits(int subtaskId, List<DynamicKafkaSourceSplit> reportedSplits) {
        if (!reportedSplits.isEmpty()) {
            pendingReportedSplitsByReader.put(
                    subtaskId,
                    mergeReportedSplits(
                            pendingReportedSplitsByReader.get(subtaskId), reportedSplits));
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
     * Pending entries can come from earlier registrations or remapped checkpoint state. Merging by
     * split id, preferring the current report, avoids both losing and duplicating splits.
     */
    private static List<DynamicKafkaSourceSplit> mergeReportedSplits(
            @Nullable List<DynamicKafkaSourceSplit> previousReportedSplits,
            List<DynamicKafkaSourceSplit> reportedSplits) {
        if (previousReportedSplits == null || previousReportedSplits.isEmpty()) {
            return new ArrayList<>(reportedSplits);
        }
        Map<String, DynamicKafkaSourceSplit> mergedBySplitId = new LinkedHashMap<>();
        for (DynamicKafkaSourceSplit split : previousReportedSplits) {
            mergedBySplitId.put(split.splitId(), split);
        }
        for (DynamicKafkaSourceSplit split : reportedSplits) {
            mergedBySplitId.put(split.splitId(), split);
        }
        return new ArrayList<>(mergedBySplitId.values());
    }

    /** Whether recovery gating is active and registrations must be deferred. */
    boolean hasPendingRecovery() {
        return initialReaderRegistrationPending || !pendingReportedSplitsByReader.isEmpty();
    }

    /**
     * Whether metadata update events must be deferred instead of sent, given the current reader
     * registration completeness.
     */
    boolean shouldDeferMetadataUpdateEvents(boolean allReadersRegistered) {
        return initialReaderRegistrationPending
                || (!pendingReportedSplitsByReader.isEmpty() && !allReadersRegistered);
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

    void markInitialRegistrationComplete() {
        initialReaderRegistrationPending = false;
    }

    boolean hasReportedSplits() {
        return !pendingReportedSplitsByReader.isEmpty();
    }

    /**
     * Returns the reported splits ordered by reader id and clears the pending state.
     *
     * <p>Note: the pending state is cleared eagerly, so the gate must not be consulted for pending
     * reported splits while reassigning (e.g. from {@code handleNoMoreSplits}).
     */
    NavigableMap<Integer, List<DynamicKafkaSourceSplit>> drainReportedSplits() {
        NavigableMap<Integer, List<DynamicKafkaSourceSplit>> reportedSplitsByReader =
                new TreeMap<>(pendingReportedSplitsByReader);
        pendingReportedSplitsByReader.clear();
        return reportedSplitsByReader;
    }
}
