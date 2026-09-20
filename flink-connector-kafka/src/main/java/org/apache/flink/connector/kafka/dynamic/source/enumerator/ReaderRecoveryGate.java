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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    /** Splits reported by readers on registration, pending redistribution. */
    private final Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader =
            new HashMap<>();

    /** Readers whose checkpoint reports are being reconciled before pending assignments flush. */
    private final Set<Integer> readersAwaitingSplitReconciliation = new HashSet<>();

    /** Readers whose metadata update events were deferred during recovery. */
    private final Set<Integer> pendingMetadataUpdateReaders = new HashSet<>();

    ReaderRecoveryGate(boolean restoredFromCheckpoint) {
        this.initialReaderRegistrationPending = restoredFromCheckpoint;
    }

    /** Records the current report during initial recovery, including an empty replacement. */
    void recordReportedSplits(int subtaskId, List<DynamicKafkaSourceSplit> reportedSplits) {
        if (initialReaderRegistrationPending) {
            pendingReportedSplitsByReader.put(subtaskId, new ArrayList<>(reportedSplits));
        }
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
