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
 * <p>When the enumerator is restored from checkpointed state, split assignment and metadata update
 * events must be deferred until the first metadata discovery has completed and every reader has
 * (re-)registered, so that restored reader splits can be redistributed consistently. This class
 * owns that gating state; the enumerator remains responsible for acting on it.
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

    /** Records splits a reader reported on registration; an empty report is ignored. */
    void recordReportedSplits(int subtaskId, List<DynamicKafkaSourceSplit> reportedSplits) {
        if (!reportedSplits.isEmpty()) {
            pendingReportedSplitsByReader.put(subtaskId, new ArrayList<>(reportedSplits));
        }
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

    /** Returns the reported splits ordered by reader id and clears the pending state. */
    NavigableMap<Integer, List<DynamicKafkaSourceSplit>> drainReportedSplits() {
        NavigableMap<Integer, List<DynamicKafkaSourceSplit>> reportedSplitsByReader =
                new TreeMap<>(pendingReportedSplitsByReader);
        pendingReportedSplitsByReader.clear();
        return reportedSplitsByReader;
    }
}
