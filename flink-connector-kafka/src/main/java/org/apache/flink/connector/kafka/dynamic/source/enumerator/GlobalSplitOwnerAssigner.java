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
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tracks globally known dynamic split ids and computes reader ownership using round-robin order.
 */
final class GlobalSplitOwnerAssigner {

    private final Set<String> knownActiveSplitIds = new HashSet<>();
    private final Map<String, Integer> recoveryOwnerBySplitId = new HashMap<>();

    void onMetadataRefresh(Set<String> activeSplitIds) {
        knownActiveSplitIds.clear();
        knownActiveSplitIds.addAll(activeSplitIds);
        recoveryOwnerBySplitId.clear();
    }

    void onRecoveredSplits(List<DynamicKafkaSourceSplit> splits, int numReaders) {
        Preconditions.checkArgument(numReaders > 0, "numReaders must be > 0");

        recoveryOwnerBySplitId.clear();
        for (DynamicKafkaSourceSplit split : splits) {
            knownActiveSplitIds.remove(split.splitId());
        }

        for (DynamicKafkaSourceSplit split : splits) {
            int targetReader = Math.floorMod(knownActiveSplitIds.size(), numReaders);
            knownActiveSplitIds.add(split.splitId());
            recoveryOwnerBySplitId.put(split.splitId(), targetReader);
        }
    }

    /** Places a returning cohort without moving any currently active split. */
    void onRetainedSplitsReadded(
            List<DynamicKafkaSourceSplit> returningSplits,
            Map<String, Integer> activeOwners,
            int numReaders) {
        Preconditions.checkArgument(numReaders > 0, "numReaders must be > 0");
        Set<String> returningIds = new TreeSet<>();
        for (DynamicKafkaSourceSplit split : returningSplits) {
            Preconditions.checkArgument(
                    !activeOwners.containsKey(split.splitId()),
                    "Returning split %s already has an active owner",
                    split.splitId());
            returningIds.add(split.splitId());
        }

        int[] loads = new int[numReaders];
        for (int owner : activeOwners.values()) {
            Preconditions.checkArgument(
                    owner >= 0 && owner < numReaders, "Invalid active reader %s", owner);
            loads[owner]++;
        }
        for (String splitId : returningIds) {
            recoveryOwnerBySplitId.remove(splitId);
        }
        // Another returning cluster may have reserved owners before its asynchronous assignment
        // runs. Count those reservations once, alongside already assigned active splits.
        recoveryOwnerBySplitId.forEach(
                (splitId, owner) -> {
                    if (!activeOwners.containsKey(splitId)) {
                        Preconditions.checkArgument(
                                owner >= 0 && owner < numReaders,
                                "Invalid reserved reader %s",
                                owner);
                        loads[owner]++;
                    }
                });
        knownActiveSplitIds.clear();
        knownActiveSplitIds.addAll(activeOwners.keySet());
        knownActiveSplitIds.addAll(recoveryOwnerBySplitId.keySet());
        for (String splitId : returningIds) {
            int owner = 0;
            for (int reader = 1; reader < numReaders; reader++) {
                if (loads[reader] < loads[owner]) {
                    owner = reader;
                }
            }
            loads[owner]++;
            recoveryOwnerBySplitId.put(splitId, owner);
            knownActiveSplitIds.add(splitId);
        }
    }

    int assignSplitOwner(String splitId, int numReaders) {
        Preconditions.checkArgument(numReaders > 0, "numReaders must be > 0");

        Integer recoveryOwner = recoveryOwnerBySplitId.remove(splitId);
        if (recoveryOwner != null) {
            return recoveryOwner;
        }

        int targetReader = Math.floorMod(knownActiveSplitIds.size(), numReaders);
        knownActiveSplitIds.add(splitId);
        return targetReader;
    }
}
