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

/**
 * Tracks globally known dynamic split ids and computes reader ownership using round-robin order.
 */
final class GlobalSplitOwnerAssigner {

    private final Set<String> knownActiveSplitIds = new HashSet<>();
    private final Map<String, Integer> preferredOwnerBySplitId = new HashMap<>();

    void onMetadataRefresh(Set<String> activeSplitIds) {
        knownActiveSplitIds.clear();
        knownActiveSplitIds.addAll(activeSplitIds);
        preferredOwnerBySplitId.keySet().retainAll(activeSplitIds);
    }

    void onSplitsBack(List<DynamicKafkaSourceSplit> splits, int subtaskId) {
        for (DynamicKafkaSourceSplit split : splits) {
            knownActiveSplitIds.add(split.splitId());
            preferredOwnerBySplitId.put(split.splitId(), subtaskId);
        }
    }

    int assignSplitOwner(String splitId, int numReaders) {
        Preconditions.checkArgument(numReaders > 0, "numReaders must be > 0");

        Integer preferredOwner = preferredOwnerBySplitId.remove(splitId);
        if (preferredOwner != null && preferredOwner >= 0 && preferredOwner < numReaders) {
            return preferredOwner;
        }

        int targetReader = Math.floorMod(knownActiveSplitIds.size(), numReaders);
        knownActiveSplitIds.add(splitId);
        return targetReader;
    }
}
