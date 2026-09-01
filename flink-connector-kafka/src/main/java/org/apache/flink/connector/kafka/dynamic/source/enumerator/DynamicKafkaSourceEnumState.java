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
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The enumerator state keeps track of the state of the sub enumerators assigned splits and
 * metadata.
 */
@Internal
public class DynamicKafkaSourceEnumState {
    private final Set<KafkaStream> kafkaStreams;
    private final Map<String, KafkaSourceEnumState> clusterEnumeratorStates;
    private final Map<String, RetainedClusterState> retainedClusterEnumeratorStates;
    private final Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader;

    public DynamicKafkaSourceEnumState() {
        this(new HashSet<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    public DynamicKafkaSourceEnumState(
            Set<KafkaStream> kafkaStreams,
            Map<String, KafkaSourceEnumState> clusterEnumeratorStates) {
        this(kafkaStreams, clusterEnumeratorStates, new HashMap<>(), new HashMap<>());
    }

    public DynamicKafkaSourceEnumState(
            Set<KafkaStream> kafkaStreams,
            Map<String, KafkaSourceEnumState> clusterEnumeratorStates,
            Map<String, RetainedClusterState> retainedClusterEnumeratorStates,
            Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader) {
        this.kafkaStreams = kafkaStreams;
        this.clusterEnumeratorStates = clusterEnumeratorStates;
        this.retainedClusterEnumeratorStates = retainedClusterEnumeratorStates;
        this.pendingReportedSplitsByReader = pendingReportedSplitsByReader;
    }

    public Set<KafkaStream> getKafkaStreams() {
        return kafkaStreams;
    }

    public Map<String, KafkaSourceEnumState> getClusterEnumeratorStates() {
        return clusterEnumeratorStates;
    }

    public Map<String, RetainedClusterState> getRetainedClusterEnumeratorStates() {
        return retainedClusterEnumeratorStates;
    }

    /**
     * Splits reported by readers on registration that were not yet reassigned when the checkpoint
     * was taken. Readers hold no splits at that point, and the sub enumerator states mark the
     * partitions as assigned without tracking the reader-reported offsets. Until reassignment this
     * map is the only record of those offsets.
     */
    public Map<Integer, List<DynamicKafkaSourceSplit>> getPendingReportedSplitsByReader() {
        return pendingReportedSplitsByReader;
    }

    /** Kafka enumerator state that stays checkpointed after its cluster becomes inactive. */
    public static class RetainedClusterState {
        private final KafkaSourceEnumState kafkaSourceEnumState;
        private final long retainedUntilMs;
        // Derived once per retained inventory; the serializer writes only the original state.
        private final Set<TopicPartition> partitions;

        public RetainedClusterState(
                KafkaSourceEnumState kafkaSourceEnumState, long retainedUntilMs) {
            this.kafkaSourceEnumState = kafkaSourceEnumState;
            this.retainedUntilMs = retainedUntilMs;
            this.partitions =
                    kafkaSourceEnumState.splits().stream()
                            .map(status -> status.split().getTopicPartition())
                            .collect(Collectors.toSet());
        }

        public KafkaSourceEnumState getKafkaSourceEnumState() {
            return kafkaSourceEnumState;
        }

        public long getRetainedUntilMs() {
            return retainedUntilMs;
        }

        boolean containsPartition(TopicPartition partition) {
            return partitions.contains(partition);
        }
    }
}
