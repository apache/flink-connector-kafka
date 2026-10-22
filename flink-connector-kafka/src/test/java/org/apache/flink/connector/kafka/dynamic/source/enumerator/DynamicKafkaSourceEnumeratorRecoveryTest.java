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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsSplitReassignmentOnRecovery;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.mock.Whitebox;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Recovery tests for {@link DynamicKafkaSourceEnumerator}. */
public class DynamicKafkaSourceEnumeratorRecoveryTest {

    @Test
    public void testReassignsReportedActiveSplitsAfterMetadataShrink() throws Throwable {
        int parallelism = 4;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String activeTopic = "active-topic";
        String removedTopic = "removed-topic";

        List<DynamicKafkaSourceSplit> activeSplits = createSplits(clusterId, activeTopic, 10);
        List<DynamicKafkaSourceSplit> reportedSplits = new ArrayList<>(activeSplits);
        reportedSplits.addAll(createSplits(clusterId, removedTopic, 2));

        KafkaStream restoredKafkaStream =
                createKafkaStream(streamId, clusterId, Set.of(activeTopic, removedTopic));
        KafkaStream currentKafkaStream = createKafkaStream(streamId, clusterId, activeTopic);
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(restoredKafkaStream, clusterId, reportedSplits);
        Properties properties = createGlobalModeProperties();

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(
                                        Collections.singleton(currentKafkaStream)),
                                context,
                                properties,
                                restoredState)) {
            enumerator.start();
            for (int reader = 0; reader < parallelism; reader++) {
                List<DynamicKafkaSourceSplit> readerReportedSplits =
                        reader == 0 ? reportedSplits : Collections.emptyList();
                context.registerReader(
                        ReaderInfo.createReaderInfo(
                                reader, "location-" + reader, readerReportedSplits));
                enumerator.addReader(reader);
            }

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            context.runNextOneTimeCallable();

            Map<Integer, Integer> assignmentCounts = new HashMap<>();
            Set<String> assignedSplitIds = new HashSet<>();
            int totalAssignments = 0;
            for (int reader = 0; reader < parallelism; reader++) {
                assignmentCounts.put(reader, 0);
            }
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                for (Map.Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assignmentCounts.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
                    totalAssignments += entry.getValue().size();
                    for (DynamicKafkaSourceSplit split : entry.getValue()) {
                        assertThat(split.getKafkaPartitionSplit().getTopic())
                                .isEqualTo(activeTopic);
                        assignedSplitIds.add(split.splitId());
                    }
                }
            }

            assertThat(totalAssignments).isEqualTo(activeSplits.size());
            assertThat(assignedSplitIds).hasSize(activeSplits.size());
            assertThat(assignmentCounts.values()).containsExactlyInAnyOrder(3, 3, 2, 2);
        }
    }

    @Test
    public void testReturnsRetainedSplitsBeforeSendingDeferredMetadata() throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String activeClusterId = "active-cluster";
        String activeTopic = "active-topic";
        DynamicKafkaSourceSplit activeSplit = createSplits(activeClusterId, activeTopic, 1).get(0);
        DynamicKafkaSourceSplit removedSplit =
                createSplits("removed-cluster", "removed-topic", 1).get(0);

        KafkaStream kafkaStream = createKafkaStream(streamId, activeClusterId, activeTopic);
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(
                        kafkaStream, activeClusterId, Collections.singletonList(activeSplit));
        Properties properties = createGlobalModeProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                restoredState)) {
            enumerator.start();
            context.registerReader(
                    ReaderInfo.createReaderInfo(
                            0, "location-0", List.of(activeSplit, removedSplit)));
            enumerator.addReader(0);
            enumerator.handleSourceEvent(0, new GetMetadataUpdateEvent());

            assertThat(context.getSentSourceEvent().getOrDefault(0, Collections.emptyList()))
                    .isEmpty();

            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            context.runNextOneTimeCallable();

            List<DynamicKafkaSourceSplit> assignedSplits =
                    context.getSplitsAssignmentSequence().stream()
                            .flatMap(
                                    assignment ->
                                            assignment.assignment().values().stream()
                                                    .flatMap(List::stream))
                            .collect(java.util.stream.Collectors.toList());
            DynamicKafkaSourceSplit retainedSplit =
                    assignedSplits.stream()
                            .filter(split -> split.splitId().equals(removedSplit.splitId()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertThat(retainedSplit.isRetained()).isTrue();
            assertThat(context.getSentSourceEvent().get(0))
                    .hasSize(1)
                    .allMatch(MetadataUpdateEvent.class::isInstance);
        }
    }

    @Test
    public void testReassignsReportedSplitsWithPerClusterOwnerSelection() throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> splits = createSplits(clusterId, topic, 4);
        KafkaStream kafkaStream = createKafkaStream(streamId, clusterId, topic);

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                createBaseProperties(),
                                createRestoredState(kafkaStream, clusterId, splits))) {
            enumerator.start();
            context.registerReader(ReaderInfo.createReaderInfo(0, "location-0", splits));
            enumerator.addReader(0);
            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);
            context.runNextOneTimeCallable();

            Map<Integer, Integer> assignmentCounts = new HashMap<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                for (Map.Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assignmentCounts.merge(entry.getKey(), entry.getValue().size(), Integer::sum);
                }
            }
            assertThat(assignmentCounts).containsEntry(0, 2).containsEntry(1, 2);
        }
    }

    @Test
    public void testSourceOptsIntoSplitReassignmentOnRecovery() {
        assertThat(
                        SupportsSplitReassignmentOnRecovery.class.isAssignableFrom(
                                DynamicKafkaSource.class))
                .isTrue();
    }

    @Test
    public void testCheckpointDuringPendingRecoveryPreservesSplitsAndReportedOffsets()
            throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> enumeratorStateSplits = createSplits(clusterId, topic, 2);
        List<DynamicKafkaSourceSplit> reportedSplits = new ArrayList<>();
        // use distinct offsets to verify that recovery preserves the reader-reported positions
        // instead of the stale EARLIEST offsets
        for (int partition = 0; partition < 2; partition++) {
            reportedSplits.add(
                    new DynamicKafkaSourceSplit(
                            clusterId,
                            new KafkaPartitionSplit(
                                    new TopicPartition(topic, partition), 42 + partition)));
        }
        KafkaStream kafkaStream = createKafkaStream(streamId, clusterId, topic);
        Properties properties = createGlobalModeProperties();

        DynamicKafkaSourceEnumState inWindowCheckpoint;
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                createRestoredState(
                                        kafkaStream, clusterId, enumeratorStateSplits))) {
            enumerator.start();
            context.registerReader(ReaderInfo.createReaderInfo(0, "location-0", reportedSplits));
            enumerator.addReader(0);
            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            inWindowCheckpoint = enumerator.snapshotState(1L);
        }

        DynamicKafkaSourceEnumState restoredCheckpoint = serdeRoundTrip(inWindowCheckpoint);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                restoredCheckpoint)) {
            enumerator.start();
            registerReadersWithoutReportedSplits(context, enumerator, parallelism);
            context.runNextOneTimeCallable();

            assertAssignedSplitIdsExactly(context, reportedSplits);
            Map<Integer, Long> startingOffsetByPartition = new HashMap<>();
            for (DynamicKafkaSourceSplit split : collectAssignedSplits(context)) {
                startingOffsetByPartition.put(
                        split.getKafkaPartitionSplit().getPartition(),
                        split.getKafkaPartitionSplit().getStartingOffset());
            }
            assertThat(startingOffsetByPartition)
                    .as("assigned splits must carry the reader-reported offsets")
                    .containsEntry(0, 42L)
                    .containsEntry(1, 43L);
        }
    }

    @Test
    public void testCheckpointDuringPendingRecoveryKeepsRetainedSplit() throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String activeClusterId = "active-cluster";
        String activeTopic = "active-topic";
        DynamicKafkaSourceSplit activeSplit = createSplits(activeClusterId, activeTopic, 1).get(0);
        DynamicKafkaSourceSplit removedSplit =
                createSplits("removed-cluster", "removed-topic", 1).get(0);
        KafkaStream kafkaStream = createKafkaStream(streamId, activeClusterId, activeTopic);
        Properties properties = createGlobalModeProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");

        DynamicKafkaSourceEnumState inWindowCheckpoint;
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                createRestoredState(
                                        kafkaStream,
                                        activeClusterId,
                                        Collections.singletonList(activeSplit)))) {
            enumerator.start();
            context.registerReader(
                    ReaderInfo.createReaderInfo(
                            0, "location-0", List.of(activeSplit, removedSplit)));
            enumerator.addReader(0);
            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            inWindowCheckpoint = enumerator.snapshotState(1L);
        }

        DynamicKafkaSourceEnumState restoredCheckpoint = serdeRoundTrip(inWindowCheckpoint);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                restoredCheckpoint)) {
            enumerator.start();
            registerReadersWithoutReportedSplits(context, enumerator, parallelism);
            context.runNextOneTimeCallable();

            assertAssignedSplitIdsExactly(context, List.of(activeSplit, removedSplit));
            DynamicKafkaSourceSplit retainedSplit =
                    collectAssignedSplits(context).stream()
                            .filter(split -> split.splitId().equals(removedSplit.splitId()))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertThat(retainedSplit.isRetained()).isTrue();
        }
    }

    @Test
    public void testRestoredPendingReportedSplitsMergeWhenDownscaleCollapsesReaderIds()
            throws Throwable {
        int parallelism = 3;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> activeSplits = createSplits(clusterId, topic, 4);
        DynamicKafkaSourceSplit removedClusterSplit =
                createSplits("removed-cluster", "removed-topic", 1).get(0);
        List<DynamicKafkaSourceSplit> reportedByReader4 =
                new ArrayList<>(activeSplits.subList(2, 4));
        reportedByReader4.add(removedClusterSplit);
        List<DynamicKafkaSourceSplit> allReportedSplits = new ArrayList<>(activeSplits);
        allReportedSplits.add(removedClusterSplit);
        KafkaStream kafkaStream = createKafkaStream(streamId, clusterId, topic);
        Properties properties = createGlobalModeProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");
        // readers 1 and 4 from a parallelism-5 checkpoint both map to reader floorMod(4, 3) = 1
        Map<Integer, List<DynamicKafkaSourceSplit>> pendingSplitsByReader = new HashMap<>();
        pendingSplitsByReader.put(1, activeSplits.subList(0, 2));
        pendingSplitsByReader.put(4, reportedByReader4);
        DynamicKafkaSourceEnumState inWindowCheckpoint =
                new DynamicKafkaSourceEnumState(
                        Collections.singleton(kafkaStream),
                        Collections.singletonMap(
                                clusterId,
                                new KafkaSourceEnumState(
                                        unwrapSplits(activeSplits), Collections.emptyList(), true)),
                        Collections.emptyMap(),
                        pendingSplitsByReader);

        DynamicKafkaSourceEnumState restoredCheckpoint = serdeRoundTrip(inWindowCheckpoint);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                properties,
                                restoredCheckpoint)) {
            enumerator.start();
            registerReadersWithoutReportedSplits(context, enumerator, parallelism);
            context.runNextOneTimeCallable();

            Integer retainedSplitReader = null;
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                for (Map.Entry<Integer, List<DynamicKafkaSourceSplit>> entry :
                        assignment.assignment().entrySet()) {
                    assertThat(entry.getKey()).isBetween(0, parallelism - 1);
                    for (DynamicKafkaSourceSplit split : entry.getValue()) {
                        if (split.splitId().equals(removedClusterSplit.splitId())) {
                            assertThat(split.isRetained()).isTrue();
                            retainedSplitReader = entry.getKey();
                        }
                    }
                }
            }
            assertAssignedSplitIdsExactly(context, allReportedSplits);
            // the retained split returns to its reporting reader, remapped as floorMod(4, 3)
            assertThat(retainedSplitReader).isEqualTo(1);
        }
    }

    @Test
    public void testRestoredPendingSplitsMergeWithFreshReport() throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        // partition 0 is reported both as restored pending (offset 10) and fresh (offset 42),
        // partition 1 only as restored pending, partition 2 only as fresh
        DynamicKafkaSourceSplit restoredOverlappingSplit =
                new DynamicKafkaSourceSplit(
                        clusterId, new KafkaPartitionSplit(new TopicPartition(topic, 0), 10));
        DynamicKafkaSourceSplit restoredOnlySplit =
                new DynamicKafkaSourceSplit(
                        clusterId, new KafkaPartitionSplit(new TopicPartition(topic, 1), 7));
        DynamicKafkaSourceSplit freshOverlappingSplit =
                new DynamicKafkaSourceSplit(
                        clusterId, new KafkaPartitionSplit(new TopicPartition(topic, 0), 42));
        DynamicKafkaSourceSplit freshOnlySplit =
                new DynamicKafkaSourceSplit(
                        clusterId, new KafkaPartitionSplit(new TopicPartition(topic, 2), 5));
        KafkaStream kafkaStream = createKafkaStream(streamId, clusterId, topic);
        DynamicKafkaSourceEnumState restoredCheckpoint =
                serdeRoundTrip(
                        new DynamicKafkaSourceEnumState(
                                Collections.singleton(kafkaStream),
                                Collections.singletonMap(
                                        clusterId,
                                        new KafkaSourceEnumState(
                                                unwrapSplits(
                                                        List.of(
                                                                restoredOverlappingSplit,
                                                                restoredOnlySplit,
                                                                freshOnlySplit)),
                                                Collections.emptyList(),
                                                true)),
                                Collections.emptyMap(),
                                Collections.singletonMap(
                                        1, List.of(restoredOverlappingSplit, restoredOnlySplit))));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                createGlobalModeProperties(),
                                restoredCheckpoint)) {
            enumerator.start();
            context.registerReader(
                    ReaderInfo.createReaderInfo(0, "location-0", Collections.emptyList()));
            enumerator.addReader(0);
            context.registerReader(
                    ReaderInfo.createReaderInfo(
                            1, "location-1", List.of(freshOverlappingSplit, freshOnlySplit)));
            enumerator.addReader(1);
            context.runNextOneTimeCallable();

            assertAssignedSplitIdsExactly(
                    context, List.of(freshOverlappingSplit, restoredOnlySplit, freshOnlySplit));
            Map<Integer, Long> startingOffsetByPartition = new HashMap<>();
            for (DynamicKafkaSourceSplit split : collectAssignedSplits(context)) {
                startingOffsetByPartition.put(
                        split.getKafkaPartitionSplit().getPartition(),
                        split.getKafkaPartitionSplit().getStartingOffset());
            }
            assertThat(startingOffsetByPartition)
                    .as("fresh report wins per split id, restored-only splits keep their offsets")
                    .containsEntry(0, 42L)
                    .containsEntry(1, 7L)
                    .containsEntry(2, 5L);
        }
    }

    @Test
    public void testChangedMetadataAfterInWindowCheckpointDropsRemovedTopicSplits()
            throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String keptTopic = "kept-topic";
        String removedTopic = "removed-topic";
        List<DynamicKafkaSourceSplit> keptSplits = createSplits(clusterId, keptTopic, 2);
        List<DynamicKafkaSourceSplit> reportedSplits = new ArrayList<>(keptSplits);
        reportedSplits.addAll(createSplits(clusterId, removedTopic, 1));
        KafkaStream restoredKafkaStream =
                createKafkaStream(streamId, clusterId, Set.of(keptTopic, removedTopic));
        KafkaStream currentKafkaStream = createKafkaStream(streamId, clusterId, keptTopic);
        Properties properties = createGlobalModeProperties();
        // retention enabled: the split is dropped because its topic was removed while the
        // cluster stayed active, not because retention is off
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");

        DynamicKafkaSourceEnumState inWindowCheckpoint;
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(
                                        Collections.singleton(restoredKafkaStream)),
                                context,
                                properties,
                                createRestoredState(
                                        restoredKafkaStream, clusterId, reportedSplits))) {
            enumerator.start();
            context.registerReader(ReaderInfo.createReaderInfo(0, "location-0", reportedSplits));
            enumerator.addReader(0);
            context.registerReader(
                    ReaderInfo.createReaderInfo(1, "location-1", Collections.emptyList()));
            enumerator.addReader(1);

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            inWindowCheckpoint = enumerator.snapshotState(1L);
        }

        DynamicKafkaSourceEnumState restoredCheckpoint = serdeRoundTrip(inWindowCheckpoint);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(
                                        Collections.singleton(currentKafkaStream)),
                                context,
                                properties,
                                restoredCheckpoint)) {
            enumerator.start();
            registerReadersWithoutReportedSplits(context, enumerator, parallelism);
            context.runNextOneTimeCallable();

            assertAssignedSplitIdsExactly(context, keptSplits);
        }
    }

    @Test
    public void testPendingOnlyRestoredStateDefersMetadataUpdatesUntilReassignment()
            throws Throwable {
        int parallelism = 2;
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> splits = createSplits(clusterId, topic, 2);
        KafkaStream kafkaStream = createKafkaStream(streamId, clusterId, topic);
        // the pending splits' cluster appears in neither the cluster nor the retained states
        DynamicKafkaSourceEnumState restoredCheckpoint =
                serdeRoundTrip(
                        new DynamicKafkaSourceEnumState(
                                Collections.singleton(kafkaStream),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                Collections.singletonMap(0, splits)));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(kafkaStream)),
                                context,
                                createGlobalModeProperties(),
                                restoredCheckpoint)) {
            enumerator.start();
            registerReadersWithoutReportedSplits(context, enumerator, parallelism);
            enumerator.handleSourceEvent(0, new GetMetadataUpdateEvent());

            // all readers are registered, but the restored pending splits are not reassigned
            // yet: the metadata update must stay deferred
            assertThat(context.getSentSourceEvent().getOrDefault(0, Collections.emptyList()))
                    .isEmpty();

            context.runNextOneTimeCallable();

            assertAssignedSplitIdsExactly(context, splits);
            assertThat(context.getSentSourceEvent().get(0))
                    .hasSize(1)
                    .allMatch(MetadataUpdateEvent.class::isInstance);
        }
    }

    @Test
    public void testLocalFailureBeforeInitialDiscoveryPreservesReportedSplits() throws Throwable {
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> checkpointSplits =
                List.of(
                        new DynamicKafkaSourceSplit(
                                clusterId,
                                new KafkaPartitionSplit(new TopicPartition(topic, 0), 42L)),
                        new DynamicKafkaSourceSplit(
                                clusterId,
                                new KafkaPartitionSplit(new TopicPartition(topic, 1), 43L)));
        KafkaStream stream = createKafkaStream(streamId, clusterId, topic);

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createGlobalModeProperties(),
                                createRestoredState(stream, clusterId, checkpointSplits))) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, checkpointSplits);
            registerReaderAndReportRestoredSplits(context, enumerator, 1, Collections.emptyList());
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // No post-checkpoint assignment exists to return before metadata discovery completes.
            context.unregisterReader(0);
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, Collections.emptyList());
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            context.runNextOneTimeCallable();

            assertExactCheckpointOffsets(context, 0, checkpointSplits);
            assertSinglePhysicalOwners(context, 0, checkpointSplits.size());
        }
    }

    @Test
    public void testInitialRestoreKeepsOneOwnerBeforeAnyLocalFailure() throws Throwable {
        verifyNumericCheckpointRecovery(false, false);
    }

    @Test
    public void testInitialRestoreDiscoveryBeforeRegistrationKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(true, false);
    }

    @Test
    public void testLocalFailureBeforeNewCheckpointKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(false, true);
    }

    @Test
    public void testLocalFailureWithReverseRegistrationKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 3, true, false, false, false);
    }

    @Test
    public void testLocalFailureWithInterleavedReturnsKeepsOneOwner() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 3, true, true, false, false);
    }

    @Test
    public void testOnlyReaderZeroFailsWithoutMovingSurvivingSplits() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 1, false, false, false, false);
    }

    @Test
    public void testOnlyReaderOneFailsWithoutMovingSurvivingSplits() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 2, false, false, false, false);
    }

    @Test
    public void testPerClusterLocalFailureDoesNotReclaimSurvivingReaderSplits() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 1, false, false, false, false, "per_cluster");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testLocalFailureOfReactivatedRetainedSplitsKeepsCurrentOwners(int failedReaderMask)
            throws Throwable {
        verifyNumericCheckpointRecovery(
                false, true, failedReaderMask, false, false, false, false, "global", true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPerClusterLocalFailureAfterRetainedClusterReactivationKeepsRestoredOwner(
            boolean bounded) throws Throwable {
        String clusterId = "cluster-0";
        KafkaStream inactiveStream = new KafkaStream("stream", Collections.emptyMap());
        KafkaStream activeStream = createKafkaStream("stream", clusterId, "topic");
        // After rescaling, the retained reader can differ from the per-cluster hash owner.
        DynamicKafkaSourceSplit checkpointSplit =
                new DynamicKafkaSourceSplit(
                        clusterId,
                        new KafkaPartitionSplit(new TopicPartition("topic", 0), 123L, 1000L));
        DynamicKafkaSourceSplit retainedSplit = checkpointSplit.retainUntil(Long.MAX_VALUE);
        assertThat(
                        KafkaSourceEnumerator.getSplitOwner(
                                checkpointSplit.getKafkaPartitionSplit().getTopicPartition(), 2))
                .isEqualTo(1);

        Properties properties = createBaseProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), "per_cluster");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");
        DynamicKafkaSourceEnumState restoredState =
                new DynamicKafkaSourceEnumState(
                        Collections.singleton(inactiveStream),
                        Collections.emptyMap(),
                        Collections.singletonMap(
                                clusterId,
                                new DynamicKafkaSourceEnumState.RetainedClusterState(
                                        new KafkaSourceEnumState(
                                                List.of(checkpointSplit.getKafkaPartitionSplit()),
                                                Collections.emptyList(),
                                                true),
                                        Long.MAX_VALUE)),
                        Collections.emptyMap());
        List<String> events = new ArrayList<>();
        try (MockKafkaMetadataService metadataService =
                        new MockKafkaMetadataService(Collections.singleton(inactiveStream));
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<DynamicKafkaSourceSplit>(2) {
                            @Override
                            public void assignSplits(
                                    SplitsAssignment<DynamicKafkaSourceSplit> assignment) {
                                assignment
                                        .assignment()
                                        .forEach(
                                                (reader, splits) ->
                                                        splits.forEach(
                                                                split ->
                                                                        events.add(
                                                                                "assign:"
                                                                                        + reader
                                                                                        + ":"
                                                                                        + split
                                                                                                .splitId())));
                                super.assignSplits(assignment);
                            }

                            @Override
                            public void signalNoMoreSplits(int reader) {
                                events.add("done:" + reader);
                                super.signalNoMoreSplits(reader);
                            }
                        };
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton("stream")),
                                metadataService,
                                context,
                                OffsetsInitializer.earliest(),
                                bounded
                                        ? OffsetsInitializer.latest()
                                        : new NoStoppingOffsetsInitializer(),
                                properties,
                                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED,
                                restoredState,
                                new NoOpKafkaEnumContextProxyFactory())) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(retainedSplit));
            registerReaderAndReportRestoredSplits(context, enumerator, 1, Collections.emptyList());
            context.runPeriodicCallable(0);

            assertExactCheckpointOffsets(context, 0, List.of(retainedSplit));
            context.getSplitsAssignmentSequence()
                    .forEach(assignment -> assertThat(assignment.assignment()).containsOnlyKeys(0));
            if (bounded) {
                assertBoundedAssignmentsBeforeCompletion(events, 0, 1);
                assertThat(events.stream().filter(event -> event.equals("done:0"))).hasSize(1);
                assertThat(events.stream().filter(event -> event.equals("done:1"))).hasSize(1);
            }

            int assignmentsBeforeReactivation = context.getSplitsAssignmentSequence().size();
            metadataService.setKafkaStreams(Collections.singleton(activeStream));
            context.runPeriodicCallable(0);
            KafkaSourceEnumerator subEnumerator =
                    (KafkaSourceEnumerator)
                            ((Map<?, ?>)
                                            Whitebox.getInternalState(
                                                    enumerator, "clusterEnumeratorMap"))
                                    .get(clusterId);
            completePartitionDiscovery(subEnumerator);
            assertThat(context.getSplitsAssignmentSequence())
                    .hasSize(assignmentsBeforeReactivation);

            for (int attempt = 0; attempt < 2; attempt++) {
                events.clear();
                int priorAssignments = context.getSplitsAssignmentSequence().size();
                context.unregisterReader(0);
                enumerator.addSplitsBack(List.of(checkpointSplit), 0);
                registerReaderAndReportRestoredSplits(
                        context, enumerator, 0, List.of(retainedSplit));

                assertExactCheckpointOffsets(context, priorAssignments, List.of(checkpointSplit));
                context.getSplitsAssignmentSequence().stream()
                        .skip(priorAssignments)
                        .forEach(
                                assignment ->
                                        assertThat(assignment.assignment()).containsOnlyKeys(0));
                if (bounded) {
                    assertBoundedAssignmentsBeforeCompletion(events, 0, 1);
                    assertThat(events.stream().filter(event -> event.equals("done:0"))).hasSize(1);
                    assertThat(events).doesNotContain("done:1");
                } else {
                    assertThat(events).noneMatch(event -> event.startsWith("done:"));
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("collidingSplitOwners")
    public void testLocalFailureKeepsDistinctOwnersWhenActiveAndRetainedSplitIdsCollide(
            boolean bounded, int retainedReader) throws Throwable {
        DynamicKafkaSourceSplit activeSplit =
                new DynamicKafkaSourceSplit(
                        "a", new KafkaPartitionSplit(new TopicPartition("b-c", 0), 123L, 1000L));
        DynamicKafkaSourceSplit retainedSplit =
                new DynamicKafkaSourceSplit(
                                "a-b",
                                new KafkaPartitionSplit(new TopicPartition("c", 0), 456L, 2000L))
                        .retainUntil(Long.MAX_VALUE);
        assertThat(activeSplit.splitId()).isEqualTo(retainedSplit.splitId());
        assertThat(
                        KafkaSourceEnumerator.getSplitOwner(
                                activeSplit.getKafkaPartitionSplit().getTopicPartition(), 2))
                .isZero();
        KafkaStream activeStream = createKafkaStream("stream", "a", "b-c");
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(activeStream, "a", List.of(activeSplit));
        Properties properties = createBaseProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), "per_cluster");
        List<String> events = new ArrayList<>();
        Map<Integer, List<DynamicKafkaSourceSplit>> reports =
                Map.of(
                        0,
                        retainedReader == 0
                                ? List.of(activeSplit, retainedSplit)
                                : List.of(activeSplit),
                        1,
                        retainedReader == 1 ? List.of(retainedSplit) : Collections.emptyList());

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<DynamicKafkaSourceSplit>(2) {
                            @Override
                            public void assignSplits(
                                    SplitsAssignment<DynamicKafkaSourceSplit> assignment) {
                                assignment
                                        .assignment()
                                        .forEach(
                                                (reader, splits) ->
                                                        splits.forEach(
                                                                split ->
                                                                        events.add(
                                                                                "assign:"
                                                                                        + reader
                                                                                        + ":"
                                                                                        + split
                                                                                                .splitId())));
                                super.assignSplits(assignment);
                            }

                            @Override
                            public void signalNoMoreSplits(int reader) {
                                events.add("done:" + reader);
                                super.signalNoMoreSplits(reader);
                            }
                        };
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton("stream")),
                                new MockKafkaMetadataService(Collections.singleton(activeStream)),
                                context,
                                OffsetsInitializer.earliest(),
                                bounded
                                        ? OffsetsInitializer.latest()
                                        : new NoStoppingOffsetsInitializer(),
                                properties,
                                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED,
                                restoredState,
                                new NoOpKafkaEnumContextProxyFactory())) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, reports.get(0));
            registerReaderAndReportRestoredSplits(context, enumerator, 1, reports.get(1));
            context.runNextOneTimeCallable();
            completePartitionDiscovery(
                    (KafkaSourceEnumerator)
                            ((Map<?, ?>)
                                            Whitebox.getInternalState(
                                                    enumerator, "clusterEnumeratorMap"))
                                    .get("a"));

            assertExactCheckpointOffsets(context, 0, List.of(activeSplit, retainedSplit));
            reports.forEach(
                    (reader, expected) ->
                            assertThat(
                                            context.getSplitsAssignmentSequence().stream()
                                                    .flatMap(
                                                            assignment ->
                                                                    assignment
                                                                            .assignment()
                                                                            .getOrDefault(
                                                                                    reader,
                                                                                    Collections
                                                                                            .emptyList())
                                                                            .stream()))
                                    .containsExactlyInAnyOrderElementsOf(expected));

            int priorAssignments = context.getSplitsAssignmentSequence().size();
            events.clear();
            context.unregisterReader(retainedReader);
            enumerator.addSplitsBack(Collections.emptyList(), retainedReader);
            registerReaderAndReportRestoredSplits(
                    context, enumerator, retainedReader, reports.get(retainedReader));

            assertExactCheckpointOffsets(context, priorAssignments, reports.get(retainedReader));
            context.getSplitsAssignmentSequence().stream()
                    .skip(priorAssignments)
                    .forEach(
                            assignment ->
                                    assertThat(assignment.assignment())
                                            .containsOnlyKeys(retainedReader));
            if (bounded) {
                assertBoundedAssignmentsBeforeCompletion(
                        events, retainedReader, reports.get(retainedReader).size());
                assertThat(events.stream().filter(event -> event.equals("done:" + retainedReader)))
                        .hasSize(1);
                assertThat(events).doesNotContain("done:" + (1 - retainedReader));
            } else {
                assertThat(events).noneMatch(event -> event.startsWith("done:"));
            }
        }
    }

    private static Stream<Arguments> collidingSplitOwners() {
        return Stream.of(
                Arguments.of(true, 0),
                Arguments.of(true, 1),
                Arguments.of(false, 0),
                Arguments.of(false, 1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInitialRecoveryReRegistrationKeepsPartitionsWithCollidingSplitIds(
            boolean bounded) throws Throwable {
        DynamicKafkaSourceSplit activeSplit = createSplit("a", "b-c", 123L, 1000L);
        DynamicKafkaSourceSplit retainedSplit =
                createSplit("a-b", "c", 456L, 2000L).retainUntil(Long.MAX_VALUE);
        assertThat(activeSplit.splitId()).isEqualTo(retainedSplit.splitId());
        KafkaStream stream = createKafkaStream("stream", "a", "b-c");
        List<DynamicKafkaSourceSplit> report = List.of(activeSplit, retainedSplit);

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createModeProperties("per_cluster"),
                                createRestoredState(stream, "a", List.of(activeSplit)),
                                bounded)) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, report);
            // Reader 0 fails while the initial gate still waits for reader 1.
            context.unregisterReader(0);
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, report);
            registerReaderAndReportRestoredSplits(context, enumerator, 1, Collections.emptyList());
            context.runNextOneTimeCallable();
            completePartitionDiscovery(getClusterEnumerator(enumerator, "a"));

            assertExactCheckpointOffsets(context, 0, report);
            assertSinglePhysicalOwners(context, 0, report.size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRestoredPendingReportsKeepPartitionsWithCollidingSplitIds(boolean bounded)
            throws Throwable {
        DynamicKafkaSourceSplit activeSplit = createSplit("a", "b-c", 123L, 1000L);
        DynamicKafkaSourceSplit retainedSplit =
                createSplit("a-b", "c", 456L, 2000L).retainUntil(Long.MAX_VALUE);
        assertThat(activeSplit.splitId()).isEqualTo(retainedSplit.splitId());
        KafkaStream stream = createKafkaStream("stream", "a", "b-c");
        List<DynamicKafkaSourceSplit> report = List.of(activeSplit, retainedSplit);
        // A checkpoint taken while the initial gate was pending carries reader 0's report.
        DynamicKafkaSourceEnumState restoredState =
                serdeRoundTrip(
                        new DynamicKafkaSourceEnumState(
                                Collections.singleton(stream),
                                Collections.singletonMap(
                                        "a",
                                        new KafkaSourceEnumState(
                                                unwrapSplits(List.of(activeSplit)),
                                                Collections.emptyList(),
                                                true)),
                                Collections.emptyMap(),
                                Collections.singletonMap(0, report)));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createModeProperties("per_cluster"),
                                restoredState,
                                bounded)) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, report);
            registerReaderAndReportRestoredSplits(context, enumerator, 1, Collections.emptyList());
            context.runNextOneTimeCallable();
            completePartitionDiscovery(getClusterEnumerator(enumerator, "a"));

            assertExactCheckpointOffsets(context, 0, report);
            assertSinglePhysicalOwners(context, 0, report.size());
        }
    }

    @ParameterizedTest
    @MethodSource("collidingSplitModes")
    public void testInitialRecoveryKeepsActivePartitionsWithCollidingSplitIds(
            String mode, boolean bounded) throws Throwable {
        DynamicKafkaSourceSplit firstSplit = createSplit("a", "b-c", 123L, 1000L);
        DynamicKafkaSourceSplit secondSplit = createSplit("a-b", "c", 456L, 2000L);
        assertThat(firstSplit.splitId()).isEqualTo(secondSplit.splitId());
        KafkaStream stream = createKafkaStream("stream", Map.of("a", "b-c", "a-b", "c"));
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(
                        stream, Map.of("a", List.of(firstSplit), "a-b", List.of(secondSplit)));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createModeProperties(mode),
                                restoredState,
                                bounded)) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(firstSplit));
            registerReaderAndReportRestoredSplits(context, enumerator, 1, List.of(secondSplit));
            context.runNextOneTimeCallable();
            completePartitionDiscovery(getClusterEnumerator(enumerator, "a"));
            completePartitionDiscovery(getClusterEnumerator(enumerator, "a-b"));

            assertExactCheckpointOffsets(context, 0, List.of(firstSplit, secondSplit));
            assertSinglePhysicalOwners(context, 0, 2);
        }
    }

    @Test
    public void testInitialRecoveryRejectsPartitionReportedByTwoReaders() throws Throwable {
        DynamicKafkaSourceSplit split = createSplit("cluster-0", "topic", 123L, 1000L);
        KafkaStream stream = createKafkaStream("stream", "cluster-0", "topic");

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createGlobalModeProperties(),
                                createRestoredState(stream, "cluster-0", List.of(split)))) {
            enumerator.start();
            context.runNextOneTimeCallable();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(split));

            assertThatThrownBy(
                            () ->
                                    registerReaderAndReportRestoredSplits(
                                            context,
                                            enumerator,
                                            1,
                                            List.of(withOffset(split, 456L))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("kafkaClusterId='cluster-0'")
                    .hasMessageContaining("reported by both reader 0 and reader 1");
        }
    }

    @ParameterizedTest
    @MethodSource("collidingSplitModes")
    public void testLocalRestartKeepsActivePartitionsWithCollidingSplitIds(
            String mode, boolean bounded) throws Throwable {
        DynamicKafkaSourceSplit firstSplit = createSplit("a", "b-c", 10L, 1000L);
        DynamicKafkaSourceSplit secondSplit = createSplit("a-b", "c", 20L, 2000L);
        assertThat(firstSplit.splitId()).isEqualTo(secondSplit.splitId());
        KafkaStream stream = createKafkaStream("stream", Map.of("a", "b-c", "a-b", "c"));

        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(1);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createModeProperties(mode),
                                new DynamicKafkaSourceEnumState(),
                                bounded)) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, Collections.emptyList());
            context.runNextOneTimeCallable();
            completePartitionDiscovery(
                    getClusterEnumerator(enumerator, "a"), firstSplit.getKafkaPartitionSplit());
            completePartitionDiscovery(
                    getClusterEnumerator(enumerator, "a-b"), secondSplit.getKafkaPartitionSplit());
            assertExactCheckpointOffsets(context, 0, List.of(firstSplit, secondSplit));

            int priorAssignments = context.getSplitsAssignmentSequence().size();
            List<DynamicKafkaSourceSplit> report =
                    List.of(withOffset(firstSplit, 500L), withOffset(secondSplit, 600L));
            context.unregisterReader(0);
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, report);

            assertExactCheckpointOffsets(context, priorAssignments, report);
        }
    }

    private static Stream<Arguments> collidingSplitModes() {
        return Stream.of(
                Arguments.of("per_cluster", true),
                Arguments.of("per_cluster", false),
                Arguments.of("global", true),
                Arguments.of("global", false));
    }

    @Test
    public void testLocalFailurePreservesStillInactiveRetainedSplit() throws Throwable {
        String cluster = "cluster-0";
        KafkaStream stream = createKafkaStream("stream", cluster, "active-topic");
        DynamicKafkaSourceSplit retained =
                new DynamicKafkaSourceSplit(
                                cluster,
                                new KafkaPartitionSplit(
                                        new TopicPartition("inactive-topic", 0), 123L))
                        .retainUntil(Long.MAX_VALUE);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createGlobalModeProperties(),
                                createRestoredState(stream, cluster, Collections.emptyList()))) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(retained));
            registerReaderAndReportRestoredSplits(context, enumerator, 1, Collections.emptyList());
            context.runNextOneTimeCallable();
            assertExactCheckpointOffsets(context, 0, List.of(retained));

            int priorAssignments = context.getSplitsAssignmentSequence().size();
            context.unregisterReader(0);
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, List.of(retained));

            assertExactCheckpointOffsets(context, priorAssignments, List.of(retained));
            context.getSplitsAssignmentSequence().stream()
                    .skip(priorAssignments)
                    .forEach(assignment -> assertThat(assignment.assignment()).containsOnlyKeys(0));
        }
    }

    @Test
    public void testLocalFailureAfterNewCheckpointUsesReportedOffsets() throws Throwable {
        verifyNumericCheckpointRecovery(false, true, 1, false, false, true, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLocalFailurePreservesCurrentOwnersAcrossClusterRetention(
            boolean failWhileInactive) throws Throwable {
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        List<DynamicKafkaSourceSplit> checkpointSplits =
                createSplits(clusterId, topic, 11).stream()
                        .map(
                                split ->
                                        withOffset(
                                                split,
                                                1000L
                                                        + split.getKafkaPartitionSplit()
                                                                .getPartition()))
                        .collect(Collectors.toList());
        List<DynamicKafkaSourceSplit> historicalReaderZeroSplits =
                checkpointSplits.stream()
                        .filter(split -> split.getKafkaPartitionSplit().getPartition() % 2 == 0)
                        .collect(Collectors.toList());
        List<DynamicKafkaSourceSplit> historicalReaderOneSplits =
                checkpointSplits.stream()
                        .filter(split -> split.getKafkaPartitionSplit().getPartition() % 2 == 1)
                        .collect(Collectors.toList());
        KafkaStream stream = createKafkaStream(streamId, clusterId, topic);
        Properties properties = createGlobalModeProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "1");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");

        try (MockKafkaMetadataService metadataService =
                        new MockKafkaMetadataService(Collections.singleton(stream));
                MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                metadataService,
                                context,
                                properties,
                                createRestoredState(stream, clusterId, checkpointSplits))) {
            enumerator.start();
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 0, historicalReaderZeroSplits);
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 1, historicalReaderOneSplits);
            context.runPeriodicCallable(0);
            assertExactCheckpointOffsets(context, 0, checkpointSplits);

            Map<Integer, List<DynamicKafkaSourceSplit>> currentAssignments = new HashMap<>();
            context.getSplitsAssignmentSequence()
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            currentAssignments
                                                                    .computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            assertThat(historicalReaderZeroSplits.stream())
                    .anyMatch(currentAssignments.get(1)::contains);

            metadataService.setKafkaStreams(
                    Collections.singleton(new KafkaStream(streamId, Collections.emptyMap())));
            context.runPeriodicCallable(0);
            assertThat(enumerator.snapshotState(1L).getRetainedClusterEnumeratorStates())
                    .containsKey(clusterId);

            if (!failWhileInactive) {
                metadataService.setKafkaStreams(Collections.singleton(stream));
                context.runPeriodicCallable(0);
            }
            int assignmentsBeforeFailure = context.getSplitsAssignmentSequence().size();
            context.unregisterReader(0);
            enumerator.addSplitsBack(currentAssignments.get(0), 0);
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 0, historicalReaderZeroSplits);

            List<DynamicKafkaSourceSplit> recoveredSplits = new ArrayList<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()
                            .subList(
                                    assignmentsBeforeFailure,
                                    context.getSplitsAssignmentSequence().size())) {
                assertThat(assignment.assignment()).containsOnlyKeys(0);
                recoveredSplits.addAll(assignment.assignment().get(0));
            }
            assertThat(recoveredSplits)
                    .allSatisfy(
                            split -> assertThat(split.isRetained()).isEqualTo(failWhileInactive));
            assertThat(
                            recoveredSplits.stream()
                                    .map(DynamicKafkaSourceSplit::clearRetention)
                                    .collect(Collectors.toList()))
                    .containsExactlyInAnyOrderElementsOf(currentAssignments.get(0));

            if (failWhileInactive) {
                int assignmentsBeforeReactivation = context.getSplitsAssignmentSequence().size();
                metadataService.setKafkaStreams(Collections.singleton(stream));
                context.runPeriodicCallable(0);
                assertThat(context.getSplitsAssignmentSequence())
                        .hasSize(assignmentsBeforeReactivation);
            }
        }
    }

    @Test
    public void testCheckpointReportsReplacePreinitializedPendingOwnersAndOffsets()
            throws Throwable {
        verifyNumericCheckpointRecovery(true, true, 3, true, true, false, true);
    }

    @Test
    public void testNewPartitionDiscoveryWaitsForRecoveryAndSurvivesRepeatedLocalFailure()
            throws Throwable {
        String cluster = "cluster-0";
        KafkaStream stream = createKafkaStream("stream", cluster, "topic");
        List<DynamicKafkaSourceSplit> checkpointSplits = createSplits(cluster, "topic", 2);
        DynamicKafkaSourceSplit discovered =
                withOffset(createSplits(cluster, "topic", 3).get(2), 2500L);
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(2);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                "stream",
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                createGlobalModeProperties(),
                                createRestoredState(stream, cluster, checkpointSplits))) {
            enumerator.start();
            context.runNextOneTimeCallable();
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 0, Collections.singletonList(checkpointSplits.get(0)));
            // Invoke the real coordinator callback with a completed broker-discovery result.
            // No broker or mock owner-selection implementation participates in the regression.
            Object subEnumerator =
                    ((Map<?, ?>) Whitebox.getInternalState(enumerator, "clusterEnumeratorMap"))
                            .get(cluster);
            Class<?> changeClass =
                    Class.forName(KafkaSourceEnumerator.class.getName() + "$PartitionSplitChange");
            Constructor<?> constructor = changeClass.getDeclaredConstructor(Set.class, Set.class);
            constructor.setAccessible(true);
            Method handler =
                    KafkaSourceEnumerator.class.getDeclaredMethod(
                            "handlePartitionSplitChanges", changeClass, Throwable.class);
            handler.setAccessible(true);
            handler.invoke(
                    subEnumerator,
                    constructor.newInstance(
                            Collections.singleton(discovered.getKafkaPartitionSplit()),
                            Collections.emptySet()),
                    null);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            registerReaderAndReportRestoredSplits(
                    context, enumerator, 1, Collections.singletonList(checkpointSplits.get(1)));
            List<DynamicKafkaSourceSplit> expected = new ArrayList<>(checkpointSplits);
            expected.add(discovered);
            assertExactCheckpointOffsets(context, 0, expected);
            Map<Integer, List<DynamicKafkaSourceSplit>> actualOwners = new HashMap<>();
            context.getSplitsAssignmentSequence()
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            actualOwners
                                                                    .computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            int failedReader =
                    actualOwners.entrySet().stream()
                            .filter(entry -> entry.getValue().contains(discovered))
                            .findFirst()
                            .get()
                            .getKey();
            for (int attempt = 0; attempt < 2; attempt++) {
                int priorAssignments = context.getSplitsAssignmentSequence().size();
                context.unregisterReader(failedReader);
                enumerator.addSplitsBack(actualOwners.get(failedReader), failedReader);
                registerReaderAndReportRestoredSplits(
                        context,
                        enumerator,
                        failedReader,
                        Collections.singletonList(checkpointSplits.get(failedReader)));
                assertExactCheckpointOffsets(
                        context, priorAssignments, actualOwners.get(failedReader));
                context.getSplitsAssignmentSequence().stream()
                        .skip(priorAssignments)
                        .forEach(
                                assignment ->
                                        assertThat(assignment.assignment())
                                                .containsOnlyKeys(failedReader));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("boundedRecoveryModes")
    public void testBoundedRecoveryAssignsAllClustersBeforeNoMoreSplits(
            String mode, boolean discoverFirst) throws Throwable {
        verifyBoundedRecoveryAssignmentOrder(mode, discoverFirst, true);
    }

    @ParameterizedTest
    @MethodSource("boundedRecoveryModes")
    public void testBoundedLocalRecoveryAssignsAllClustersBeforeNoMoreSplits(
            String mode, boolean discoverFirst) throws Throwable {
        verifyBoundedRecoveryAssignmentOrder(mode, discoverFirst, false);
    }

    private void verifyBoundedRecoveryAssignmentOrder(
            String mode, boolean discoverFirst, boolean checkInitialOrder) throws Throwable {
        Map<String, ClusterMetadata> metadata = new HashMap<>();
        Map<String, KafkaSourceEnumState> states = new HashMap<>();
        Map<Integer, List<DynamicKafkaSourceSplit>> reports = new HashMap<>();
        for (String cluster : List.of("cluster-0", "cluster-1")) {
            metadata.putAll(createKafkaStream("stream", cluster, "topic").getClusterMetadataMap());
            List<DynamicKafkaSourceSplit> splits = new ArrayList<>();
            for (int reader = 0; reader < 2; reader++) {
                DynamicKafkaSourceSplit split =
                        new DynamicKafkaSourceSplit(
                                cluster,
                                new KafkaPartitionSplit(
                                        new TopicPartition("topic", reader), 100L + reader, 1000L));
                splits.add(split);
                reports.computeIfAbsent(reader, ignored -> new ArrayList<>()).add(split);
            }
            states.put(
                    cluster,
                    new KafkaSourceEnumState(unwrapSplits(splits), Collections.emptyList(), true));
        }
        KafkaStream stream = new KafkaStream("stream", metadata);
        Properties properties = createBaseProperties();
        properties.setProperty(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), mode);
        List<String> events = new ArrayList<>();
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<DynamicKafkaSourceSplit>(2) {
                            @Override
                            public void assignSplits(
                                    SplitsAssignment<DynamicKafkaSourceSplit> assignment) {
                                assignment
                                        .assignment()
                                        .forEach(
                                                (reader, splits) ->
                                                        splits.forEach(
                                                                split ->
                                                                        events.add(
                                                                                "assign:"
                                                                                        + reader
                                                                                        + ":"
                                                                                        + split
                                                                                                .splitId())));
                                super.assignSplits(assignment);
                            }

                            @Override
                            public void signalNoMoreSplits(int reader) {
                                events.add("done:" + reader);
                                super.signalNoMoreSplits(reader);
                            }
                        };
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Collections.singleton("stream")),
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                OffsetsInitializer.earliest(),
                                OffsetsInitializer.latest(),
                                properties,
                                Boundedness.BOUNDED,
                                new DynamicKafkaSourceEnumState(
                                        Collections.singleton(stream), states),
                                new NoOpKafkaEnumContextProxyFactory())) {
            enumerator.start();
            // Complete actual sub-enumerator discovery callbacks without broker I/O. Each
            // sub-enumerator must subsequently signal completion during its addReader flush.
            Map<?, ?> subEnumerators =
                    (Map<?, ?>) Whitebox.getInternalState(enumerator, "clusterEnumeratorMap");
            Class<?> changeClass =
                    Class.forName(KafkaSourceEnumerator.class.getName() + "$PartitionSplitChange");
            Constructor<?> constructor = changeClass.getDeclaredConstructor(Set.class, Set.class);
            constructor.setAccessible(true);
            Method handler =
                    KafkaSourceEnumerator.class.getDeclaredMethod(
                            "handlePartitionSplitChanges", changeClass, Throwable.class);
            handler.setAccessible(true);
            for (Object subEnumerator : subEnumerators.values()) {
                handler.invoke(
                        subEnumerator,
                        constructor.newInstance(Collections.emptySet(), Collections.emptySet()),
                        null);
            }
            if (discoverFirst) {
                context.runNextOneTimeCallable();
            }
            for (int reader = 0; reader < 2; reader++) {
                registerReaderAndReportRestoredSplits(
                        context, enumerator, reader, reports.get(reader));
            }
            if (!discoverFirst) {
                context.runNextOneTimeCallable();
            }
            if (checkInitialOrder) {
                assertBoundedAssignmentsBeforeCompletion(events, 0, 2);
                assertBoundedAssignmentsBeforeCompletion(events, 1, 2);
            }
            Map<Integer, List<DynamicKafkaSourceSplit>> assigned = new HashMap<>();
            context.getSplitsAssignmentSequence()
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            assigned.computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            events.clear();
            int priorAssignments = context.getSplitsAssignmentSequence().size();
            context.unregisterReader(0);
            context.unregisterReader(1);
            List<DynamicKafkaSourceSplit> readerZeroReports = new ArrayList<>(assigned.get(0));
            readerZeroReports.add(
                    new DynamicKafkaSourceSplit(
                                    "removed-cluster",
                                    new KafkaPartitionSplit(
                                            new TopicPartition("removed-topic", 0), 5L, 1000L))
                            .retainUntil(Long.MAX_VALUE));
            enumerator.addSplitsBack(Collections.emptyList(), 0);
            registerReaderAndReportRestoredSplits(context, enumerator, 0, readerZeroReports);

            assertBoundedAssignmentsBeforeCompletion(events, 0, 3);
            assertThat(events).noneMatch(event -> event.startsWith("assign:1:"));
            assertThat(events).doesNotContain("done:1");
            assertExactCheckpointOffsets(context, priorAssignments, readerZeroReports);
            context.getSplitsAssignmentSequence().stream()
                    .skip(priorAssignments)
                    .flatMap(assignment -> assignment.assignment().values().stream())
                    .flatMap(List::stream)
                    .forEach(
                            split ->
                                    assertThat(split.getKafkaPartitionSplit().getStoppingOffset())
                                            .contains(1000L));
            registerReaderAndReportRestoredSplits(context, enumerator, 1, assigned.get(1));
            assertBoundedAssignmentsBeforeCompletion(events, 1, 2);
            assertThat(events.stream().filter(event -> event.equals("done:0"))).hasSize(1);
            assertThat(events.stream().filter(event -> event.equals("done:1"))).hasSize(1);
        }
    }

    private static Stream<Arguments> boundedRecoveryModes() {
        return Stream.of(
                Arguments.of("per_cluster", true),
                Arguments.of("per_cluster", false),
                Arguments.of("global", true),
                Arguments.of("global", false));
    }

    private static void assertBoundedAssignmentsBeforeCompletion(
            List<String> events, int reader, int expectedSplits) {
        String assignmentPrefix = "assign:" + reader + ":";
        int completion = events.indexOf("done:" + reader);
        assertThat(completion).as("completion must be signaled: %s", events).isNotNegative();
        assertThat(events.stream().filter(event -> event.startsWith(assignmentPrefix)))
                .hasSize(expectedSplits);
        assertThat(
                        events.subList(0, completion).stream()
                                .filter(event -> event.startsWith(assignmentPrefix)))
                .as("all restored assignments must precede completion: %s", events)
                .hasSize(expectedSplits);
    }

    private void verifyNumericCheckpointRecovery(boolean discoverFirst, boolean localFailure)
            throws Throwable {
        verifyNumericCheckpointRecovery(discoverFirst, localFailure, 3, false, false, false, false);
    }

    private void verifyNumericCheckpointRecovery(
            boolean discoverFirst,
            boolean localFailure,
            int failedReaderMask,
            boolean reverseRegistration,
            boolean interleavedReturns,
            boolean checkpointAfterRedistribution,
            boolean restoredPendingCopies)
            throws Throwable {
        verifyNumericCheckpointRecovery(
                discoverFirst,
                localFailure,
                failedReaderMask,
                reverseRegistration,
                interleavedReturns,
                checkpointAfterRedistribution,
                restoredPendingCopies,
                "global");
    }

    private void verifyNumericCheckpointRecovery(
            boolean discoverFirst,
            boolean localFailure,
            int failedReaderMask,
            boolean reverseRegistration,
            boolean interleavedReturns,
            boolean checkpointAfterRedistribution,
            boolean restoredPendingCopies,
            String mode)
            throws Throwable {
        verifyNumericCheckpointRecovery(
                discoverFirst,
                localFailure,
                failedReaderMask,
                reverseRegistration,
                interleavedReturns,
                checkpointAfterRedistribution,
                restoredPendingCopies,
                mode,
                false);
    }

    private void verifyNumericCheckpointRecovery(
            boolean discoverFirst,
            boolean localFailure,
            int failedReaderMask,
            boolean reverseRegistration,
            boolean interleavedReturns,
            boolean checkpointAfterRedistribution,
            boolean restoredPendingCopies,
            String mode,
            boolean retainedCheckpointReports)
            throws Throwable {
        String streamId = "stream";
        String clusterId = "cluster-0";
        String topic = "topic";
        int parallelism = 2;
        List<DynamicKafkaSourceSplit> checkpointSplits =
                createSplits(clusterId, topic, 11).stream()
                        .map(
                                split ->
                                        new DynamicKafkaSourceSplit(
                                                clusterId,
                                                new KafkaPartitionSplit(
                                                        split.getKafkaPartitionSplit()
                                                                .getTopicPartition(),
                                                        1000L
                                                                + split.getKafkaPartitionSplit()
                                                                        .getTopicPartition()
                                                                        .partition())))
                        .collect(Collectors.toList());
        KafkaStream stream = createKafkaStream(streamId, clusterId, topic);
        Properties properties = createBaseProperties();
        properties.setProperty(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), mode);
        Map<Integer, List<DynamicKafkaSourceSplit>> checkpointOwners = new HashMap<>();
        for (DynamicKafkaSourceSplit split : checkpointSplits) {
            int owner =
                    split.getKafkaPartitionSplit().getTopicPartition().partition() % parallelism;
            checkpointOwners
                    .computeIfAbsent(owner, ignored -> new ArrayList<>())
                    .add(retainedCheckpointReports ? split.retainUntil(Long.MAX_VALUE) : split);
        }
        DynamicKafkaSourceEnumState restoredState =
                createRestoredState(stream, clusterId, checkpointSplits);
        if (restoredPendingCopies) {
            restoredState =
                    new DynamicKafkaSourceEnumState(
                            Collections.singleton(stream),
                            Collections.singletonMap(
                                    clusterId,
                                    new KafkaSourceEnumState(
                                            Collections.emptyList(),
                                            checkpointSplits.stream()
                                                    .map(
                                                            split ->
                                                                    withOffset(split, 9000L)
                                                                            .getKafkaPartitionSplit())
                                                    .collect(Collectors.toList()),
                                            true)));
        }
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(parallelism);
                DynamicKafkaSourceEnumerator enumerator =
                        createEnumerator(
                                streamId,
                                new MockKafkaMetadataService(Collections.singleton(stream)),
                                context,
                                properties,
                                restoredState)) {
            enumerator.start();
            if (discoverFirst) {
                context.runNextOneTimeCallable();
            }
            for (int reader = 0; reader < parallelism; reader++) {
                registerReaderAndReportRestoredSplits(
                        context, enumerator, reader, checkpointOwners.get(reader));
            }
            if (!discoverFirst) {
                context.runNextOneTimeCallable();
            }
            assertSinglePhysicalOwners(context, 0, 11);
            assertExactCheckpointOffsets(context, 0, checkpointSplits);
            if (!localFailure) {
                return;
            }
            // Model SourceCoordinator local recovery: return all
            // assignments since the restored checkpoint, then register original checkpoint reports.
            Map<Integer, List<DynamicKafkaSourceSplit>> uncheckpointedAssignments = new HashMap<>();
            for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                    context.getSplitsAssignmentSequence()) {
                assignment
                        .assignment()
                        .forEach(
                                (reader, splits) ->
                                        uncheckpointedAssignments
                                                .computeIfAbsent(
                                                        reader, ignored -> new ArrayList<>())
                                                .addAll(splits));
            }
            int assignmentsBeforeFailure = context.getSplitsAssignmentSequence().size();
            Map<Integer, List<DynamicKafkaSourceSplit>> currentAssignments = new HashMap<>();
            uncheckpointedAssignments.forEach(
                    (reader, splits) -> currentAssignments.put(reader, new ArrayList<>(splits)));
            if (checkpointAfterRedistribution) {
                // Runtime no longer returns assignments covered by a completed checkpoint.
                // Reader reports now use the current owner and the checkpointed next offset.
                checkpointOwners.clear();
                currentAssignments.forEach(
                        (reader, splits) ->
                                checkpointOwners.put(
                                        reader,
                                        splits.stream()
                                                .map(
                                                        split ->
                                                                withOffset(
                                                                        split,
                                                                        split.getKafkaPartitionSplit()
                                                                                        .getStartingOffset()
                                                                                + 10000L))
                                                .collect(Collectors.toList())));
                uncheckpointedAssignments.replaceAll((reader, splits) -> Collections.emptyList());
            }
            for (int reader = 0; reader < parallelism; reader++) {
                if ((failedReaderMask & (1 << reader)) != 0) {
                    context.unregisterReader(reader);
                }
            }
            if (!interleavedReturns) {
                for (int reader = 0; reader < parallelism; reader++) {
                    if ((failedReaderMask & (1 << reader)) != 0) {
                        enumerator.addSplitsBack(uncheckpointedAssignments.get(reader), reader);
                    }
                }
            }
            for (int index = 0; index < parallelism; index++) {
                int reader = reverseRegistration ? parallelism - 1 - index : index;
                if ((failedReaderMask & (1 << reader)) != 0) {
                    if (interleavedReturns) {
                        enumerator.addSplitsBack(uncheckpointedAssignments.get(reader), reader);
                    }
                    registerReaderAndReportRestoredSplits(
                            context, enumerator, reader, checkpointOwners.get(reader));
                }
            }
            Map<Integer, List<DynamicKafkaSourceSplit>> assignmentsAfterFailure = new HashMap<>();
            context.getSplitsAssignmentSequence().stream()
                    .skip(assignmentsBeforeFailure)
                    .forEach(
                            assignment ->
                                    assignment
                                            .assignment()
                                            .forEach(
                                                    (reader, splits) ->
                                                            assignmentsAfterFailure
                                                                    .computeIfAbsent(
                                                                            reader,
                                                                            ignored ->
                                                                                    new ArrayList<>())
                                                                    .addAll(splits)));
            for (int reader = 0; reader < parallelism; reader++) {
                if ((failedReaderMask & (1 << reader)) == 0) {
                    assertThat(assignmentsAfterFailure).doesNotContainKey(reader);
                } else {
                    List<DynamicKafkaSourceSplit> expected =
                            checkpointAfterRedistribution
                                    ? checkpointOwners.get(reader)
                                    : currentAssignments.get(reader);
                    assertThat(assignmentsAfterFailure.get(reader))
                            .as(
                                    "failed reader %s keeps its actual owner and checkpoint offsets",
                                    reader)
                            .containsExactlyInAnyOrderElementsOf(expected);
                }
            }
        }
    }

    private static DynamicKafkaSourceSplit withOffset(DynamicKafkaSourceSplit split, long offset) {
        return new DynamicKafkaSourceSplit(
                split.getKafkaClusterId(),
                new KafkaPartitionSplit(
                        split.getKafkaPartitionSplit().getTopicPartition(), offset));
    }

    private static KafkaSourceEnumerator getClusterEnumerator(
            DynamicKafkaSourceEnumerator enumerator, String clusterId) {
        return (KafkaSourceEnumerator)
                ((Map<?, ?>) Whitebox.getInternalState(enumerator, "clusterEnumeratorMap"))
                        .get(clusterId);
    }

    private static void completePartitionDiscovery(
            KafkaSourceEnumerator enumerator, KafkaPartitionSplit... discoveredSplits)
            throws Exception {
        Class<?> changeClass =
                Class.forName(KafkaSourceEnumerator.class.getName() + "$PartitionSplitChange");
        Constructor<?> constructor = changeClass.getDeclaredConstructor(Set.class, Set.class);
        constructor.setAccessible(true);
        Method handler =
                KafkaSourceEnumerator.class.getDeclaredMethod(
                        "handlePartitionSplitChanges", changeClass, Throwable.class);
        handler.setAccessible(true);
        handler.invoke(
                enumerator,
                constructor.newInstance(Set.of(discoveredSplits), Collections.emptySet()),
                null);
    }

    private static void assertExactCheckpointOffsets(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            int skipAssignments,
            List<DynamicKafkaSourceSplit> expected) {
        assertThat(
                        context.getSplitsAssignmentSequence().stream()
                                .skip(skipAssignments)
                                .flatMap(assignment -> assignment.assignment().values().stream())
                                .flatMap(List::stream)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void assertSinglePhysicalOwners(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            int skipAssignments,
            int expectedPartitions) {
        Map<Tuple2<String, TopicPartition>, Set<Integer>> owners = new HashMap<>();
        context.getSplitsAssignmentSequence().stream()
                .skip(skipAssignments)
                .forEach(
                        assignment ->
                                assignment
                                        .assignment()
                                        .forEach(
                                                (reader, splits) ->
                                                        splits.forEach(
                                                                split ->
                                                                        owners.computeIfAbsent(
                                                                                        DynamicKafkaSourceEnumerator
                                                                                                .toPartitionKey(
                                                                                                        split),
                                                                                        ignored ->
                                                                                                new HashSet<>())
                                                                                .add(reader))));
        assertThat(owners).hasSize(expectedPartitions);
        assertThat(owners.entrySet())
                .as("every physical split must have one active reader: %s", owners)
                .allSatisfy(entry -> assertThat(entry.getValue()).hasSize(1));
    }

    private static void registerReaderAndReportRestoredSplits(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            DynamicKafkaSourceEnumerator enumerator,
            int readerId,
            List<DynamicKafkaSourceSplit> restoredSplits) {
        context.registerReader(
                ReaderInfo.createReaderInfo(readerId, "location-" + readerId, restoredSplits));
        enumerator.addReader(readerId);
    }

    private static void registerReadersWithoutReportedSplits(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            DynamicKafkaSourceEnumerator enumerator,
            int parallelism) {
        for (int reader = 0; reader < parallelism; reader++) {
            context.registerReader(
                    ReaderInfo.createReaderInfo(
                            reader, "location-" + reader, Collections.emptyList()));
            enumerator.addReader(reader);
        }
    }

    private static DynamicKafkaSourceEnumState serdeRoundTrip(DynamicKafkaSourceEnumState state)
            throws Exception {
        DynamicKafkaSourceEnumStateSerializer serializer =
                new DynamicKafkaSourceEnumStateSerializer();
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(state));
    }

    /**
     * Asserts the assigned splits match the expected splits by id, including multiplicity, so a
     * duplicate assignment of the same split cannot go unnoticed.
     */
    private static void assertAssignedSplitIdsExactly(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            List<DynamicKafkaSourceSplit> expectedSplits) {
        List<String> assignedSplitIds = new ArrayList<>();
        for (DynamicKafkaSourceSplit split : collectAssignedSplits(context)) {
            assignedSplitIds.add(split.splitId());
        }
        List<String> expectedSplitIds = new ArrayList<>();
        for (DynamicKafkaSourceSplit split : expectedSplits) {
            expectedSplitIds.add(split.splitId());
        }
        assertThat(assignedSplitIds).containsExactlyInAnyOrderElementsOf(expectedSplitIds);
    }

    private static List<DynamicKafkaSourceSplit> collectAssignedSplits(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context) {
        List<DynamicKafkaSourceSplit> assignedSplits = new ArrayList<>();
        for (SplitsAssignment<DynamicKafkaSourceSplit> assignment :
                context.getSplitsAssignmentSequence()) {
            for (List<DynamicKafkaSourceSplit> readerSplits : assignment.assignment().values()) {
                assignedSplits.addAll(readerSplits);
            }
        }
        return assignedSplits;
    }

    private static DynamicKafkaSourceEnumState createRestoredState(
            KafkaStream kafkaStream, String clusterId, List<DynamicKafkaSourceSplit> activeSplits) {
        return createRestoredState(kafkaStream, Collections.singletonMap(clusterId, activeSplits));
    }

    private static DynamicKafkaSourceEnumState createRestoredState(
            KafkaStream kafkaStream, Map<String, List<DynamicKafkaSourceSplit>> splitsByCluster) {
        Map<String, KafkaSourceEnumState> clusterStates = new HashMap<>();
        splitsByCluster.forEach(
                (clusterId, activeSplits) ->
                        clusterStates.put(
                                clusterId,
                                new KafkaSourceEnumState(
                                        unwrapSplits(activeSplits),
                                        Collections.emptyList(),
                                        true)));
        return new DynamicKafkaSourceEnumState(Collections.singleton(kafkaStream), clusterStates);
    }

    private static Properties createGlobalModeProperties() {
        return createModeProperties(
                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());
    }

    private static Properties createModeProperties(String mode) {
        Properties properties = createBaseProperties();
        properties.setProperty(DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(), mode);
        return properties;
    }

    private static Properties createBaseProperties() {
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), "0");
        return properties;
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            String streamId,
            KafkaMetadataService metadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            Properties properties,
            DynamicKafkaSourceEnumState restoredState) {
        return createEnumerator(
                streamId, metadataService, context, properties, restoredState, false);
    }

    private static DynamicKafkaSourceEnumerator createEnumerator(
            String streamId,
            KafkaMetadataService metadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            Properties properties,
            DynamicKafkaSourceEnumState restoredState,
            boolean bounded) {
        return new DynamicKafkaSourceEnumerator(
                new KafkaStreamSetSubscriber(Collections.singleton(streamId)),
                metadataService,
                context,
                OffsetsInitializer.earliest(),
                bounded ? OffsetsInitializer.latest() : new NoStoppingOffsetsInitializer(),
                properties,
                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED,
                restoredState,
                new NoOpKafkaEnumContextProxyFactory());
    }

    private static KafkaStream createKafkaStream(
            String streamId, String clusterId, String activeTopic) {
        return createKafkaStream(streamId, clusterId, Collections.singleton(activeTopic));
    }

    private static KafkaStream createKafkaStream(
            String streamId, Map<String, String> activeTopicByCluster) {
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Map<String, ClusterMetadata> clusterMetadata = new HashMap<>();
        activeTopicByCluster.forEach(
                (clusterId, topic) ->
                        clusterMetadata.put(
                                clusterId,
                                new ClusterMetadata(
                                        Collections.singleton(topic), clusterProperties)));
        return new KafkaStream(streamId, clusterMetadata);
    }

    private static KafkaStream createKafkaStream(
            String streamId, String clusterId, Set<String> activeTopics) {
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return new KafkaStream(
                streamId,
                Collections.singletonMap(
                        clusterId, new ClusterMetadata(activeTopics, clusterProperties)));
    }

    private static DynamicKafkaSourceSplit createSplit(
            String clusterId, String topic, long startingOffset, long stoppingOffset) {
        return new DynamicKafkaSourceSplit(
                clusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, 0), startingOffset, stoppingOffset));
    }

    private static List<DynamicKafkaSourceSplit> createSplits(
            String clusterId, String topic, int count) {
        List<DynamicKafkaSourceSplit> splits = new ArrayList<>();
        for (int partition = 0; partition < count; partition++) {
            splits.add(
                    new DynamicKafkaSourceSplit(
                            clusterId,
                            new KafkaPartitionSplit(
                                    new TopicPartition(topic, partition),
                                    KafkaPartitionSplit.EARLIEST_OFFSET)));
        }
        return splits;
    }

    private static List<KafkaPartitionSplit> unwrapSplits(
            List<DynamicKafkaSourceSplit> dynamicSplits) {
        List<KafkaPartitionSplit> splits = new ArrayList<>();
        for (DynamicKafkaSourceSplit split : dynamicSplits) {
            splits.add(split.getKafkaPartitionSplit());
        }
        return splits;
    }

    private static class NoOpKafkaEnumContextProxyFactory
            implements StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory {

        @Override
        public StoppableKafkaEnumContextProxy create(
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                Runnable signalNoMoreSplitsCallback) {
            return new NoOpKafkaEnumContextProxy(
                    kafkaClusterId, kafkaMetadataService, enumContext, signalNoMoreSplitsCallback);
        }
    }

    private static class NoOpKafkaEnumContextProxy extends StoppableKafkaEnumContextProxy {

        private NoOpKafkaEnumContextProxy(
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
                Runnable signalNoMoreSplitsCallback) {
            super(kafkaClusterId, kafkaMetadataService, enumContext, signalNoMoreSplitsCallback);
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {}

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {}
    }
}
