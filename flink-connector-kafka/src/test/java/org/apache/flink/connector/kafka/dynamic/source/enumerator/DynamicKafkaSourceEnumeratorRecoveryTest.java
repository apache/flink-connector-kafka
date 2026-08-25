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
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

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

import static org.assertj.core.api.Assertions.assertThat;

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
        return new DynamicKafkaSourceEnumState(
                Collections.singleton(kafkaStream),
                Collections.singletonMap(
                        clusterId,
                        new KafkaSourceEnumState(
                                unwrapSplits(activeSplits), Collections.emptyList(), true)));
    }

    private static Properties createGlobalModeProperties() {
        Properties properties = createBaseProperties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());
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
        return new DynamicKafkaSourceEnumerator(
                new KafkaStreamSetSubscriber(Collections.singleton(streamId)),
                metadataService,
                context,
                OffsetsInitializer.earliest(),
                new NoStoppingOffsetsInitializer(),
                properties,
                Boundedness.CONTINUOUS_UNBOUNDED,
                restoredState,
                new NoOpKafkaEnumContextProxyFactory());
    }

    private static KafkaStream createKafkaStream(
            String streamId, String clusterId, String activeTopic) {
        return createKafkaStream(streamId, clusterId, Collections.singleton(activeTopic));
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
