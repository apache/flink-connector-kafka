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

package org.apache.flink.connector.kafka.dynamic.source.reader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.RequestRetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.RetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.util.InstantiationUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Coordinator-authoritative retention and dormant-only handoff invariants. */
class DynamicKafkaSourceReaderRetentionTest {

    @Test
    void testMetadataCarriesImmutableRetentionAuthorityOverTheWire() throws Exception {
        Map<String, Long> deadlines = new HashMap<>();
        deadlines.put("removed", 10L);
        MetadataUpdateEvent event = new MetadataUpdateEvent(Collections.emptySet(), deadlines);
        deadlines.clear();
        MetadataUpdateEvent restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(event), getClass().getClassLoader());
        assertThat(restored).isEqualTo(event);
        assertThat(restored.getRetainedClusterDeadlines()).containsEntry("removed", 10L);
        assertThatThrownBy(() -> restored.getRetainedClusterDeadlines().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(new MetadataUpdateEvent(Collections.emptySet()))
                .isNotEqualTo(event)
                .isEqualTo(new MetadataUpdateEvent(Collections.emptySet(), Collections.emptyMap()));
    }

    @Test
    void testRestoredOffsetsWaitForAuthorityAndSurviveDelayedReaderClock() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        DynamicKafkaSourceSplit retained = split("removed", 42).retainUntil(1L);
        try (DynamicKafkaSourceReader<Integer> reader = createReader(context)) {
            reader.addSplits(List.of(retained));
            assertThat(reader.snapshotState(1)).containsExactly(retained);
            assertThat(request(reader, context, 1, "removed").getRetainedSplitOffsets()).isEmpty();
            assertThat(reader.snapshotState(2)).containsExactly(retained);

            reader.handleSourceEvents(metadata(Map.of("removed", 1L)));
            assertThat(request(reader, context, 2, "removed").getRetainedSplitOffsets())
                    .containsExactlyEntriesOf(Map.of(retained.splitId(), 42L));
            assertThat(reader.snapshotState(3)).containsExactly(retained);
            reader.handleSourceEvents(metadata(Map.of("removed", 1L)));
            assertThat(reader.snapshotState(4)).containsExactly(retained);
        }
    }

    @Test
    void testEpochReplacementNeverRetagsOldShadowsAndMissingAuthorityClearsThem() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        DynamicKafkaSourceSplit old = split("removed", 99).retainUntil(10L);
        DynamicKafkaSourceSplit current = split("removed", 20).retainUntil(20L);
        try (DynamicKafkaSourceReader<Integer> reader = createReader(context)) {
            reader.addSplits(List.of(old));
            reader.handleSourceEvents(metadata(Map.of("removed", 20L)));
            assertThat(reader.snapshotState(1)).isEmpty();
            reader.addSplits(List.of(old, current));
            assertThat(reader.snapshotState(2)).containsExactly(current);
            assertThat(request(reader, context, 1, "removed").getRetainedSplitOffsets())
                    .containsExactlyEntriesOf(Map.of(current.splitId(), 20L));

            reader.handleSourceEvents(new MetadataUpdateEvent(Collections.emptySet()));
            reader.addSplits(List.of(current));
            assertThat(reader.snapshotState(3)).isEmpty();
            assertThat(request(reader, context, 2, "removed").getRetainedSplitOffsets()).isEmpty();
        }
    }

    @Test
    void testPendingActiveRemovalUsesExactDeadlineAndRepeatedRemovalDoesNotRenewIt()
            throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        DynamicKafkaSourceSplit active = split("removed", 42);
        try (DynamicKafkaSourceReader<Integer> reader = createReader(context)) {
            reader.addSplits(List.of(active));
            reader.handleSourceEvents(metadata(Map.of("removed", 1L)));
            DynamicKafkaSourceSplit retained = active.retainUntil(1L);
            assertThat(reader.snapshotState(1)).containsExactly(retained);
            reader.handleSourceEvents(metadata(Map.of("removed", 1L)));
            assertThat(reader.snapshotState(2)).containsExactly(retained);
            reader.handleSourceEvents(metadata(Map.of("removed", 2L)));
            assertThat(reader.snapshotState(3)).isEmpty();
            assertThat(request(reader, context, 1, "removed").getRetainedSplitOffsets()).isEmpty();
        }
    }

    @Test
    void testActiveRemovalRetainsOnlyLiveSplitsAndReleasesTheirOutputs() throws Exception {
        try (DynamicKafkaSourceReader<Integer> reader = createReader(new TestingReaderContext())) {
            installSubReader(reader, "removed", 42);
            installSubReader(reader, "completed");
            reader.handleSourceEvents(metadata(Map.of("removed", 1L, "completed", 1L)));
            assertThat(reader.snapshotState(1))
                    .containsExactly(split("removed", 42).retainUntil(1L));
            TrackingReaderOutput output = new TrackingReaderOutput();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
            assertThat(output.releasedSplitIds).containsExactly(split("removed", 42).splitId());
            assertThat(reader.isAvailable()).isNotDone();
        }
    }

    @Test
    void testDormantHandoffAndCleanupDoNotPauseUnrelatedActiveReader() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        DynamicKafkaSourceSplit retained = split("removed", 42).retainUntil(1L);
        Set<KafkaStream> streams =
                Set.of(
                        new KafkaStream(
                                "stream",
                                Map.of(
                                        "active",
                                        new ClusterMetadata(Set.of("topic"), new Properties()))));
        try (DynamicKafkaSourceReader<Integer> reader = createReader(context)) {
            RecordingKafkaSourceReader activeReader = installSubReader(reader, "active", 10);
            reader.addSplits(List.of(retained));
            reader.handleSourceEvents(new MetadataUpdateEvent(streams, Map.of("removed", 1L)));
            CompletableFuture<Void> availability = reader.isAvailable();
            assertThat(availability).isDone();
            assertThat(request(reader, context, 1, "removed").getRetainedSplitOffsets())
                    .containsExactlyEntriesOf(Map.of(retained.splitId(), 42L));
            TrackingReaderOutput output = new TrackingReaderOutput();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(output.getEmittedRecords()).containsExactly(10);
            assertThat(reader.snapshotState(1))
                    .containsExactlyInAnyOrder(split("active", 11), retained);

            reader.handleSourceEvents(new MetadataUpdateEvent(streams));
            assertThat(reader.snapshotState(2)).containsExactly(split("active", 11));
            assertThat(activeReader.closeCount).isZero();
            assertThat(reader.isAvailable()).isDone();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(output.getEmittedRecords()).containsExactly(10, 11);
            assertThat(request(reader, context, 2, "removed").getRetainedSplitOffsets()).isEmpty();
        }
    }

    @Test
    void testAuthorityOnlyMetadataSkipsSnapshotsWithSparseOwnership() throws Exception {
        TestingReaderContext context = new TestingReaderContext();
        Set<KafkaStream> streams =
                Set.of(
                        new KafkaStream(
                                "stream",
                                Map.of(
                                        "active",
                                        new ClusterMetadata(Set.of("topic"), new Properties()),
                                        "empty",
                                        new ClusterMetadata(Set.of("topic"), new Properties()))));
        DynamicKafkaSourceSplit retained = split("removed", 42).retainUntil(1L);
        try (DynamicKafkaSourceReader<Integer> reader = createReader(context)) {
            RecordingKafkaSourceReader active = installSubReader(reader, "active", 10);
            RecordingKafkaSourceReader empty = installSubReader(reader, "empty", 20);
            reader.addSplits(List.of(retained));
            reader.handleSourceEvents(new MetadataUpdateEvent(streams, Map.of("removed", 1L)));

            // A subscribed cluster need not have a locally assigned split, for example after a
            // bounded partition finishes. Global metadata still includes both clusters.
            empty.state.clear();
            int activeSnapshots = active.snapshotCount;
            int emptySnapshots = empty.snapshotCount;
            active.forbidSnapshots = true;
            empty.forbidSnapshots = true;
            reader.handleSourceEvents(new MetadataUpdateEvent(streams));
            reader.handleSourceEvents(new MetadataUpdateEvent(streams));

            assertThat(active.snapshotCount).isEqualTo(activeSnapshots);
            assertThat(empty.snapshotCount).isEqualTo(emptySnapshots);
            assertThat(active.closeCount).isZero();
            assertThat(empty.closeCount).isZero();
            assertThat(request(reader, context, 1, "removed").getRetainedSplitOffsets()).isEmpty();
            assertThat(reader.isAvailable()).isDone();
            TrackingReaderOutput output = new TrackingReaderOutput();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(output.getEmittedRecords()).containsExactly(10);
            assertThat(output.releasedSplitIds).isEmpty();

            active.forbidSnapshots = false;
            empty.forbidSnapshots = false;
            assertThat(reader.snapshotState(1)).containsExactly(split("active", 11));
        }
    }

    private static MetadataUpdateEvent metadata(Map<String, Long> deadlines) {
        return new MetadataUpdateEvent(Collections.emptySet(), deadlines);
    }

    private static DynamicKafkaSourceReader<Integer> createReader(TestingReaderContext context) {
        Properties properties = new Properties();
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");
        return new DynamicKafkaSourceReader<>(
                context,
                KafkaRecordDeserializationSchema.valueOnly(IntegerDeserializer.class),
                properties);
    }

    private static RetainedSplitOffsetsEvent request(
            DynamicKafkaSourceReader<Integer> reader,
            TestingReaderContext context,
            long handoffId,
            String cluster) {
        reader.handleSourceEvents(new RequestRetainedSplitOffsetsEvent(handoffId, cluster));
        return (RetainedSplitOffsetsEvent)
                context.getSentEvents().get(context.getSentEvents().size() - 1);
    }

    private static DynamicKafkaSourceSplit split(String cluster, long offset) {
        return new DynamicKafkaSourceSplit(
                cluster, new KafkaPartitionSplit(new TopicPartition("topic", 0), offset));
    }

    @SuppressWarnings("unchecked")
    private static RecordingKafkaSourceReader installSubReader(
            DynamicKafkaSourceReader<Integer> reader, String cluster, long... offsets)
            throws Exception {
        RecordingKafkaSourceReader subReader = new RecordingKafkaSourceReader();
        for (long offset : offsets) {
            subReader.state.add(new KafkaPartitionSplit(new TopicPartition("topic", 0), offset));
        }
        Field readersField = DynamicKafkaSourceReader.class.getDeclaredField("clusterReaderMap");
        readersField.setAccessible(true);
        NavigableMap<String, KafkaSourceReader<Integer>> readers =
                (NavigableMap<String, KafkaSourceReader<Integer>>) readersField.get(reader);
        readers.put(cluster, subReader);
        Field availabilityField =
                DynamicKafkaSourceReader.class.getDeclaredField("availabilityHelper");
        availabilityField.setAccessible(true);
        availabilityField.set(reader, new MultipleFuturesAvailabilityHelper(readers.size()));
        return subReader;
    }

    private static final class RecordingKafkaSourceReader extends KafkaSourceReader<Integer> {
        private final List<KafkaPartitionSplit> state = new ArrayList<>();
        private int closeCount;
        private int snapshotCount;
        private boolean forbidSnapshots;

        private RecordingKafkaSourceReader() {
            this(new FutureCompletingBlockingQueue<>());
        }

        private RecordingKafkaSourceReader(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                        queue) {
            super(
                    queue,
                    new KafkaSourceFetcherManager(
                            queue,
                            () -> {
                                throw new AssertionError("This test does not need a Kafka fetcher");
                            },
                            ignored -> {}),
                    (record, output, state) -> {},
                    new Configuration(),
                    new TestingReaderContext(),
                    new KafkaSourceReaderMetrics(
                            UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
        }

        @Override
        public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
            if (forbidSnapshots) {
                throw new AssertionError(
                        "Authority-only metadata must not snapshot active readers");
            }
            snapshotCount++;
            return new ArrayList<>(state);
        }

        @Override
        public int getNumberOfCurrentlyAssignedSplits() {
            return state.size();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) {
            if (state.isEmpty()) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            KafkaPartitionSplit current = state.get(0);
            output.collect((int) current.getStartingOffset());
            state.set(
                    0,
                    new KafkaPartitionSplit(
                            current.getTopicPartition(), current.getStartingOffset() + 1));
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public void close() throws Exception {
            closeCount++;
            super.close();
        }
    }

    private static final class TrackingReaderOutput extends TestingReaderOutput<Integer> {
        private final List<String> releasedSplitIds = new ArrayList<>();

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public void releaseOutputForSplit(String splitId) {
            releasedSplitIds.add(splitId);
        }
    }
}
