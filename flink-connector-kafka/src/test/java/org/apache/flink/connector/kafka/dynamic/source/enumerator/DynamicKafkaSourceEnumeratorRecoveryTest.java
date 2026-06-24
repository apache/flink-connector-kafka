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
