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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.RequestRetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.mock.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Reader registration and retained-handoff fixtures for dynamic enumerator tests. */
final class DynamicKafkaSourceEnumeratorTestUtils {

    private DynamicKafkaSourceEnumeratorTestUtils() {}

    static void mockRegisterRestoredReader(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            DynamicKafkaSourceEnumerator enumerator,
            int reader,
            DynamicKafkaSourceEnumState checkpoint) {
        context.registerReader(
                ReaderInfo.createReaderInfo(
                        reader,
                        "location " + reader,
                        new ArrayList<>(
                                checkpointReaderReport(
                                        checkpoint, reader, context.currentParallelism()))));
        enumerator.addReader(reader);
        enumerator.handleSourceEvent(reader, new GetMetadataUpdateEvent());
    }

    /** Reader checkpoints contain concrete progress, separately from enumerator startup offsets. */
    static List<DynamicKafkaSourceSplit> checkpointReaderReport(
            DynamicKafkaSourceEnumState checkpoint, int reader, int parallelism) {
        List<DynamicKafkaSourceSplit> assigned = new ArrayList<>();
        new TreeMap<>(checkpoint.getClusterEnumeratorStates())
                .forEach(
                        (cluster, state) ->
                                state.assignedSplits()
                                        .forEach(
                                                split ->
                                                        assigned.add(
                                                                checkpointedReaderSplit(
                                                                        cluster, split))));
        checkpoint
                .getRetainedClusterEnumeratorStates()
                .forEach(
                        (cluster, retained) ->
                                retained.getKafkaSourceEnumState()
                                        .assignedSplits()
                                        .forEach(
                                                split ->
                                                        assigned.add(
                                                                checkpointedReaderSplit(
                                                                                cluster, split)
                                                                        .retainUntil(
                                                                                retained
                                                                                        .getRetainedUntilMs()))));
        assigned.sort(Comparator.comparing(DynamicKafkaSourceSplit::splitId));
        List<DynamicKafkaSourceSplit> report = new ArrayList<>();
        for (int index = reader; index < assigned.size(); index += parallelism) {
            report.add(assigned.get(index));
        }
        return report;
    }

    static DynamicKafkaSourceSplit checkpointedReaderSplit(
            String cluster, KafkaPartitionSplit split) {
        return new DynamicKafkaSourceSplit(
                cluster,
                new KafkaPartitionSplit(
                        split.getTopicPartition(),
                        split.getStartingOffset() >= 0
                                ? split.getStartingOffset()
                                : 10L + split.getPartition(),
                        split.getStoppingOffset().orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET)));
    }

    static Properties retainedSplitOffsetHandoffProperties(long metadataDiscoveryIntervalMs) {
        Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "0");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(),
                Long.toString(metadataDiscoveryIntervalMs));
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_METADATA_REMOVED_CLUSTER_RETENTION_MS.key(),
                "60000");
        properties.setProperty(
                DynamicKafkaSourceOptions.STREAM_ENUMERATOR_MODE.key(),
                DynamicKafkaSourceOptions.EnumeratorMode.GLOBAL.name().toLowerCase());
        return properties;
    }

    static MetadataUpdateEvent getLatestMetadataUpdateEvent(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context, int readerId)
            throws Exception {
        List<SourceEvent> sourceEvents = context.getSentSourceEvent().get(readerId);
        assertThat(sourceEvents)
                .as("source events should have been sent to reader %s", readerId)
                .isNotNull();
        return sourceEvents.stream()
                .filter(MetadataUpdateEvent.class::isInstance)
                .map(MetadataUpdateEvent.class::cast)
                .reduce((first, second) -> second)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        String.format(
                                                "metadata update event was not sent to reader %s",
                                                readerId)));
    }

    static RequestRetainedSplitOffsetsEvent getLatestRetainedSplitOffsetRequest(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context, int readerId)
            throws Exception {
        List<SourceEvent> sourceEvents = context.getSentSourceEvent().get(readerId);
        assertThat(sourceEvents)
                .as("source events should have been sent to reader %s", readerId)
                .isNotNull();
        return sourceEvents.stream()
                .filter(RequestRetainedSplitOffsetsEvent.class::isInstance)
                .map(RequestRetainedSplitOffsetsEvent.class::cast)
                .reduce((first, second) -> second)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        String.format(
                                                "retained split offset request was not sent to reader %s",
                                                readerId)));
    }

    @SuppressWarnings("unchecked")
    static MetadataUpdateEvent getLatestMetadataUpdateEventWithoutContextSync(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context, int readerId) {
        Map<Integer, List<SourceEvent>> sentSourceEvents =
                (Map<Integer, List<SourceEvent>>)
                        Whitebox.getInternalState(context, "sentSourceEvent");
        List<SourceEvent> sourceEvents = sentSourceEvents.get(readerId);
        assertThat(sourceEvents)
                .as("reader %s should have received source events", readerId)
                .isNotNull();
        return sourceEvents.stream()
                .filter(MetadataUpdateEvent.class::isInstance)
                .map(MetadataUpdateEvent.class::cast)
                .reduce((first, second) -> second)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        String.format(
                                                "reader %s did not receive metadata update event",
                                                readerId)));
    }

    static boolean hasLatestMetadataUpdateEvent(
            MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context,
            int readerId,
            KafkaStream expectedKafkaStream) {
        try {
            return getLatestMetadataUpdateEventWithoutContextSync(context, readerId)
                    .getKafkaStreams()
                    .equals(Collections.singleton(expectedKafkaStream));
        } catch (AssertionError e) {
            return false;
        }
    }
}
