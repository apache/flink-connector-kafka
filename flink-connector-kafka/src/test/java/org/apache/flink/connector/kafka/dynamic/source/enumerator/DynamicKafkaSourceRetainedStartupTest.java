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
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.RequestRetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.RetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumeratorTestUtils.getLatestMetadataUpdateEvent;
import static org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumeratorTestUtils.getLatestRetainedSplitOffsetRequest;
import static org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumeratorTestUtils.retainedSplitOffsetHandoffProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Failed cluster startup must not discard the sole retained copy of reader progress. */
class DynamicKafkaSourceRetainedStartupTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInactiveClusterStartupFailureIsFatalOnlyWhenProgressExists(boolean restored)
            throws Throwable {
        String cluster = "cluster";
        DynamicKafkaSourceSplit progress =
                new DynamicKafkaSourceSplit(
                        cluster, new KafkaPartitionSplit(new TopicPartition("topic", 0), 100));
        Set<KafkaStream> initialStreams = streams(cluster, "localhost:9092");
        DynamicKafkaSourceEnumState checkpoint =
                restored
                        ? new DynamicKafkaSourceEnumState(
                                initialStreams,
                                Map.of(
                                        cluster,
                                        new KafkaSourceEnumState(
                                                List.of(progress.getKafkaPartitionSplit()),
                                                List.of(),
                                                true)))
                        : new DynamicKafkaSourceEnumState();
        Properties properties = retainedSplitOffsetHandoffProperties(1000);
        // This invalid AdminClient-only property fails startup without requiring a reader failure.
        properties.setProperty(AdminClientConfig.RETRIES_CONFIG, "-1");
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(1);
                MockKafkaMetadataService metadata =
                        new MockKafkaMetadataService(initialStreams) {
                            @Override
                            public boolean isClusterActive(String kafkaClusterId) {
                                // The independent liveness lookup can observe removal after
                                // discovery.
                                return false;
                            }
                        };
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Set.of("stream")),
                                metadata,
                                context,
                                OffsetsInitializer.earliest(),
                                new NoStoppingOffsetsInitializer(),
                                properties,
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                checkpoint,
                                (enumContext, kafkaCluster, service, callback) ->
                                        new NoDiscoveryContext(
                                                kafkaCluster, service, enumContext, callback))) {
            enumerator.start();
            context.registerReader(
                    ReaderInfo.createReaderInfo(
                            0, "reader", restored ? List.of(progress) : List.of()));
            enumerator.addReader(0);

            if (restored) {
                assertThatThrownBy(() -> context.runPeriodicCallable(0))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessageContaining("Failed to create enumerator");
                assertThat(enumerator.snapshotState(-1).getClusterEnumeratorStates())
                        .as("known progress must survive startup failure for checkpoint recovery")
                        .containsKey(cluster);
            } else {
                context.runPeriodicCallable(0);
                assertThat(enumerator.snapshotState(-1).getClusterEnumeratorStates()).isEmpty();
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            }
        }
    }

    @Test
    void testHandoffStartFailurePreservesOffsetsWhenMetadataAlreadyRemovedCluster()
            throws Throwable {
        String cluster = "cluster";
        DynamicKafkaSourceSplit progress =
                new DynamicKafkaSourceSplit(
                        cluster, new KafkaPartitionSplit(new TopicPartition("topic", 0), 100));
        Set<KafkaStream> initialStreams = streams(cluster, "localhost:9092");
        DynamicKafkaSourceEnumState checkpoint =
                new DynamicKafkaSourceEnumState(
                        initialStreams,
                        Map.of(
                                cluster,
                                new KafkaSourceEnumState(
                                        List.of(progress.getKafkaPartitionSplit()),
                                        List.of(),
                                        true)));
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> context =
                        new MockSplitEnumeratorContext<>(1);
                MockKafkaMetadataService metadata = new MockKafkaMetadataService(initialStreams);
                DynamicKafkaSourceEnumerator enumerator =
                        new DynamicKafkaSourceEnumerator(
                                new KafkaStreamSetSubscriber(Set.of("stream")),
                                metadata,
                                context,
                                OffsetsInitializer.earliest(),
                                new NoStoppingOffsetsInitializer(),
                                retainedSplitOffsetHandoffProperties(1000),
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                checkpoint,
                                (enumContext, kafkaCluster, service, callback) ->
                                        new NoDiscoveryContext(
                                                kafkaCluster, service, enumContext, callback))) {
            enumerator.start();
            context.registerReader(ReaderInfo.createReaderInfo(0, "reader", List.of(progress)));
            enumerator.addReader(0);
            context.runPeriodicCallable(0);
            metadata.setKafkaStreams(Set.of());
            context.runPeriodicCallable(0);

            // Empty bootstrap configuration deterministically makes AdminClient creation fail.
            // Discovery sees a return, but the metadata service removes it again before commit.
            metadata.setKafkaStreams(streams(cluster, ""));
            context.runPeriodicCallable(0);
            RequestRetainedSplitOffsetsEvent request =
                    getLatestRetainedSplitOffsetRequest(context, 0);
            enumerator.handleSourceEvent(
                    0,
                    new RetainedSplitOffsetsEvent(
                            request.getHandoffId(), cluster, Map.of(progress.splitId(), 100L)));
            enumerator.snapshotState(10);
            int assignmentsBeforeCommit = context.getSplitsAssignmentSequence().size();
            metadata.setKafkaStreams(Set.of());

            assertThatThrownBy(() -> enumerator.notifyCheckpointComplete(10))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to create enumerator");
            assertThat(context.getSplitsAssignmentSequence()).hasSize(assignmentsBeforeCommit);
            assertThat(enumerator.snapshotState(-1).getRetainedClusterEnumeratorStates())
                    .containsKey(cluster);
            assertThat(getLatestMetadataUpdateEvent(context, 0).getRetainedClusterDeadlines())
                    .as("failed startup cannot authorize readers to delete retained progress")
                    .containsKey(cluster);
        }
    }

    private static Set<KafkaStream> streams(String cluster, String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Set.of(
                new KafkaStream(
                        "stream",
                        Map.of(cluster, new ClusterMetadata(Set.of("topic"), properties))));
    }

    private static class NoDiscoveryContext extends StoppableKafkaEnumContextProxy {
        private NoDiscoveryContext(
                String cluster,
                KafkaMetadataService metadata,
                SplitEnumeratorContext<DynamicKafkaSourceSplit> context,
                Runnable callback) {
            super(cluster, metadata, context, callback);
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
