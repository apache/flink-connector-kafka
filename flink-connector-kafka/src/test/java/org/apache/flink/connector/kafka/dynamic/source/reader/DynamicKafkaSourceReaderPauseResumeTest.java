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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicKafkaSourceReaderPauseResumeTest {
    private static final String TOPIC = "DynamicKafkaSourceReaderPauseResumeTest";

    @Test
    void testPauseOrResumeRoutesDynamicSplitIdsToOwningCluster() throws Exception {
        try (DynamicKafkaSourceReader<Integer> reader = createReader()) {
            RecordingKafkaSourceReader cluster0Reader = new RecordingKafkaSourceReader();
            RecordingKafkaSourceReader cluster1Reader = new RecordingKafkaSourceReader();
            registerSubReader(reader, "cluster-0", cluster0Reader);
            registerSubReader(reader, "cluster-1", cluster1Reader);

            DynamicKafkaSourceSplit cluster0Split = createSplit("cluster-0", TOPIC, 0);
            DynamicKafkaSourceSplit cluster1Split = createSplit("cluster-1", TOPIC, 1);
            reader.addSplits(List.of(cluster0Split, cluster1Split));

            reader.pauseOrResumeSplits(
                    Collections.singleton(cluster0Split.splitId()),
                    Collections.singleton(cluster1Split.splitId()));

            assertThat(cluster0Reader.getPausedSplits())
                    .containsExactly(cluster0Split.splitId());
            assertThat(cluster0Reader.getResumedSplits()).isEmpty();
            assertThat(cluster1Reader.getPausedSplits()).isEmpty();
            assertThat(cluster1Reader.getResumedSplits())
                    .containsExactly(cluster1Split.splitId());
        }
    }

    @Test
    void testPauseOrResumeRoutesOverlappingClusterIdsWithoutPrefixMatching() throws Exception {
        String shortClusterId = "a";
        String overlappingClusterId = shortClusterId + "-" + TOPIC;

        try (DynamicKafkaSourceReader<Integer> reader = createReader()) {
            RecordingKafkaSourceReader shortClusterReader = new RecordingKafkaSourceReader();
            RecordingKafkaSourceReader overlappingClusterReader = new RecordingKafkaSourceReader();
            registerSubReader(reader, shortClusterId, shortClusterReader);
            registerSubReader(reader, overlappingClusterId, overlappingClusterReader);

            DynamicKafkaSourceSplit shortClusterSplit = createSplit(shortClusterId, TOPIC, 0);
            DynamicKafkaSourceSplit overlappingClusterSplit =
                    createSplit(overlappingClusterId, TOPIC, 0);
            reader.addSplits(List.of(shortClusterSplit, overlappingClusterSplit));

            reader.pauseOrResumeSplits(
                    Collections.singleton(shortClusterSplit.splitId()),
                    Collections.singleton(overlappingClusterSplit.splitId()));

            assertThat(shortClusterReader.getPausedSplits())
                    .containsExactly(shortClusterSplit.splitId());
            assertThat(shortClusterReader.getResumedSplits()).isEmpty();
            assertThat(overlappingClusterReader.getPausedSplits()).isEmpty();
            assertThat(overlappingClusterReader.getResumedSplits())
                    .containsExactly(overlappingClusterSplit.splitId());
        }
    }

    private static DynamicKafkaSourceReader<Integer> createReader() {
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());

        return new DynamicKafkaSourceReader<>(
                new TestingReaderContext(),
                KafkaRecordDeserializationSchema.valueOnly(IntegerDeserializer.class),
                properties);
    }

    private static DynamicKafkaSourceSplit createSplit(
            String kafkaClusterId, String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                kafkaClusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, partition),
                        0L,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET));
    }

    @SuppressWarnings("unchecked")
    private static void registerSubReader(
            DynamicKafkaSourceReader<Integer> reader,
            String kafkaClusterId,
            KafkaSourceReader<Integer> subReader)
            throws Exception {
        Field clusterReaderMapField =
                DynamicKafkaSourceReader.class.getDeclaredField("clusterReaderMap");
        clusterReaderMapField.setAccessible(true);
        NavigableMap<String, KafkaSourceReader<Integer>> clusterReaderMap =
                (NavigableMap<String, KafkaSourceReader<Integer>>)
                        clusterReaderMapField.get(reader);
        clusterReaderMap.put(kafkaClusterId, subReader);
    }

    private static final class RecordingKafkaSourceReader extends KafkaSourceReader<Integer> {
        private List<String> pausedSplits = Collections.emptyList();
        private List<String> resumedSplits = Collections.emptyList();

        private RecordingKafkaSourceReader() {
            this(new FutureCompletingBlockingQueue<>());
        }

        private RecordingKafkaSourceReader(
                FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                        elementsQueue) {
            super(
                    elementsQueue,
                    new KafkaSourceFetcherManager(
                            elementsQueue, NoOpSplitReader::new, ignored -> {}),
                    noOpRecordEmitter(),
                    new Configuration(),
                    new TestingReaderContext(),
                    new KafkaSourceReaderMetrics(
                            UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
        }

        @Override
        public void pauseOrResumeSplits(
                Collection<String> splitsToPause, Collection<String> splitsToResume) {
            pausedSplits = new ArrayList<>(splitsToPause);
            resumedSplits = new ArrayList<>(splitsToResume);
        }

        private List<String> getPausedSplits() {
            return pausedSplits;
        }

        private List<String> getResumedSplits() {
            return resumedSplits;
        }

        private static RecordEmitter<
                        ConsumerRecord<byte[], byte[]>,
                        Integer,
                        KafkaPartitionSplitState>
                noOpRecordEmitter() {
            return (record, output, splitState) -> {};
        }
    }

    private static final class NoOpSplitReader
            implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
        @Override
        public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
            return null;
        }

        @Override
        public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChanges) {}

        @Override
        public void wakeUp() {}

        @Override
        public void close() {}
    }
}
