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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the split ids of {@link DynamicKafkaSourceReader} keep the split-local outputs of
 * different partitions apart.
 */
class DynamicKafkaSourceReaderSplitOutputTest {
    private static final String SHORT_CLUSTER_ID = "a";
    private static final String LONG_CLUSTER_ID = "a-b";
    // The two partitions format the same split id when the cluster id and the partition id are
    // joined with '-': a + b-c-0 and a-b + c-0.
    private static final String DASHED_TOPIC = "b-c";
    private static final String PLAIN_TOPIC = "c";

    private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    @Test
    void testAmbiguousClusterAndTopicIdsDoNotShareASplitOutput() throws Exception {
        DynamicKafkaSourceSplit finishingSplit = split(SHORT_CLUSTER_ID, DASHED_TOPIC);
        DynamicKafkaSourceSplit readingSplit = split(LONG_CLUSTER_ID, PLAIN_TOPIC);

        try (DynamicKafkaSourceReader<Integer> reader = createReader()) {
            registerSubReader(
                    reader,
                    SHORT_CLUSTER_ID,
                    new FakeSplitReader(records(finishingSplit, 100L, true)));
            registerSubReader(
                    reader,
                    LONG_CLUSTER_ID,
                    new FakeSplitReader(
                            records(readingSplit, 50L, false), records(readingSplit, 60L, false)));
            setIsActivelyConsumingSplits(reader);

            MultiplexingReaderOutput readerOutput = new MultiplexingReaderOutput();
            // The source operator creates an output for every split it assigns to the reader.
            readerOutput.createOutputForSplit(finishingSplit.splitId());
            readerOutput.createOutputForSplit(readingSplit.splitId());

            reader.addSplits(Arrays.asList(finishingSplit, readingSplit));
            pollUntil(
                    reader,
                    readerOutput,
                    () ->
                            !readerOutput.releasedSplitIds().isEmpty()
                                    && readerOutput.watermarksOf(readingSplit.splitId()).size()
                                            >= 2);

            assertThat(readerOutput.outputFor(finishingSplit.splitId()))
                    .as("each physical partition needs its own split-local output")
                    .isNotSameAs(readerOutput.outputFor(readingSplit.splitId()));
            assertThat(readerOutput.releasedSplitIds())
                    .as("finishing a split must not release the output another split writes to")
                    .doesNotContain(readingSplit.splitId());
            assertThat(readerOutput.watermarksOf(finishingSplit.splitId())).containsExactly(100L);
            assertThat(readerOutput.watermarksOf(readingSplit.splitId()))
                    .as("the partitions keep their own split-local watermarks")
                    .containsExactly(50L, 60L);
            assertThat(readerOutput.combinedWatermarks())
                    .as("the reading partition still holds back the combined watermark")
                    .endsWith(60L);
        }
    }

    private static void pollUntil(
            DynamicKafkaSourceReader<Integer> reader,
            ReaderOutput<Integer> readerOutput,
            BooleanSupplier condition)
            throws Exception {
        final long deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            reader.pollNext(readerOutput);
            // The records are handed over by the split fetcher thread of the sub readers.
            Thread.sleep(1);
        }
    }

    private static DynamicKafkaSourceSplit split(String clusterId, String topic) {
        return new DynamicKafkaSourceSplit(
                clusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, 0), 0L, KafkaPartitionSplit.NO_STOPPING_OFFSET));
    }

    private static RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> records(
            DynamicKafkaSourceSplit split, long watermark, boolean finished) {
        RecordsBySplits.Builder<ConsumerRecord<byte[], byte[]>> builder =
                new RecordsBySplits.Builder<>();
        builder.add(
                split.splitId(),
                new ConsumerRecord<>(
                        split.getTopic(),
                        split.getPartition(),
                        0L,
                        watermark,
                        TimestampType.CREATE_TIME,
                        0,
                        0,
                        null,
                        new byte[0],
                        new RecordHeaders(),
                        Optional.empty()));
        if (finished) {
            builder.addFinishedSplit(split.splitId());
        }
        return builder.build();
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

    @SuppressWarnings("unchecked")
    private static void registerSubReader(
            DynamicKafkaSourceReader<Integer> reader,
            String kafkaClusterId,
            FakeSplitReader splitReader)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        KafkaSourceReader<Integer> subReader =
                new KafkaSourceReader<>(
                        elementsQueue,
                        new KafkaSourceFetcherManager(
                                elementsQueue, () -> splitReader, ignore -> {}),
                        watermarkEmitter(),
                        new Configuration(),
                        new TestingReaderContext(),
                        new KafkaSourceReaderMetrics(
                                UnregisteredMetricsGroup.createSourceReaderMetricGroup()));

        Field clusterReaderMapField =
                DynamicKafkaSourceReader.class.getDeclaredField("clusterReaderMap");
        clusterReaderMapField.setAccessible(true);
        NavigableMap<String, KafkaSourceReader<Integer>> clusterReaderMap =
                (NavigableMap<String, KafkaSourceReader<Integer>>)
                        clusterReaderMapField.get(reader);
        clusterReaderMap.put(kafkaClusterId, subReader);
    }

    private static void setIsActivelyConsumingSplits(DynamicKafkaSourceReader<Integer> reader)
            throws Exception {
        Field isActivelyConsumingSplitsField =
                DynamicKafkaSourceReader.class.getDeclaredField("isActivelyConsumingSplits");
        isActivelyConsumingSplitsField.setAccessible(true);
        isActivelyConsumingSplitsField.set(reader, true);
    }

    /** Emits one watermark per record, so records carry the watermark of their partition. */
    private static RecordEmitter<ConsumerRecord<byte[], byte[]>, Integer, KafkaPartitionSplitState>
            watermarkEmitter() {
        return (record, output, splitState) ->
                output.emitWatermark(new Watermark(record.timestamp()));
    }

    private static final class FakeSplitReader
            implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
        private final Queue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> batches;

        @SafeVarargs
        private FakeSplitReader(RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>... batches) {
            this.batches = new ConcurrentLinkedQueue<>(Arrays.asList(batches));
        }

        @Override
        public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() {
            RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> batch = batches.poll();
            if (batch != null) {
                return batch;
            }
            return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
        }

        @Override
        public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChange) {}

        @Override
        public void wakeUp() {}

        @Override
        public void close() {}
    }

    /**
     * Models the split output handling of the source operator: one split-local output per split id,
     * combined through a {@link WatermarkOutputMultiplexer} so that the emitted combined watermarks
     * reveal which partitions hold the combined watermark back.
     */
    private static final class MultiplexingReaderOutput implements ReaderOutput<Integer> {
        private final List<Long> combinedWatermarks = new ArrayList<>();
        private final WatermarkOutputMultiplexer watermarkMultiplexer =
                new WatermarkOutputMultiplexer(
                        new WatermarkOutput() {
                            @Override
                            public void emitWatermark(Watermark watermark) {
                                combinedWatermarks.add(watermark.getTimestamp());
                            }

                            @Override
                            public void markIdle() {}

                            @Override
                            public void markActive() {}
                        });
        private final Map<String, RecordingSplitOutput> createdSplitOutputs = new HashMap<>();
        private final Map<String, RecordingSplitOutput> registeredSplitOutputs = new HashMap<>();
        private final List<String> releasedSplitIds = new ArrayList<>();

        @Override
        public SourceOutput<Integer> createOutputForSplit(String splitId) {
            RecordingSplitOutput splitOutput = registeredSplitOutputs.get(splitId);
            if (splitOutput == null) {
                watermarkMultiplexer.registerNewOutput(splitId);
                splitOutput =
                        new RecordingSplitOutput(watermarkMultiplexer.getImmediateOutput(splitId));
                registeredSplitOutputs.put(splitId, splitOutput);
                createdSplitOutputs.putIfAbsent(splitId, splitOutput);
            }
            return splitOutput;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            releasedSplitIds.add(splitId);
            registeredSplitOutputs.remove(splitId);
            if (watermarkMultiplexer.unregisterOutput(splitId)) {
                // The operator emits periodically, after the split output is unregistered.
                watermarkMultiplexer.onPeriodicEmit();
            }
        }

        @Override
        public void collect(Integer record) {}

        @Override
        public void collect(Integer record, long timestamp) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        private RecordingSplitOutput outputFor(String splitId) {
            return createdSplitOutputs.get(splitId);
        }

        private List<String> releasedSplitIds() {
            return releasedSplitIds;
        }

        private List<Long> watermarksOf(String splitId) {
            return createdSplitOutputs.get(splitId).watermarks();
        }

        private List<Long> combinedWatermarks() {
            return combinedWatermarks;
        }
    }

    private static final class RecordingSplitOutput implements SourceOutput<Integer> {
        private final WatermarkOutput watermarkOutput;
        private final List<Long> watermarks = new ArrayList<>();

        private RecordingSplitOutput(WatermarkOutput watermarkOutput) {
            this.watermarkOutput = watermarkOutput;
        }

        @Override
        public void collect(Integer record) {}

        @Override
        public void collect(Integer record, long timestamp) {}

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarks.add(watermark.getTimestamp());
            watermarkOutput.emitWatermark(watermark);
        }

        @Override
        public void markIdle() {
            watermarkOutput.markIdle();
        }

        @Override
        public void markActive() {
            watermarkOutput.markActive();
        }

        private List<Long> watermarks() {
            return watermarks;
        }
    }
}
