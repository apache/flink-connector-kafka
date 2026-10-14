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

package org.apache.flink.connector.kafka.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.reader.KafkaPartitionSplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The SplitFetcherManager for Kafka source. This class is needed to help commit the offsets to
 * Kafka using the KafkaConsumer inside the {@link
 * org.apache.flink.connector.kafka.source.reader.KafkaPartitionSplitReader}.
 */
@Internal
public class KafkaSourceFetcherManager
        extends SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceFetcherManager.class);

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     *     system.
     * @param splitFinishedHook Hook for handling finished splits in split fetchers.
     */
    public KafkaSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                    elementsQueue,
            Supplier<SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit>>
                    splitReaderSupplier,
            Consumer<Collection<String>> splitFinishedHook) {
        super(splitReaderSupplier, new Configuration(), splitFinishedHook);
    }

    public void commitOffsets(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, OffsetCommitCallback callback) {
        LOG.debug("Committing offsets {}", offsetsToCommit);
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        SplitFetcher<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> splitFetcher =
                fetchers.get(0);
        if (splitFetcher != null) {
            // The fetcher thread is still running. This should be the majority of the cases.
            enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
        } else {
            splitFetcher = createSplitFetcher();
            enqueueOffsetsCommitTask(splitFetcher, offsetsToCommit, callback);
            startFetcher(splitFetcher);
        }
    }

    /**
     * Commits the given offsets synchronously when the fetcher closes its consumer, after {@link
     * #close(long)} shut the fetcher down. Must be called before {@link #close(long)}.
     */
    public void commitOffsetsOnClose(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        // A fetcher that is shutting down stays listed until it exits, so try each of them.
        for (SplitFetcher<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> fetcher :
                fetchers.values()) {
            if (((KafkaPartitionSplitReader) fetcher.getSplitReader())
                    .commitOffsetsOnClose(offsetsToCommit)) {
                return;
            }
        }
        // No fetcher runs, or all are already closing their consumers. Only a fetcher thread may
        // use a consumer, so start one that commits as it closes.
        final SplitFetcher<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> splitFetcher =
                createSplitFetcher();
        ((KafkaPartitionSplitReader) splitFetcher.getSplitReader())
                .commitOffsetsOnClose(offsetsToCommit);
        startFetcher(splitFetcher);
    }

    private void enqueueOffsetsCommitTask(
            SplitFetcher<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> splitFetcher,
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
            OffsetCommitCallback callback) {
        KafkaPartitionSplitReader kafkaReader =
                (KafkaPartitionSplitReader) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() throws IOException {
                        kafkaReader.notifyCheckpointComplete(offsetsToCommit, callback);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
