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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/** Fake {@link OffsetsInitializer.PartitionOffsetsRetriever} for unit tests. */
public final class MockPartitionOffsetsRetriever
        implements OffsetsInitializer.PartitionOffsetsRetriever {

    /** Fake offsets retriever for a given set of topic partitions. */
    public interface OffsetsRetriever
            extends Function<Collection<TopicPartition>, Map<TopicPartition, Long>> {}

    /**
     * Fake offsets retrieve for a given set of topic partitions and their target timestamp
     * position.
     */
    public interface TimestampOffsetsRetriever
            extends Function<Map<TopicPartition, Long>, Map<TopicPartition, OffsetAndTimestamp>> {}

    public static final OffsetsRetriever UNSUPPORTED_RETRIEVAL =
            partitions -> {
                throw new UnsupportedOperationException("The method was not supposed to be called");
            };
    private final OffsetsRetriever committedOffsets;
    private final OffsetsRetriever endOffsets;
    private final OffsetsRetriever beginningOffsets;
    private final TimestampOffsetsRetriever offsetsForTimes;

    public static MockPartitionOffsetsRetriever noInteractions() {
        return new MockPartitionOffsetsRetriever(
                UNSUPPORTED_RETRIEVAL,
                UNSUPPORTED_RETRIEVAL,
                UNSUPPORTED_RETRIEVAL,
                partitions -> {
                    throw new UnsupportedOperationException(
                            "The method was not supposed to be called");
                });
    }

    public static MockPartitionOffsetsRetriever timestampAndEnd(
            TimestampOffsetsRetriever retriever, OffsetsRetriever endOffsets) {
        return new MockPartitionOffsetsRetriever(
                UNSUPPORTED_RETRIEVAL, endOffsets, UNSUPPORTED_RETRIEVAL, retriever);
    }

    public static MockPartitionOffsetsRetriever latest(OffsetsRetriever endOffsets) {
        return new MockPartitionOffsetsRetriever(
                UNSUPPORTED_RETRIEVAL,
                endOffsets,
                UNSUPPORTED_RETRIEVAL,
                partitions -> {
                    throw new UnsupportedOperationException(
                            "The method was not supposed to be called");
                });
    }

    private MockPartitionOffsetsRetriever(
            OffsetsRetriever committedOffsets,
            OffsetsRetriever endOffsets,
            OffsetsRetriever beginningOffsets,
            TimestampOffsetsRetriever offsetsForTimes) {
        this.committedOffsets = committedOffsets;
        this.endOffsets = endOffsets;
        this.beginningOffsets = beginningOffsets;
        this.offsetsForTimes = offsetsForTimes;
    }

    @Override
    public Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions) {
        return committedOffsets.apply(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets.apply(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets.apply(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes.apply(timestampsToSearch);
    }
}
