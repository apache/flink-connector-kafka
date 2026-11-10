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

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaPartitionSplitReaderWrapper}. */
class KafkaPartitionSplitReaderWrapperTest {

    @Test
    void testSplitIdsMatchTheIdsOfTheDynamicSplits() {
        DynamicKafkaSourceSplit shortClusterIdSplit = split("a", "b-c", 0);
        DynamicKafkaSourceSplit longClusterIdSplit = split("a-b", "c", 0);

        String shortClusterIdSplitId = wrap(shortClusterIdSplit).nextSplit();
        String longClusterIdSplitId = wrap(longClusterIdSplit).nextSplit();

        // The wrapper reformats the ids of the underlying split reader, so the ids it emits have to
        // be the ids the split reader assigned the splits under.
        assertThat(shortClusterIdSplitId).isEqualTo(shortClusterIdSplit.splitId());
        assertThat(longClusterIdSplitId).isEqualTo(longClusterIdSplit.splitId());
        assertThat(shortClusterIdSplitId).isNotEqualTo(longClusterIdSplitId);
        assertThat(wrap(shortClusterIdSplit).finishedSplits())
                .containsExactly(shortClusterIdSplit.splitId());
    }

    private static RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> wrap(
            DynamicKafkaSourceSplit split) {
        RecordsBySplits.Builder<ConsumerRecord<byte[], byte[]>> builder =
                new RecordsBySplits.Builder<>();
        String partitionSplitId = KafkaPartitionSplit.toSplitId(split.getTopicPartition());
        builder.add(partitionSplitId, record(split));
        builder.addFinishedSplit(partitionSplitId);
        return new KafkaPartitionSplitReaderWrapper.WrappedRecordsWithSplitIds(
                builder.build(), split.getKafkaClusterId());
    }

    private static DynamicKafkaSourceSplit split(String clusterId, String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                clusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, partition),
                        0L,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET));
    }

    private static ConsumerRecord<byte[], byte[]> record(DynamicKafkaSourceSplit split) {
        return new ConsumerRecord<>(
                split.getTopic(),
                split.getPartition(),
                0L,
                0L,
                TimestampType.CREATE_TIME,
                0,
                0,
                null,
                new byte[0],
                new RecordHeaders(),
                Optional.empty());
    }
}
