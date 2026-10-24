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

package org.apache.flink.connector.kafka.dynamic.source.split;

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamicKafkaSourceSplit}. */
class DynamicKafkaSourceSplitTest {

    @Test
    void testSplitIdDoesNotCollideWhenClusterIdAndTopicShareTheSeparator() {
        DynamicKafkaSourceSplit shortClusterIdSplit = split("a", "b-c", 0);
        DynamicKafkaSourceSplit longClusterIdSplit = split("a-b", "c", 0);

        assertThat(shortClusterIdSplit.splitId()).isNotEqualTo(longClusterIdSplit.splitId());
    }

    @Test
    void testSplitIdIsUniquePerPhysicalPartition() {
        assertThat(split("cluster", "topic", 0).splitId())
                .isNotEqualTo(split("cluster", "topic", 1).splitId())
                .isNotEqualTo(split("cluster", "other-topic", 0).splitId())
                .isNotEqualTo(split("other-cluster", "topic", 0).splitId());
    }

    @Test
    void testSplitIdIsStableForTheSamePhysicalPartition() {
        assertThat(split("cluster", "topic", 0).splitId())
                .isEqualTo(split("cluster", "topic", 0).splitId())
                .isEqualTo(
                        DynamicKafkaSourceSplit.toSplitId(
                                "cluster",
                                KafkaPartitionSplit.toSplitId(new TopicPartition("topic", 0))));
    }

    private static DynamicKafkaSourceSplit split(String clusterId, String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                clusterId,
                new KafkaPartitionSplit(
                        new TopicPartition(topic, partition),
                        0L,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET));
    }
}
