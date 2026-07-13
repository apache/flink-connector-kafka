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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class PartitionOffsetsRetrieverImplTest {

    @Test
    void filtersCommittedOffsetsToRequestedPartitionsWithNonNullOffsets() {
        TopicPartition requestedWithOffset = new TopicPartition("topic", 0);
        TopicPartition requestedWithNullOffset = new TopicPartition("topic", 1);
        TopicPartition requestedWithoutOffset = new TopicPartition("topic", 2);
        TopicPartition unrequestedWithOffset = new TopicPartition("topic", 3);

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(requestedWithOffset, new OffsetAndMetadata(42L));
        committedOffsets.put(requestedWithNullOffset, null);
        committedOffsets.put(unrequestedWithOffset, new OffsetAndMetadata(123L));

        Map<TopicPartition, Long> filteredOffsets =
                KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl.filterCommittedOffsets(
                        Arrays.asList(
                                requestedWithOffset,
                                requestedWithNullOffset,
                                requestedWithoutOffset),
                        committedOffsets);

        assertThat(filteredOffsets).containsOnly(entry(requestedWithOffset, 42L));
    }
}
