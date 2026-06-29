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

package org.apache.flink.connector.kafka.source.enumerator.initializer;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SpecifiedOffsetsInitializer}. */
class SpecifiedOffsetsInitializerTest {

    @Test
    void incompleteSpecificOffsetsUsesResetStrategyWithoutCommittedOffsetLookup() {
        TopicPartition specifiedPartition = new TopicPartition("topic", 0);
        TopicPartition unspecifiedPartition = new TopicPartition("topic", 1);
        OffsetsInitializer initializer =
                OffsetsInitializer.offsets(Map.of(specifiedPartition, 111L));

        Map<TopicPartition, Long> offsets =
                initializer.getPartitionOffsets(
                        List.of(specifiedPartition, unspecifiedPartition),
                        new OffsetsInitializer.PartitionOffsetsRetriever() {
                            @Override
                            public Map<TopicPartition, Long> committedOffsets(
                                    Collection<TopicPartition> partitions) {
                                throw new AssertionError(
                                        "Committed offsets should not be used for incomplete specific offsets");
                            }

                            @Override
                            public Map<TopicPartition, Long> endOffsets(
                                    Collection<TopicPartition> partitions) {
                                throw new AssertionError("Latest offsets should not be used");
                            }

                            @Override
                            public Map<TopicPartition, Long> beginningOffsets(
                                    Collection<TopicPartition> partitions) {
                                assertThat(partitions).containsExactly(unspecifiedPartition);
                                return Map.of(unspecifiedPartition, 0L);
                            }

                            @Override
                            public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                                    Map<TopicPartition, Long> timestampsToSearch) {
                                throw new AssertionError("Timestamp offsets should not be used");
                            }
                        });

        assertThat(offsets)
                .containsEntry(specifiedPartition, 111L)
                .containsEntry(unspecifiedPartition, 0L)
                .hasSize(2);
    }
}
