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

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for overriding the reset strategy of an {@link OffsetsInitializer}. */
class OffsetsInitializerWithOffsetResetStrategyTest {

    @Test
    void testWithOffsetResetStrategyDelegatesPartitionOffsets() {
        TopicPartition partition = new TopicPartition("topic", 0);
        Map<TopicPartition, Long> expectedOffsets =
                Collections.singletonMap(partition, KafkaPartitionSplit.EARLIEST_OFFSET);
        OffsetsInitializer initializer =
                OffsetsInitializer.withOffsetResetStrategy(
                        new TestingOffsetsInitializer(expectedOffsets), OffsetResetStrategy.NONE);

        assertThat(initializer.getPartitionOffsets(Collections.singleton(partition), null))
                .isEqualTo(expectedOffsets);
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(OffsetResetStrategy.NONE);
    }

    private static class TestingOffsetsInitializer implements OffsetsInitializer {
        private static final long serialVersionUID = 3848778106181634780L;

        private final Map<TopicPartition, Long> offsets;

        private TestingOffsetsInitializer(Map<TopicPartition, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public Map<TopicPartition, Long> getPartitionOffsets(
                Collection<TopicPartition> partitions,
                PartitionOffsetsRetriever partitionOffsetsRetriever) {
            return offsets;
        }

        @Override
        public OffsetResetStrategy getAutoOffsetResetStrategy() {
            return OffsetResetStrategy.EARLIEST;
        }
    }
}
