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

import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.SplitAndAssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus.ASSIGNED;
import static org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus.UNASSIGNED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test {@link
 * org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumStateSerializer}.
 */
public class DynamicKafkaSourceEnumStateSerializerTest {

    @Test
    public void testSerde() throws Exception {
        DynamicKafkaSourceEnumStateSerializer dynamicKafkaSourceEnumStateSerializer =
                new DynamicKafkaSourceEnumStateSerializer();

        Properties propertiesForCluster0 = new Properties();
        propertiesForCluster0.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "cluster0:9092");
        Properties propertiesForCluster1 = new Properties();
        propertiesForCluster1.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "cluster1:9092");

        OffsetsInitializer cluster0StartingOffsetsInitializer =
                new TestingOffsetsInitializer("cluster0-start", OffsetResetStrategy.EARLIEST);
        OffsetsInitializer cluster0StoppingOffsetsInitializer =
                new TestingOffsetsInitializer("cluster0-stop", OffsetResetStrategy.LATEST);

        Set<KafkaStream> kafkaStreams =
                ImmutableSet.of(
                        new KafkaStream(
                                "stream0",
                                ImmutableMap.of(
                                        "cluster0",
                                        new ClusterMetadata(
                                                ImmutableSet.of("topic0", "topic1"),
                                                propertiesForCluster0,
                                                cluster0StartingOffsetsInitializer,
                                                cluster0StoppingOffsetsInitializer),
                                        "cluster1",
                                        new ClusterMetadata(
                                                ImmutableSet.of("topic2", "topic3"),
                                                propertiesForCluster1))),
                        new KafkaStream(
                                "stream1",
                                ImmutableMap.of(
                                        "cluster1",
                                        new ClusterMetadata(
                                                ImmutableSet.of("topic4", "topic5"),
                                                propertiesForCluster1))));

        DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState =
                new DynamicKafkaSourceEnumState(
                        kafkaStreams,
                        ImmutableMap.of(
                                "cluster0",
                                new KafkaSourceEnumState(
                                        ImmutableSet.of(
                                                getSplitAssignment("topic0", 0, ASSIGNED),
                                                getSplitAssignment("topic1", 1, UNASSIGNED)),
                                        true),
                                "cluster1",
                                new KafkaSourceEnumState(
                                        ImmutableSet.of(
                                                getSplitAssignment("topic2", 0, UNASSIGNED),
                                                getSplitAssignment("topic3", 1, UNASSIGNED),
                                                getSplitAssignment("topic4", 2, UNASSIGNED),
                                                getSplitAssignment("topic5", 3, UNASSIGNED)),
                                        false)));

        DynamicKafkaSourceEnumState dynamicKafkaSourceEnumStateAfterSerde =
                dynamicKafkaSourceEnumStateSerializer.deserialize(
                        dynamicKafkaSourceEnumStateSerializer.getVersion(),
                        dynamicKafkaSourceEnumStateSerializer.serialize(
                                dynamicKafkaSourceEnumState));

        assertThat(dynamicKafkaSourceEnumState)
                .usingRecursiveComparison()
                .isEqualTo(dynamicKafkaSourceEnumStateAfterSerde);
    }

    private static SplitAndAssignmentStatus getSplitAssignment(
            String topic, int partition, AssignmentStatus assignStatus) {
        return new SplitAndAssignmentStatus(
                new KafkaPartitionSplit(new TopicPartition(topic, partition), 0), assignStatus);
    }

    private static final class TestingOffsetsInitializer implements OffsetsInitializer {
        private static final long serialVersionUID = 1L;
        private final String id;
        private final OffsetResetStrategy offsetResetStrategy;

        private TestingOffsetsInitializer(String id, OffsetResetStrategy offsetResetStrategy) {
            this.id = id;
            this.offsetResetStrategy = offsetResetStrategy;
        }

        @Override
        public Map<TopicPartition, Long> getPartitionOffsets(
                Collection<TopicPartition> partitions,
                OffsetsInitializer.PartitionOffsetsRetriever partitionOffsetsRetriever) {
            return Collections.emptyMap();
        }

        @Override
        public OffsetResetStrategy getAutoOffsetResetStrategy() {
            return offsetResetStrategy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingOffsetsInitializer that = (TestingOffsetsInitializer) o;
            return Objects.equals(id, that.id)
                    && Objects.equals(offsetResetStrategy, that.offsetResetStrategy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, offsetResetStrategy);
        }
    }
}
