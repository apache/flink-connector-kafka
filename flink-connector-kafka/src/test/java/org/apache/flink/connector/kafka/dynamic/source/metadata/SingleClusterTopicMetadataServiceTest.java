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

package org.apache.flink.connector.kafka.dynamic.source.metadata;

import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.metadata.SingleClusterTopicMetadataService;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class SingleClusterTopicMetadataServiceTest {

    private static final String TOPIC0 = "SingleClusterTopicMetadataServiceTest-1";
    private static final String TOPIC1 = "SingleClusterTopicMetadataServiceTest-2";

    private static KafkaMetadataService kafkaMetadataService;
    private static KafkaTestBase.KafkaClusterTestEnvMetadata kafkaClusterTestEnvMetadata0;

    @BeforeAll
    static void beforeAll() throws Throwable {
        DynamicKafkaSourceTestHelper.setup();
        DynamicKafkaSourceTestHelper.createTopic(TOPIC0, 3);
        DynamicKafkaSourceTestHelper.createTopic(TOPIC1, 3);

        kafkaClusterTestEnvMetadata0 =
                DynamicKafkaSourceTestHelper.getKafkaClusterTestEnvMetadata(0);

        kafkaMetadataService =
                new SingleClusterTopicMetadataService(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                        kafkaClusterTestEnvMetadata0.getStandardProperties());
    }

    @AfterAll
    static void afterAll() throws Exception {
        DynamicKafkaSourceTestHelper.tearDown();
    }

    @Test
    void getAllStreams() {
        Set<KafkaStream> allStreams = kafkaMetadataService.getAllStreams();
        assertThat(allStreams)
                .as("stream names should be equal to topic names")
                .containsExactlyInAnyOrder(
                        new KafkaStream(
                                TOPIC0,
                                ImmutableMap.of(
                                        kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                                        new ClusterMetadata(
                                                Collections.singleton(TOPIC0),
                                                kafkaClusterTestEnvMetadata0
                                                        .getStandardProperties()))),
                        new KafkaStream(
                                TOPIC1,
                                Collections.singletonMap(
                                        kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                                        new ClusterMetadata(
                                                Collections.singleton(TOPIC1),
                                                kafkaClusterTestEnvMetadata0
                                                        .getStandardProperties()))));
    }

    @Test
    void describeStreams() {
        Map<String, KafkaStream> streamMap =
                kafkaMetadataService.describeStreams(Collections.singleton(TOPIC1));
        assertThat(streamMap)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                TOPIC1,
                                new KafkaStream(
                                        TOPIC1,
                                        Collections.singletonMap(
                                                kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                                                new ClusterMetadata(
                                                        Collections.singleton(TOPIC1),
                                                        kafkaClusterTestEnvMetadata0
                                                                .getStandardProperties())))));

        assertThatCode(
                        () ->
                                kafkaMetadataService.describeStreams(
                                        Collections.singleton("unknown-stream")))
                .as("the stream topic cannot be found in kafka and we rethrow")
                .hasRootCauseInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void describeStreamsIncludesOffsetsInitializers() throws Exception {
        OffsetsInitializer startingOffsetsInitializer =
                new TestingOffsetsInitializer("start", OffsetResetStrategy.EARLIEST);
        OffsetsInitializer stoppingOffsetsInitializer =
                new TestingOffsetsInitializer("stop", OffsetResetStrategy.LATEST);

        KafkaMetadataService metadataService =
                new SingleClusterTopicMetadataService(
                        kafkaClusterTestEnvMetadata0.getKafkaClusterId(),
                        kafkaClusterTestEnvMetadata0.getStandardProperties(),
                        startingOffsetsInitializer,
                        stoppingOffsetsInitializer);

        try {
            Map<String, KafkaStream> streamMap =
                    metadataService.describeStreams(Collections.singleton(TOPIC0));
            ClusterMetadata clusterMetadata =
                    streamMap
                            .get(TOPIC0)
                            .getClusterMetadataMap()
                            .get(kafkaClusterTestEnvMetadata0.getKafkaClusterId());

            assertThat(clusterMetadata.getStartingOffsetsInitializer())
                    .isSameAs(startingOffsetsInitializer);
            assertThat(clusterMetadata.getStoppingOffsetsInitializer())
                    .isSameAs(stoppingOffsetsInitializer);
        } finally {
            metadataService.close();
        }
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
        public Map<org.apache.kafka.common.TopicPartition, Long> getPartitionOffsets(
                Collection<org.apache.kafka.common.TopicPartition> partitions,
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
