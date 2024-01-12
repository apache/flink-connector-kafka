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
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
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
}
