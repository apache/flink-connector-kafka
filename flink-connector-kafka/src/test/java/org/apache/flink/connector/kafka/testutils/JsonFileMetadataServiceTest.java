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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** A test class for {@link JsonFileMetadataService}. */
public class JsonFileMetadataServiceTest {

    @Test
    public void testParseFile() throws IOException {
        JsonFileMetadataService jsonFileMetadataService =
                new JsonFileMetadataService(
                        Resources.getResource("stream-metadata.json").getPath(), Duration.ZERO);
        Set<KafkaStream> kafkaStreams = jsonFileMetadataService.parseFile();

        Properties propertiesForCluster0 = new Properties();
        propertiesForCluster0.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap-server-0:443");
        Properties propertiesForCluster1 = new Properties();
        propertiesForCluster1.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap-server-1:443");
        Properties propertiesForCluster2 = new Properties();
        propertiesForCluster2.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap-server-2:443");

        assertThat(kafkaStreams)
                .containsExactlyInAnyOrderElementsOf(
                        ImmutableSet.of(
                                new KafkaStream(
                                        "stream0",
                                        ImmutableMap.of(
                                                "cluster0",
                                                new ClusterMetadata(
                                                        ImmutableSet.of("topic0", "topic1"),
                                                        propertiesForCluster0),
                                                "cluster1",
                                                new ClusterMetadata(
                                                        ImmutableSet.of("topic2", "topic3"),
                                                        propertiesForCluster1))),
                                new KafkaStream(
                                        "stream1",
                                        ImmutableMap.of(
                                                "cluster2",
                                                new ClusterMetadata(
                                                        ImmutableSet.of("topic4", "topic5"),
                                                        propertiesForCluster2)))));
    }
}
