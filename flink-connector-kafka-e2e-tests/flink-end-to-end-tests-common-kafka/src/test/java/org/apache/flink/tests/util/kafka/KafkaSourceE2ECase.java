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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContextFactory;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;

import static org.apache.flink.connector.kafka.testutils.DockerImageVersions.KAFKA;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContext.SplitMappingMode.PARTITION;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContext.SplitMappingMode.TOPIC;

/** Kafka E2E test based on connector testing framework. */
public class KafkaSourceE2ECase extends SourceTestSuiteBase<String> {
    private static final String KAFKA_HOSTNAME = "kafka";

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    // Defines TestEnvironment
    @TestEnv FlinkContainerTestEnvironment flink = new FlinkContainerTestEnvironment(1, 6);

    TestKafkaContainer kafkaContainer =
            new TestKafkaContainer(KAFKA).withNetworkAliases(KAFKA_HOSTNAME);

    // Defines ConnectorExternalSystem
    @SuppressWarnings({"rawtypes", "unchecked"})
    @TestExternalSystem
    DefaultContainerizedExternalSystem<?> kafka =
            DefaultContainerizedExternalSystem.builder()
                    .fromContainer((GenericContainer) kafkaContainer.getContainer())
                    .bindWithFlinkContainer(flink.getFlinkContainers().getJobManager())
                    .build();

    // Defines 2 External context Factories, so test cases will be invoked twice using these two
    // kinds of external contexts.
    @SuppressWarnings("unused")
    @TestContext
    KafkaSourceExternalContextFactory singleTopic =
            new KafkaSourceExternalContextFactory(
                    kafkaContainer,
                    Arrays.asList(
                            ResourceTestUtils.getResource("kafka-connector.jar").toUri().toURL(),
                            ResourceTestUtils.getResource("kafka-clients.jar").toUri().toURL()),
                    PARTITION);

    @SuppressWarnings("unused")
    @TestContext
    KafkaSourceExternalContextFactory multipleTopic =
            new KafkaSourceExternalContextFactory(
                    kafkaContainer,
                    Arrays.asList(
                            ResourceTestUtils.getResource("kafka-connector.jar").toUri().toURL(),
                            ResourceTestUtils.getResource("kafka-clients.jar").toUri().toURL()),
                    TOPIC);

    public KafkaSourceE2ECase() throws Exception {}
}
