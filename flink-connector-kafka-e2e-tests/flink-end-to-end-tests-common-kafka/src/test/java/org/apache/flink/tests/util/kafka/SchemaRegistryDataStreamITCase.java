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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.FileUtils;
import org.apache.flink.test.util.JobSubmission;
import org.apache.flink.tests.util.kafka.containers.SchemaRegistryContainer;

import example.avro.EventType;
import example.avro.User;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the DataStream Confluent Schema Registry Avro schemas with {@code
 * KafkaSource} and {@code KafkaSink}.
 */
@Testcontainers
class SchemaRegistryDataStreamITCase {

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final String INTER_CONTAINER_REGISTRY_ALIAS = "registry";
    private static final Network NETWORK = Network.newNetwork();

    private static final List<User> USERS =
            List.of(
                    user("Alyssa", "250", "green"),
                    user("Charlie", "10", "blue"),
                    user("Ben", "7", "red"));

    @Container
    private static final TestKafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(SchemaRegistryDataStreamITCase.class)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Container
    private static final SchemaRegistryContainer REGISTRY =
            new SchemaRegistryContainer(DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY))
                    .withKafka(INTER_CONTAINER_KAFKA_ALIAS + ":9093")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_REGISTRY_ALIAS)
                    .dependsOn(KAFKA_CONTAINER.getContainer());

    @RegisterExtension
    private static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.basedOn(getConfiguration()))
                    .withTestcontainersSettings(
                            TestcontainersSettings.builder()
                                    .network(NETWORK)
                                    .logger(
                                            KafkaUtil.getLogger(
                                                    "flink", SchemaRegistryDataStreamITCase.class))
                                    .dependsOn(REGISTRY)
                                    .build())
                    .build();

    private static Configuration getConfiguration() {
        final Configuration config = new Configuration();
        // fail fast instead of restarting when a record cannot be (de)serialized
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        // Workaround for FLINK-36454: FlinkContainers replaces the distribution's config.yaml,
        // which drops the JVM options that bin/flink needs on Java 17
        config.set(
                CoreOptions.FLINK_JVM_OPTIONS,
                GlobalConfiguration.loadConfiguration(
                                FileUtils.findFlinkDist().resolve("conf").toString())
                        .get(CoreOptions.FLINK_JVM_OPTIONS));
        return config;
    }

    @Test
    void testReadAndWriteWithSchemaRegistry() throws Exception {
        final String inputTopic = "test-avro-input-" + UUID.randomUUID();
        final String stringTopic = "test-string-output-" + UUID.randomUUID();
        final String avroTopic = "test-avro-output-" + UUID.randomUUID();
        final String outputSubject = avroTopic + "-value";

        final KafkaContainerClient kafkaClient = new KafkaContainerClient(KAFKA_CONTAINER);
        kafkaClient.createTopic(1, 1, inputTopic);
        kafkaClient.createTopic(1, 1, stringTopic);
        kafkaClient.createTopic(1, 1, avroTopic);

        final SchemaRegistryClient registryClient =
                new CachedSchemaRegistryClient(REGISTRY.getSchemaRegistryUrl(), 10);
        final Map<String, Object> serdeConfig =
                Map.of(
                        "schema.registry.url",
                        REGISTRY.getSchemaRegistryUrl(),
                        "specific.avro.reader",
                        true);
        kafkaClient.sendMessages(
                inputTopic, new KafkaAvroSerializer(registryClient, serdeConfig), USERS.toArray());

        final JobID jobId =
                FLINK.submitJob(
                        new JobSubmission.JobSubmissionBuilder(
                                        ResourceTestUtils.getResource(
                                                ".*/confluent-schema-registry-test\\.jar"))
                                .setDetached(true)
                                .addArgument("--input-topic", inputTopic)
                                .addArgument("--output-string-topic", stringTopic)
                                .addArgument("--output-avro-topic", avroTopic)
                                .addArgument("--output-subject", outputSubject)
                                .addArgument(
                                        "--bootstrap.servers",
                                        INTER_CONTAINER_KAFKA_ALIAS + ":9093")
                                .addArgument("--group.id", "confluent-schema-registry-test")
                                .addArgument(
                                        "--schema-registry-url",
                                        "http://" + INTER_CONTAINER_REGISTRY_ALIAS + ":8082")
                                .build());
        final JobResult result = FLINK.getRestClusterClient().requestJobResult(jobId).get();
        if (!result.isSuccess()) {
            throw new AssertionError(
                    "Job " + jobId + " did not succeed",
                    result.getSerializedThrowable().orElse(null));
        }

        assertThat(
                        kafkaClient.readMessages(
                                3, "string-reader", stringTopic, new StringDeserializer()))
                .containsExactlyInAnyOrderElementsOf(
                        USERS.stream().map(User::toString).collect(Collectors.toList()));
        assertThat(
                        kafkaClient.readMessages(
                                3,
                                "avro-reader",
                                avroTopic,
                                new KafkaAvroDeserializer(registryClient, serdeConfig)))
                .containsExactlyInAnyOrderElementsOf(USERS);
        assertThat(registryClient.getAllVersions(outputSubject)).hasSize(1);
    }

    private static User user(String name, String favoriteNumber, String favoriteColor) {
        return User.newBuilder()
                .setName(name)
                .setFavoriteNumber(favoriteNumber)
                .setFavoriteColor(favoriteColor)
                .setEventType(EventType.meeting)
                .build();
    }
}
