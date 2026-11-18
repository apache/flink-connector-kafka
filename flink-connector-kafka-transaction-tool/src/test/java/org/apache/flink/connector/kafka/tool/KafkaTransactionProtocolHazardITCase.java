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

package org.apache.flink.connector.kafka.tool;

import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.KafkaUtil;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies why an older transaction's protocol cannot be inferred from current broker features. */
@Testcontainers
class KafkaTransactionProtocolHazardITCase {

    @Container
    private static final TestKafkaContainer KAFKA =
            new TestKafkaContainer(DockerImageVersions.APACHE_KAFKA);

    @Test
    void testV2CanMistakeAbortedV1TransactionForNewerCommit() throws Exception {
        final Properties properties = producerProperties();
        final String topic = UUID.randomUUID().toString();
        final String transactionalId = UUID.randomUUID().toString();
        try (Admin admin = Admin.create(properties)) {
            try {
                updateTransactionVersion(
                        admin, (short) 1, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE);
                KafkaUtil.createNewTopicAndWaitForPartitionAssignment(topic, 1, 1, properties);

                final TransactionDescription original;
                try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, 0, null, "T1"));
                    producer.flush();
                    original = describe(admin, transactionalId);
                    assertThat(original.state()).isEqualTo(TransactionState.ONGOING);
                    producer.abortTransaction();
                }
                waitForState(admin, transactionalId, TransactionState.COMPLETE_ABORT);

                try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(topic, 0, null, "T2"));
                    producer.flush();
                    producer.commitTransaction();
                }
                waitForState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);
                final TransactionDescription committed = describe(admin, transactionalId);
                assertThat(committed.producerId()).isEqualTo(original.producerId());
                assertThat(committed.producerEpoch()).isEqualTo(original.producerEpoch() + 1);

                updateTransactionVersion(admin, (short) 2, FeatureUpdate.UpgradeType.UPGRADE);
                final KafkaTransactionManager manager = new KafkaTransactionManager();
                assertThatThrownBy(
                                () ->
                                        manager.commitTransaction(
                                                KAFKA.getBootstrapServers(),
                                                transactionalId,
                                                original.producerId(),
                                                (short) original.producerEpoch()))
                        .hasMessageContaining("Failed to commit transaction");
                assertUnchanged(admin, transactionalId, committed);

                // V2 mistakes T1's epoch for a retry of T2's commit. Pin this unsafe choice to
                // explain why the tool defaults to V1 and must never infer V2 from broker features.
                assertThatCode(
                                () ->
                                        manager.commitTransaction(
                                                KAFKA.getBootstrapServers(),
                                                transactionalId,
                                                original.producerId(),
                                                (short) original.producerEpoch(),
                                                true))
                        .doesNotThrowAnyException();

                assertUnchanged(admin, transactionalId, committed);
                properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
                assertThat(
                                KafkaUtil.drainAllRecordsFromTopic(topic, properties, true).stream()
                                        .map(
                                                record ->
                                                        new String(
                                                                record.value(),
                                                                StandardCharsets.UTF_8))
                                        .collect(Collectors.toList()))
                        .containsExactly("T2");
            } finally {
                updateTransactionVersion(admin, (short) 2, FeatureUpdate.UpgradeType.UPGRADE);
            }
        }
    }

    private static Properties producerProperties() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static void updateTransactionVersion(
            Admin admin, short version, FeatureUpdate.UpgradeType upgradeType) throws Exception {
        admin.updateFeatures(
                        Map.of("transaction.version", new FeatureUpdate(version, upgradeType)),
                        new UpdateFeaturesOptions())
                .all()
                .get();
        CommonTestUtils.waitUntilCondition(
                () ->
                        admin.describeFeatures()
                                        .featureMetadata()
                                        .get()
                                        .finalizedFeatures()
                                        .get("transaction.version")
                                        .maxVersionLevel()
                                == version);
    }

    private static TransactionDescription describe(Admin admin, String transactionalId)
            throws Exception {
        return admin.describeTransactions(Collections.singletonList(transactionalId))
                .description(transactionalId)
                .get();
    }

    private static void waitForState(Admin admin, String transactionalId, TransactionState state)
            throws Exception {
        CommonTestUtils.waitUntilCondition(() -> describe(admin, transactionalId).state() == state);
    }

    private static void assertUnchanged(
            Admin admin, String transactionalId, TransactionDescription expected) throws Exception {
        final TransactionDescription actual = describe(admin, transactionalId);
        assertThat(actual.state()).isEqualTo(expected.state());
        assertThat(actual.producerId()).isEqualTo(expected.producerId());
        assertThat(actual.producerEpoch()).isEqualTo(expected.producerEpoch());
    }
}
