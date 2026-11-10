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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.testutils.DockerImageVersions;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.advanceEpoch;
import static org.apache.flink.connector.kafka.sink.KafkaRecoveryTestUtils.getProperties;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;

/** Committer recovery with known transaction protocols and unknown legacy protocol metadata. */
@Testcontainers
class KafkaTransactionProtocolRecoveryITCase {

    @Container
    private static final TestKafkaContainer KAFKA =
            new TestKafkaContainer(DockerImageVersions.APACHE_KAFKA)
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1");

    @AfterEach
    void check() {
        checkProducerLeak();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void testRecoverMixedProtocols(boolean alreadyCommitted) throws Exception {
        Properties properties = getProperties(KAFKA.getBootstrapServers());
        String prefix = "mixed-protocol-recovery-" + alreadyCommitted;
        List<KafkaCommittable> saved = new ArrayList<>();
        saved.add(
                prepareTransaction(
                        properties,
                        prefix + "-v2-first",
                        TransactionProtocol.V2,
                        CommittableFormat.CURRENT,
                        0,
                        alreadyCommitted));
        saved.add(
                prepareTransaction(
                        properties,
                        prefix + "-v1",
                        TransactionProtocol.V1,
                        CommittableFormat.CURRENT,
                        0,
                        alreadyCommitted));
        saved.add(
                prepareTransaction(
                        properties,
                        prefix + "-v2-last",
                        TransactionProtocol.V2,
                        CommittableFormat.CURRENT,
                        0,
                        alreadyCommitted));

        recoverSuccessfullyWithSharedProducer(properties, prefix, saved);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testRecoverLegacyRolloverAfterPendingTransactions() throws Exception {
        Properties properties = getProperties(KAFKA.getBootstrapServers());
        String prefix = "legacy-batch-recovery";

        // Prepare the rollover first so advancing 32766 epochs cannot time out the pending
        // transactions that will be recovered before it.
        KafkaCommittable rollover =
                prepareTransaction(
                        properties,
                        prefix + "-rollover",
                        TransactionProtocol.V2,
                        CommittableFormat.LEGACY,
                        Short.MAX_VALUE - 1,
                        true);
        assertThat(drainAllRecordsFromTopic(rollover.getTransactionalId(), properties, true))
                .singleElement()
                .satisfies(record -> assertThat(record.value()).isEqualTo(new byte[] {1}));

        List<KafkaCommittable> saved = new ArrayList<>();
        // Committing the pending transactions lets the shared recovery producer discover V2.
        // Legacy state has no protocol flag and must not reset that discovery to known V1.
        saved.add(
                prepareTransaction(
                        properties,
                        prefix + "-pending-0",
                        TransactionProtocol.V2,
                        CommittableFormat.LEGACY,
                        0,
                        false));
        saved.add(
                prepareTransaction(
                        properties,
                        prefix + "-pending-1",
                        TransactionProtocol.V2,
                        CommittableFormat.LEGACY,
                        0,
                        false));
        saved.add(rollover);

        recoverSuccessfullyWithSharedProducer(properties, prefix, saved);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void testRecoverLegacyRolloverWithoutProtocolDiscovery() throws Exception {
        Properties properties = getProperties(KAFKA.getBootstrapServers());
        String transactionalId = "legacy-rollover-without-discovery";
        KafkaCommittable saved =
                prepareTransaction(
                        properties,
                        transactionalId,
                        TransactionProtocol.V2,
                        CommittableFormat.LEGACY,
                        Short.MAX_VALUE - 1,
                        true);
        assertThat(saved.getTransactionV2Enabled()).isNull();

        // A fresh producer cannot recognize this completed V2 transaction through V1 EndTxn.
        // Missing legacy protocol metadata must not cause repeated failover, but the rejection
        // must still be reported as a failure rather than as a confirmed commit.
        for (int attempt = 0; attempt < 3; attempt++) {
            try (ReadableBackchannel<TransactionFinished> backchannel =
                            BackchannelFactory.getInstance()
                                    .getReadableBackchannel(0, 0, transactionalId);
                    KafkaCommitter committer =
                            new KafkaCommitter(
                                    properties,
                                    transactionalId,
                                    0,
                                    0,
                                    true,
                                    FlinkKafkaInternalProducer::new)) {
                MockCommitRequest<KafkaCommittable> request = new MockCommitRequest<>(saved);
                committer.commit(Collections.singletonList(request));
                assertThat(request.getFailedWithUnknownReason()).isNull();
                assertThat(request.getNumberOfRetries()).isZero();
                assertThat(backchannel.poll())
                        .isEqualTo(TransactionFinished.erroneously(transactionalId));
                assertThat(backchannel.poll()).isNull();
            }
        }

        assertThat(drainAllRecordsFromTopic(transactionalId, properties, true))
                .singleElement()
                .satisfies(record -> assertThat(record.value()).isEqualTo(new byte[] {1}));
    }

    private static void recoverSuccessfullyWithSharedProducer(
            Properties properties, String prefix, List<KafkaCommittable> saved) throws Exception {
        AtomicInteger createdProducers = new AtomicInteger();
        try (ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance().getReadableBackchannel(0, 0, prefix);
                KafkaCommitter committer =
                        new KafkaCommitter(
                                properties,
                                prefix,
                                0,
                                0,
                                true,
                                (config, transactionalId) -> {
                                    createdProducers.incrementAndGet();
                                    return new FlinkKafkaInternalProducer<>(
                                            config, transactionalId);
                                })) {
            for (KafkaCommittable committable : saved) {
                MockCommitRequest<KafkaCommittable> request = new MockCommitRequest<>(committable);
                committer.commit(Collections.singletonList(request));
                assertThat(request.getFailedWithUnknownReason())
                        .as("Unknown failure for %s", committable.getTransactionalId())
                        .isNull();
                assertThat(request.getNumberOfRetries()).isZero();
                // Also reject swallowed fencing errors or deferred retries.
                assertThat(backchannel.poll())
                        .as("Recovery acknowledgement for %s", committable.getTransactionalId())
                        .isEqualTo(
                                TransactionFinished.successful(committable.getTransactionalId()));
            }
            assertThat(backchannel.poll()).isNull();
            assertThat(createdProducers).hasValue(1);
        }

        for (KafkaCommittable committable : saved) {
            assertThat(drainAllRecordsFromTopic(committable.getTransactionalId(), properties, true))
                    .singleElement()
                    .satisfies(record -> assertThat(record.value()).isEqualTo(new byte[] {1}));
        }
    }

    private static KafkaCommittable prepareTransaction(
            Properties properties,
            String transactionalId,
            TransactionProtocol protocol,
            CommittableFormat format,
            int targetEpoch,
            boolean alreadyCommitted)
            throws Exception {
        boolean transactionV2Enabled = protocol == TransactionProtocol.V2;
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
            producer.initTransactions();
            advanceEpoch(producer, targetEpoch);
            assertThat(producer.isTransactionV2Enabled()).isTrue();
            if (!transactionV2Enabled) {
                // A V2-capable broker also accepts V1 transactions from older clients. Select
                // V1 before writing, so this is a real V1 transaction, not mislabeled V2 state.
                useTransactionV1(producer);
            }
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(transactionalId, new byte[] {1})).get();
            producer.flush();
            assertThat(producer.isTransactionV2Enabled()).isEqualTo(transactionV2Enabled);

            KafkaCommittable saved;
            if (format == CommittableFormat.LEGACY) {
                saved = deserializeLegacyCommittable(producer);
                assertThat(saved.getTransactionV2Enabled()).isNull();
            } else {
                KafkaCommittableSerializer serializer = new KafkaCommittableSerializer();
                saved =
                        serializer.deserialize(
                                serializer.getVersion(),
                                serializer.serialize(KafkaCommittable.of(producer)));
                assertThat(saved.getTransactionV2Enabled()).isEqualTo(transactionV2Enabled);
            }
            if (alreadyCommitted) {
                producer.commitTransaction();
                if (transactionV2Enabled && targetEpoch == Short.MAX_VALUE - 1) {
                    assertThat(producer.getProducerId()).isNotEqualTo(saved.getProducerId());
                    assertThat(producer.getEpoch()).isZero();
                } else {
                    assertThat(producer.getProducerId()).isEqualTo(saved.getProducerId());
                    assertThat(producer.getEpoch())
                            .isEqualTo((short) (saved.getEpoch() + (transactionV2Enabled ? 1 : 0)));
                }
            }
            return saved;
        }
    }

    private static KafkaCommittable deserializeLegacyCommittable(
            FlinkKafkaInternalProducer<?, ?> producer) throws IOException {
        // Encode the actual version-1 wire format. The current serializer would retain V2 and
        // hide the legacy-state regression.
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeShort(producer.getEpoch());
            out.writeLong(producer.getProducerId());
            out.writeUTF(producer.getTransactionalId());
            return new KafkaCommittableSerializer().deserialize(1, bytes.toByteArray());
        }
    }

    private static void useTransactionV1(FlinkKafkaInternalProducer<?, ?> producer)
            throws ReflectiveOperationException {
        Field managerField = KafkaProducer.class.getDeclaredField("transactionManager");
        managerField.setAccessible(true);
        Object transactionManager = managerField.get(producer);
        synchronized (transactionManager) {
            Field protocolField =
                    transactionManager.getClass().getDeclaredField("isTransactionV2Enabled");
            protocolField.setAccessible(true);
            protocolField.setBoolean(transactionManager, false);
        }
    }

    private enum TransactionProtocol {
        V1,
        V2
    }

    private enum CommittableFormat {
        LEGACY,
        CURRENT
    }
}
