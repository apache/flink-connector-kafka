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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SinkOptions;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.kafka.testutils.DockerImageVersions.APACHE_KAFKA;
import static org.apache.flink.connector.kafka.testutils.DockerImageVersions.CP_KAFKA;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaCommitter} that need a broker. */
class KafkaCommitterITCase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    private static final String TRANSACTIONAL_ID_PREFIX = "test-prefix";
    private static final int SUB_ID = 0;
    private static final int ATTEMPT = 0;
    private static final int MAX_RETRIES = SinkOptions.COMMITTER_RETRIES.defaultValue();

    private TestKafkaContainer kafkaContainer;
    private boolean brokerPaused;

    @AfterEach
    void tearDown() {
        try {
            checkProducerLeak();
        } finally {
            if (kafkaContainer != null) {
                kafkaContainer.stop();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {CP_KAFKA, APACHE_KAFKA})
    void testRetryRestoredCommittablesAfterTimeout(String dockerImage) throws Exception {
        startBroker(dockerImage);

        final String topic = "retry-restored-committables-after-timeout";
        final String firstTransactionalId = TRANSACTIONAL_ID_PREFIX + "-0";
        final String secondTransactionalId = TRANSACTIONAL_ID_PREFIX + "-1";
        final MockCommitRequest<KafkaCommittable> first =
                new MockCommitRequest<>(precommitRecord(topic, firstTransactionalId));
        final MockCommitRequest<KafkaCommittable> second =
                new MockCommitRequest<>(precommitRecord(topic, secondTransactionalId));

        final AtomicInteger producerCreations = new AtomicInteger();
        try (KafkaCommitter committer =
                        new KafkaCommitter(
                                getCommitterConfig(),
                                TRANSACTIONAL_ID_PREFIX,
                                SUB_ID,
                                ATTEMPT,
                                false,
                                (properties, transactionalId) -> {
                                    producerCreations.incrementAndGet();
                                    return new FlinkKafkaInternalProducer<>(
                                            properties, transactionalId);
                                });
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANSACTIONAL_ID_PREFIX)) {
            List<CommitRequest<KafkaCommittable>> pending = List.of(first, second);
            pauseBroker();
            try {
                pending = commitAndGetRetries(committer, pending);
            } finally {
                unpauseBroker();
            }
            assertThat(pending).containsExactly(first, second);

            pending = retry(committer, pending);

            assertThat(pending).isEmpty();
            assertThat(first.getFailedWithUnknownReason()).isNull();
            assertThat(second.getFailedWithUnknownReason()).isNull();
            assertThat(Arrays.asList(backchannel.poll(), backchannel.poll()))
                    .containsExactlyInAnyOrder(
                            TransactionFinished.successful(firstTransactionalId),
                            TransactionFinished.successful(secondTransactionalId));
        }
        waitForCommittedRecords(topic, firstTransactionalId, secondTransactionalId);
        // every transaction was committed by the producer of its first attempt
        assertThat(producerCreations.get()).isEqualTo(2);
    }

    /**
     * The commit of the first transaction times out and fails later, because the transaction was
     * fenced. The broker gives that answer while the committer commits the second transaction. The
     * answer must not be taken for the outcome of the second transaction, which is intact and would
     * be dropped otherwise.
     */
    @ParameterizedTest
    @ValueSource(strings = {CP_KAFKA, APACHE_KAFKA})
    void testFencedCommitAfterTimeoutDoesNotFailNextCommittable(String dockerImage)
            throws Exception {
        startBroker(dockerImage);

        final String topic = "fenced-commit-after-timeout";
        final String fencedTransactionalId = TRANSACTIONAL_ID_PREFIX + "-0";
        final String intactTransactionalId = TRANSACTIONAL_ID_PREFIX + "-1";
        final MockCommitRequest<KafkaCommittable> fenced =
                new MockCommitRequest<>(precommitRecord(topic, fencedTransactionalId));
        final MockCommitRequest<KafkaCommittable> intact =
                new MockCommitRequest<>(precommitRecord(topic, intactTransactionalId));
        fence(fencedTransactionalId);

        try (KafkaCommitter committer =
                        new KafkaCommitter(
                                getCommitterConfig(),
                                TRANSACTIONAL_ID_PREFIX,
                                SUB_ID,
                                ATTEMPT,
                                false,
                                (properties, transactionalId) ->
                                        new FlinkKafkaInternalProducer<byte[], byte[]>(
                                                properties, transactionalId) {
                                            @Override
                                            public void commitTransaction()
                                                    throws ProducerFencedException {
                                                // the broker answers again as soon as the committer
                                                // turns to the intact transaction
                                                if (intactTransactionalId.equals(
                                                        getTransactionalId())) {
                                                    unpauseBroker();
                                                }
                                                super.commitTransaction();
                                            }
                                        });
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANSACTIONAL_ID_PREFIX)) {
            List<CommitRequest<KafkaCommittable>> pending = List.of(fenced, intact);
            pauseBroker();
            try {
                pending = commitAndGetRetries(committer, pending);
            } finally {
                unpauseBroker();
            }

            pending = retry(committer, pending);

            assertThat(pending).isEmpty();
            assertThat(fenced.getFailedWithUnknownReason()).isNull();
            assertThat(intact.getFailedWithUnknownReason()).isNull();
            assertThat(Arrays.asList(backchannel.poll(), backchannel.poll()))
                    .containsExactlyInAnyOrder(
                            TransactionFinished.erroneously(fencedTransactionalId),
                            TransactionFinished.successful(intactTransactionalId));
        }
        waitForCommittedRecords(topic, intactTransactionalId);
    }

    /**
     * A committable of a chained writer comes with the producer of the writer. After a timeout the
     * commit has to be repeated on that producer, and the writer has to be able to use the producer
     * for its next transaction afterwards.
     */
    @ParameterizedTest
    @ValueSource(strings = {CP_KAFKA, APACHE_KAFKA})
    void testRetryWithWriterProducerAfterTimeout(String dockerImage) throws Exception {
        startBroker(dockerImage);

        final String topic = "retry-writer-producer-after-timeout";
        final String transactionalId = TRANSACTIONAL_ID_PREFIX + "-0";
        final String nextTransactionalId = TRANSACTIONAL_ID_PREFIX + "-1";

        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                        new FlinkKafkaInternalProducer<>(getCommitterConfig(), transactionalId);
                KafkaCommitter committer =
                        new KafkaCommitter(
                                getCommitterConfig(),
                                TRANSACTIONAL_ID_PREFIX,
                                SUB_ID,
                                ATTEMPT,
                                false,
                                FlinkKafkaInternalProducer::new);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANSACTIONAL_ID_PREFIX)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(newRecord(topic, transactionalId));
            producer.flush();
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));
            producer.precommitTransaction();

            List<CommitRequest<KafkaCommittable>> pending = List.of(request);
            pauseBroker();
            try {
                pending = commitAndGetRetries(committer, pending);
            } finally {
                unpauseBroker();
            }
            assertThat(pending).containsExactly(request);

            pending = retry(committer, pending);

            assertThat(pending).isEmpty();
            assertThat(request.getFailedWithUnknownReason()).isNull();
            assertThat(backchannel.poll())
                    .isEqualTo(TransactionFinished.successful(transactionalId));

            // what the producer pool of the writer does with a producer that was handed back
            producer.setTransactionId(nextTransactionalId);
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(newRecord(topic, nextTransactionalId));
            producer.commitTransaction();
        }
        waitForCommittedRecords(topic, transactionalId, nextTransactionalId);
    }

    /** Calls the committer once and returns the requests that asked for a retry. */
    private static List<CommitRequest<KafkaCommittable>> commitAndGetRetries(
            KafkaCommitter committer, List<CommitRequest<KafkaCommittable>> requests)
            throws IOException, InterruptedException {
        final List<Integer> retriesBefore =
                requests.stream()
                        .map(CommitRequest::getNumberOfRetries)
                        .collect(Collectors.toList());
        committer.commit(requests);
        return IntStream.range(0, requests.size())
                .filter(i -> requests.get(i).getNumberOfRetries() > retriesBefore.get(i))
                .mapToObj(requests::get)
                .collect(Collectors.toList());
    }

    /**
     * Retries like Flink does: at once, only the requests that asked for it, and a limited number
     * of times. Returns the requests that still ask for a retry after that.
     */
    private static List<CommitRequest<KafkaCommittable>> retry(
            KafkaCommitter committer, List<CommitRequest<KafkaCommittable>> requests)
            throws IOException, InterruptedException {
        List<CommitRequest<KafkaCommittable>> pending = requests;
        for (int retry = 0; retry < MAX_RETRIES && !pending.isEmpty(); retry++) {
            pending = commitAndGetRetries(committer, pending);
        }
        return pending;
    }

    /**
     * Writes a record in a transaction and returns the committable in the form in which it is
     * restored from state, that is without the producer.
     */
    private KafkaCommittable precommitRecord(String topic, String transactionalId) {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(getProducerConfig(), transactionalId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(newRecord(topic, transactionalId));
            producer.flush();
            return new KafkaCommittable(
                    producer.getProducerId(), producer.getEpoch(), transactionalId, null);
        }
    }

    /** Takes over the transactional id like a newer producer, which aborts the open transaction. */
    private void fence(String transactionalId) {
        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(getProducerConfig(), transactionalId)) {
            producer.initTransactions();
        }
    }

    /** The value of the record is the id of the transaction that writes it. */
    private static ProducerRecord<byte[], byte[]> newRecord(String topic, String transactionalId) {
        return new ProducerRecord<>(topic, transactionalId.getBytes(StandardCharsets.UTF_8));
    }

    private void waitForCommittedRecords(String topic, String... transactionalIds)
            throws Exception {
        final List<String> expected =
                Arrays.stream(transactionalIds).sorted().collect(Collectors.toList());
        // the broker answers a commit before it has written the transaction markers
        CommonTestUtils.waitUtil(
                () -> readCommittedRecords(topic).equals(expected),
                Duration.ofSeconds(30),
                "The committed records should be those of the transactions " + expected + ".");
    }

    private List<String> readCommittedRecords(String topic) {
        return drainAllRecordsFromTopic(topic, getConsumerConfig(), true).stream()
                .map(record -> new String(record.value(), StandardCharsets.UTF_8))
                .sorted()
                .collect(Collectors.toList());
    }

    private void startBroker(String dockerImage) {
        kafkaContainer = new TestKafkaContainer(dockerImage);
        kafkaContainer.start();
    }

    private void pauseBroker() {
        DockerClientFactory.instance()
                .client()
                .pauseContainerCmd(kafkaContainer.getContainerId())
                .exec();
        brokerPaused = true;
    }

    private void unpauseBroker() {
        if (brokerPaused) {
            DockerClientFactory.instance()
                    .client()
                    .unpauseContainerCmd(kafkaContainer.getContainerId())
                    .exec();
            brokerPaused = false;
        }
    }

    private Properties getProducerConfig() {
        Properties kafkaProducerConfig = new Properties();
        kafkaProducerConfig.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        kafkaProducerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // the open transaction must outlive the test
        kafkaProducerConfig.put(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) Duration.ofMinutes(10).toMillis());
        return kafkaProducerConfig;
    }

    /** A commit gives up waiting for the paused broker after three seconds. */
    private Properties getCommitterConfig() {
        Properties committerConfig = getProducerConfig();
        committerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        return committerConfig;
    }

    private Properties getConsumerConfig() {
        Properties kafkaConsumerConfig = new Properties();
        kafkaConsumerConfig.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return kafkaConsumerConfig;
    }
}
