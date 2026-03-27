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

import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.event.Level.ERROR;

/** Tests for {@link KafkaCommitter}. */
class KafkaCommitterTest {

    private static final int PRODUCER_ID = 0;
    private static final short EPOCH = 0;
    private static final String TRANS_ID = "transactionalId";
    public static final int ATTEMPT = 2;
    public static final int SUB_ID = 1;
    private static final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>>
            MOCK_FACTORY = (properties, transactionalId) -> new MockProducer(properties, null);

    @RegisterExtension
    public final LoggerAuditingExtension errorLogger =
            new LoggerAuditingExtension(KafkaCommitter.class, ERROR);

    @AfterEach
    public void check() {
        checkProducerLeak();
    }

    /** Causes a network error by inactive broker and tests that a retry will happen. */
    @Test
    public void testRetryCommittableOnRetriableError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);
            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isEqualTo(1);
            assertThat(backchannel).doesNotHave(transactionFinished(true));
        }
    }

    @Test
    public void testFailJobOnUnknownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            // will fail because transaction not started
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));
            committer.commit(Collections.singletonList(request));
            assertThat(request.getFailedWithUnknownReason())
                    .isInstanceOf(IllegalStateException.class);
            assertThat(request.getFailedWithUnknownReason().getMessage())
                    .contains("Transaction was not started");
            // do not recycle if a fail-over is triggered;
            // else there may be a race-condition in creating a new transaction with the same name
            assertThat(backchannel).doesNotHave(transactionFinished(false));
        }
    }

    @Test
    public void testFailJobOnKnownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<?, ?> producer =
                        new MockProducer(properties, new ProducerFencedException("test"));
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            // will fail because transaction not started
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));
            committer.commit(Collections.singletonList(request));
            assertThat(backchannel).has(transactionFinished(false));
        }
    }

    @Test
    void testInvalidPidMappingExceptionIsKnownFailure() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<?, ?> producer =
                        new MockProducer(properties, new InvalidPidMappingException("test"));
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));
            committer.commit(Collections.singletonList(request));
            assertThat(backchannel).has(transactionFinished(false));
        }
    }

    @Test
    public void testCommitterProducerClosedOnError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        AtomicInteger creationCounter = new AtomicInteger();
        BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> failingFactory =
                (props, transactionalId) -> {
                    creationCounter.incrementAndGet();
                    return new MockProducer(props, new ProducerFencedException("test"));
                };
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, failingFactory);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(new KafkaCommittable(0, (short) 0, TRANS_ID, null));
            committer.commit(Collections.singletonList(request));

            // will not reuse the producer in case of error
            assertThat(backchannel).has(transactionFinished(false));
            assertThat(committer.getCommittingProducer()).isNull();
            assertThat(creationCounter.get()).isEqualTo(1);

            // create a second producer
            committer.commit(Collections.singletonList(request));

            assertThat(backchannel).has(transactionFinished(false));
            assertThat(committer.getCommittingProducer()).isNull();
            assertThat(creationCounter.get()).isEqualTo(2);
        }
    }

    @Test
    public void testInterrupt() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        Properties properties = getProperties();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                "http://localhost:" + serverSocket.getLocalPort());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
        try (final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);

            AtomicBoolean interrupting = interruptOnMessage(Thread.currentThread(), serverSocket);
            assertThatThrownBy(() -> committer.commit(Collections.singletonList(request)))
                    .isInstanceOf(InterruptedException.class);

            // verify that the interrupt happened only after committing started
            assertThat(interrupting).isTrue();

            // no errors are logged
            assertThat(errorLogger.getMessages()).isEmpty();

            assertThat(backchannel).doesNotHave(transactionFinished(true));
        }
    }

    private AtomicBoolean interruptOnMessage(Thread mainThread, ServerSocket serverSocket) {
        final AtomicBoolean interrupting = new AtomicBoolean();
        new Thread(
                        () -> {
                            try {
                                serverSocket.accept().getInputStream().read();
                                interrupting.set(true);
                                mainThread.interrupt();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        "canceller")
                .start();
        return interrupting;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testKafkaCommitterRecyclesTransactionalId(boolean hasProducer)
            throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (FlinkKafkaInternalProducer<?, ?> producer = new MockProducer(properties, null);
                final KafkaCommitter committer =
                        new KafkaCommitter(
                                properties, TRANS_ID, SUB_ID, ATTEMPT, false, MOCK_FACTORY);
                ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(
                            new KafkaCommittable(
                                    PRODUCER_ID, EPOCH, TRANS_ID, hasProducer ? producer : null));

            committer.commit(Collections.singletonList(request));
            assertThat(backchannel).has(transactionFinished(true));
            // will reuse the committer producer if no producer is passed
            assertThat(committer.getCommittingProducer() == null).isEqualTo(hasProducer);
        }
    }

    private Condition<? super ReadableBackchannel<TransactionFinished>> transactionFinished(
            boolean success) {
        return new Condition<>(
                backchannel -> {
                    TransactionFinished polled = backchannel.poll();
                    return polled != null && polled.isSuccess() == success;
                },
                "recycled producer");
    }

    Properties getProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:1");
        // Low timeout will fail commitTransaction quicker
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private static class MockProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {
        private final RuntimeException commitException;

        public MockProducer(Properties properties, RuntimeException commitException) {
            super(properties, KafkaCommitterTest.TRANS_ID);
            this.commitException = commitException;
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            if (commitException != null) {
                throw commitException;
            }
        }

        @Override
        public void flush() {}
    }
}
