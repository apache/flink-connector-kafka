/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaCommitter}. */
@ExtendWith({TestLoggerExtension.class})
class KafkaCommitterTest {

    private static final int PRODUCER_ID = 0;
    private static final short EPOCH = 0;
    private static final String TRANS_ID = "transactionalId";
    public static final int ATTEMPT = 2;
    public static final int SUB_ID = 1;

    @AfterEach
    public void check() {
        checkProducerLeak();
    }

    /** Causes a network error by inactive broker and tests that a retry will happen. */
    @Test
    public void testRetryCommittableOnRetriableError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(properties, TRANS_ID, SUB_ID, ATTEMPT);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<?> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);
            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isEqualTo(1);
            assertThat(backchannel).doesNotHave(recycledProducer());
        }
    }

    @Test
    public void testFailJobOnUnknownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(properties, TRANS_ID, SUB_ID, ATTEMPT);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANS_ID);
                ReadableBackchannel<?> backchannel =
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
            assertThat(backchannel).doesNotHave(recycledProducer());
        }
    }

    @Test
    public void testFailJobOnKnownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer =
                        new KafkaCommitter(properties, TRANS_ID, SUB_ID, ATTEMPT);
                FlinkKafkaInternalProducer<?, ?> producer =
                        new MockProducer(properties, new ProducerFencedException("test"));
                ReadableBackchannel<?> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            // will fail because transaction not started
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(KafkaCommittable.of(producer));
            committer.commit(Collections.singletonList(request));
            // do not recycle if a fail-over is triggered;
            // else there may be a race-condition in creating a new transaction with the same name
            assertThat(backchannel).has(recycledProducer());
        }
    }

    @Test
    public void testKafkaCommitterRecyclesProducer() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (FlinkKafkaInternalProducer<?, ?> producer = new MockProducer(properties, null);
                final KafkaCommitter committer =
                        new KafkaCommitter(properties, TRANS_ID, SUB_ID, ATTEMPT);
                ReadableBackchannel<?> backchannel =
                        BackchannelFactory.getInstance()
                                .getReadableBackchannel(SUB_ID, ATTEMPT, TRANS_ID)) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(
                            new KafkaCommittable(PRODUCER_ID, EPOCH, TRANS_ID, producer));

            committer.commit(Collections.singletonList(request));
            assertThat(backchannel).has(recycledProducer());
        }
    }

    private Condition<? super ReadableBackchannel<?>> recycledProducer() {
        return new Condition<>(backchannel -> backchannel.poll() != null, "recycled producer");
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
