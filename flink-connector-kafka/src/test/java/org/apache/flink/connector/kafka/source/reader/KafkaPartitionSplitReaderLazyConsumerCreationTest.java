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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for FLINK-36434: the {@link KafkaConsumer} inside {@link KafkaPartitionSplitReader}
 * must be created lazily on the thread that uses it (the split fetcher thread), not on the thread
 * that constructs the reader (the source-reader or checkpoint thread), because the consumer is not
 * thread-safe.
 *
 * <p>These tests need no running Kafka cluster: constructing a {@link KafkaConsumer} performs no
 * network I/O, and {@link KafkaPartitionSplitReader#fetch()} tolerates the {@code
 * IllegalStateException}/{@code WakeupException} that polling without an assignment raises.
 */
class KafkaPartitionSplitReaderLazyConsumerCreationTest {

    /** Records the thread the consumer was created on and every {@code wakeup()} call. */
    private static class CreationRecordingReader extends KafkaPartitionSplitReader {
        private final AtomicReference<Thread> creationThread = new AtomicReference<>();
        private final AtomicInteger wakeups = new AtomicInteger();

        CreationRecordingReader() {
            super(
                    testProperties(),
                    new TestingReaderContext(
                            new Configuration(),
                            UnregisteredMetricsGroup.createSourceReaderMetricGroup()),
                    new KafkaSourceReaderMetrics(
                            UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
        }

        @Override
        KafkaConsumer<byte[], byte[]> createConsumer(Properties consumerProps) {
            creationThread.set(Thread.currentThread());
            return new KafkaConsumer<byte[], byte[]>(consumerProps) {
                @Override
                public void wakeup() {
                    wakeups.incrementAndGet();
                    super.wakeup();
                }
            };
        }
    }

    private static Properties testProperties() {
        Properties props = new Properties();
        // Never contacted: the consumer performs no network I/O until the first poll-style call.
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        // Keep close() from attempting an offset auto-commit against the unreachable broker.
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), "test-client");
        return props;
    }

    @Test
    void testConsumerIsCreatedLazilyOnTheUsingThread() throws Exception {
        CreationRecordingReader reader = new CreationRecordingReader();

        // Constructing the reader (source-reader / checkpoint thread) must not create a consumer.
        assertThat(reader.creationThread.get()).isNull();

        // First use on a different thread (standing in for the split fetcher thread).
        Thread useThread =
                CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        reader.fetch();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .thenApply(ignored -> reader.creationThread.get())
                        .get();

        assertThat(useThread).isNotNull();
        assertThat(useThread).isNotSameAs(Thread.currentThread());
        reader.close();
    }

    @Test
    void testWakeUpBeforeConsumerCreationIsSafelyDropped() throws Exception {
        CreationRecordingReader reader = new CreationRecordingReader();

        // wakeUp() before the consumer exists: no NPE, no consumer created. The base
        // SplitFetcher only wakes a running fetch task, which cannot exist before
        // handleSplitsChanges() has created the consumer, so a pre-creation wakeup has
        // nothing to interrupt and is dropped rather than deferred.
        assertThatCode(reader::wakeUp).doesNotThrowAnyException();
        assertThat(reader.creationThread.get()).isNull();
        assertThat(reader.wakeups.get()).isZero();

        // First use creates the consumer; the dropped wakeup is NOT replayed against it.
        CompletableFuture.runAsync(
                        () -> {
                            try {
                                reader.fetch();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .get();

        assertThat(reader.creationThread.get()).isNotNull();
        assertThat(reader.wakeups.get()).isZero();
        reader.close();
    }

    @Test
    void testWakeUpAfterCreationDelegatesDirectly() throws Exception {
        CreationRecordingReader reader = new CreationRecordingReader();
        // Trigger creation on this thread via the test accessor.
        reader.consumer();
        assertThat(reader.wakeups.get()).isZero();

        reader.wakeUp();
        assertThat(reader.wakeups.get()).isEqualTo(1);
        reader.close();
    }

    @Test
    void testCloseWithoutUseNeverCreatesConsumer() {
        CreationRecordingReader reader = new CreationRecordingReader();
        assertThatCode(reader::close).doesNotThrowAnyException();
        assertThat(reader.creationThread.get()).isNull();
    }
}
