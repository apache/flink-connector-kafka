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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSetSubscriber;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** A test for {@link SynchronizedKafkaMetadataService}. */
public class SynchronizedKafkaMetadataServiceTest {

    @Test
    public void testSubscribedStreamsAndClusterActivityChecksAreSerialized() throws Exception {
        BlockingKafkaMetadataService delegate = new BlockingKafkaMetadataService();
        KafkaMetadataService metadataService = new SynchronizedKafkaMetadataService(delegate);
        KafkaStreamSetSubscriber kafkaStreamSubscriber =
                new KafkaStreamSetSubscriber(Collections.singleton("stream"));
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<Set<KafkaStream>> subscribedStreams =
                    executor.submit(
                            () -> kafkaStreamSubscriber.getSubscribedStreams(metadataService));
            assertThat(delegate.awaitDescribeStreamsStarted())
                    .as("subscribed stream metadata fetch should start")
                    .isTrue();

            Future<Boolean> isClusterActive =
                    executor.submit(() -> metadataService.isClusterActive("cluster"));
            assertThat(delegate.awaitIsClusterActiveStarted(100, TimeUnit.MILLISECONDS))
                    .as("cluster activity check should wait for subscribed stream fetch")
                    .isFalse();

            delegate.allowDescribeStreams();
            assertThat(subscribedStreams.get(10, TimeUnit.SECONDS)).isEmpty();
            assertThat(isClusterActive.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(delegate.awaitIsClusterActiveStarted())
                    .as("cluster activity check should run after subscribed stream fetch")
                    .isTrue();
            assertThat(delegate.maxConcurrentCalls()).isEqualTo(1);
        } finally {
            delegate.allowDescribeStreams();
            executor.shutdownNow();
        }
    }

    @Test
    public void testCloseCanUnblockInFlightMetadataCall() throws Exception {
        BlockingKafkaMetadataService delegate = new BlockingKafkaMetadataService();
        KafkaMetadataService metadataService = new SynchronizedKafkaMetadataService(delegate);
        KafkaStreamSetSubscriber kafkaStreamSubscriber =
                new KafkaStreamSetSubscriber(Collections.singleton("stream"));
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<Set<KafkaStream>> subscribedStreams =
                    executor.submit(
                            () -> kafkaStreamSubscriber.getSubscribedStreams(metadataService));
            assertThat(delegate.awaitDescribeStreamsStarted())
                    .as("subscribed stream metadata fetch should start")
                    .isTrue();

            Future<?> close =
                    executor.submit(
                            () -> {
                                metadataService.close();
                                return null;
                            });
            assertThatCode(() -> close.get(10, TimeUnit.SECONDS))
                    .as("metadata service close should unblock the in-flight fetch")
                    .doesNotThrowAnyException();
            assertThat(subscribedStreams.get(10, TimeUnit.SECONDS)).isEmpty();
            assertThat(delegate.awaitCloseCalled())
                    .as("delegate close should run while metadata fetch is in flight")
                    .isTrue();
        } finally {
            delegate.allowDescribeStreams();
            executor.shutdownNow();
        }
    }

    private static class BlockingKafkaMetadataService implements KafkaMetadataService {
        private final CountDownLatch describeStreamsStarted = new CountDownLatch(1);
        private final CountDownLatch allowDescribeStreams = new CountDownLatch(1);
        private final CountDownLatch isClusterActiveStarted = new CountDownLatch(1);
        private final CountDownLatch closeCalled = new CountDownLatch(1);
        private final AtomicInteger concurrentCalls = new AtomicInteger();
        private final AtomicInteger maxConcurrentCalls = new AtomicInteger();

        @Override
        public Set<KafkaStream> getAllStreams() {
            return runMetadataCall(Collections::emptySet);
        }

        @Override
        public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
            return runMetadataCall(
                    () -> {
                        describeStreamsStarted.countDown();
                        assertThat(allowDescribeStreams.await(10, TimeUnit.SECONDS))
                                .as("test should release subscribed stream metadata fetch")
                                .isTrue();
                        return Collections.emptyMap();
                    });
        }

        @Override
        public boolean isClusterActive(String kafkaClusterId) {
            return runMetadataCall(
                    () -> {
                        isClusterActiveStarted.countDown();
                        return true;
                    });
        }

        private <T> T runMetadataCall(CheckedSupplier<T> callable) {
            int activeCalls = concurrentCalls.incrementAndGet();
            maxConcurrentCalls.updateAndGet(currentMax -> Math.max(currentMax, activeCalls));
            try {
                return callable.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                concurrentCalls.decrementAndGet();
            }
        }

        private boolean awaitDescribeStreamsStarted() throws InterruptedException {
            return describeStreamsStarted.await(10, TimeUnit.SECONDS);
        }

        private boolean awaitIsClusterActiveStarted() throws InterruptedException {
            return awaitIsClusterActiveStarted(10, TimeUnit.SECONDS);
        }

        private boolean awaitIsClusterActiveStarted(long timeout, TimeUnit timeUnit)
                throws InterruptedException {
            return isClusterActiveStarted.await(timeout, timeUnit);
        }

        private void allowDescribeStreams() {
            allowDescribeStreams.countDown();
        }

        private int maxConcurrentCalls() {
            return maxConcurrentCalls.get();
        }

        @Override
        public void close() {
            closeCalled.countDown();
            allowDescribeStreams();
        }

        private boolean awaitCloseCalled() throws InterruptedException {
            return closeCalled.await(10, TimeUnit.SECONDS);
        }
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }
}
