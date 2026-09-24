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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A test for {@link StoppableKafkaMetadataServiceDiscoveryContext}. */
public class StoppableKafkaMetadataServiceDiscoveryContextTest {

    @Test
    public void testMetadataDiscoveryDoesNotUseSourceCoordinatorWorker() throws Exception {
        TrackingSplitEnumeratorContext enumContext = new TrackingSplitEnumeratorContext();

        AtomicReference<String> callbackResult = new AtomicReference<>();
        try (StoppableKafkaMetadataServiceDiscoveryContext context =
                new StoppableKafkaMetadataServiceDiscoveryContext(enumContext)) {
            context.callAsync(
                    () -> "metadata-refresh-complete",
                    (result, t) -> {
                        assertThat(t).isNull();
                        callbackResult.set(result);
                    });

            CommonTestUtils.waitUtil(
                    () -> "metadata-refresh-complete".equals(callbackResult.get()),
                    Duration.ofSeconds(5),
                    "Metadata refresh callback did not complete");
            assertThat(callbackResult).hasValue("metadata-refresh-complete");
            assertThat(enumContext.coordinatorThreadCallCount()).isEqualTo(1);
            assertThat(enumContext.sourceCoordinatorAsyncCallCount()).isZero();
        }
    }

    @Test
    public void testCloseWaitsForMetadataDiscoveryWorker() throws Exception {
        TrackingSplitEnumeratorContext enumContext = new TrackingSplitEnumeratorContext();
        CountDownLatch callableStarted = new CountDownLatch(1);
        CountDownLatch allowCallableToFinish = new CountDownLatch(1);
        ExecutorService closeExecutor = Executors.newSingleThreadExecutor();

        try (StoppableKafkaMetadataServiceDiscoveryContext context =
                new StoppableKafkaMetadataServiceDiscoveryContext(enumContext)) {
            context.callAsync(
                    () -> {
                        callableStarted.countDown();
                        awaitUninterruptibly(allowCallableToFinish);
                        return "metadata-refresh-complete";
                    },
                    (result, t) -> {});

            assertThat(callableStarted.await(10, TimeUnit.SECONDS))
                    .as("metadata discovery callable should start")
                    .isTrue();

            Future<?> closeFuture =
                    closeExecutor.submit(
                            () -> {
                                context.close();
                                return null;
                            });
            assertThatThrownBy(() -> closeFuture.get(100, TimeUnit.MILLISECONDS))
                    .as("close should wait for the metadata discovery worker")
                    .isInstanceOf(TimeoutException.class);

            allowCallableToFinish.countDown();
            assertThatCode(() -> closeFuture.get(10, TimeUnit.SECONDS))
                    .as("close should finish after the metadata discovery worker exits")
                    .doesNotThrowAnyException();
            assertThat(enumContext.coordinatorThreadCallCount()).isZero();
        } finally {
            allowCallableToFinish.countDown();
            closeExecutor.shutdownNow();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static class TrackingSplitEnumeratorContext
            implements SplitEnumeratorContext<DynamicKafkaSourceSplit> {
        private final AtomicInteger sourceCoordinatorAsyncCallCount = new AtomicInteger();
        private final AtomicInteger coordinatorThreadCallCount = new AtomicInteger();

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return Collections.emptyMap();
        }

        @Override
        public void assignSplits(SplitsAssignment<DynamicKafkaSourceSplit> newSplitAssignments) {}

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            sourceCoordinatorAsyncCallCount.incrementAndGet();
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelay,
                long period) {
            sourceCoordinatorAsyncCallCount.incrementAndGet();
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            coordinatorThreadCallCount.incrementAndGet();
            runnable.run();
        }

        private int sourceCoordinatorAsyncCallCount() {
            return sourceCoordinatorAsyncCallCount.get();
        }

        private int coordinatorThreadCallCount() {
            return coordinatorThreadCallCount.get();
        }
    }
}
