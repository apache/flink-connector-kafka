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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Runs dynamic Kafka stream metadata discovery outside Flink's shared source coordinator worker.
 *
 * <p>The source coordinator worker is also used by per-cluster Kafka partition discovery. Removed
 * clusters can block that shared worker in Kafka Admin calls, so stream metadata discovery needs an
 * isolated worker to keep cluster removal reconciliation responsive.
 */
@Internal
public class StoppableKafkaMetadataServiceDiscoveryContext implements AutoCloseable {
    private final SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext;
    private final ScheduledExecutorService metadataDiscoveryWorker;
    private volatile boolean isClosing;

    public StoppableKafkaMetadataServiceDiscoveryContext(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext) {
        this.enumContext = enumContext;
        this.metadataDiscoveryWorker =
                Executors.newScheduledThreadPool(
                        1,
                        runnable ->
                                createDaemonThread(
                                        runnable, "dynamic-kafka-metadata-discovery-worker"));
        this.isClosing = false;
    }

    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        metadataDiscoveryWorker.execute(() -> invokeCallable(callable, handler));
    }

    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelay,
            long period) {
        metadataDiscoveryWorker.scheduleAtFixedRate(
                () -> invokeCallable(callable, handler),
                initialDelay,
                period,
                TimeUnit.MILLISECONDS);
    }

    private <T> void invokeCallable(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        T result = null;
        Throwable throwable = null;
        try {
            result = callable.call();
        } catch (Throwable t) {
            throwable = t;
        }

        T fetchedResult = result;
        Throwable fetchError = throwable;
        runInCoordinatorThreadIfOpen(handler, fetchedResult, fetchError);
    }

    private synchronized <T> void runInCoordinatorThreadIfOpen(
            BiConsumer<T, Throwable> handler, T result, Throwable t) {
        if (!isClosing) {
            enumContext.runInCoordinatorThread(() -> handleResult(handler, result, t));
        }
    }

    private <T> void handleResult(BiConsumer<T, Throwable> handler, T result, Throwable t) {
        if (!isClosing) {
            handler.accept(result, t);
        }
    }

    private static Thread createDaemonThread(Runnable runnable, String threadName) {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(true);
        return thread;
    }

    void prepareForClose() {
        synchronized (this) {
            isClosing = true;
            metadataDiscoveryWorker.shutdownNow();
        }
    }

    @Override
    public void close() throws InterruptedException {
        prepareForClose();
        metadataDiscoveryWorker.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /** Factory for metadata discovery contexts. */
    @Internal
    public interface StoppableKafkaMetadataServiceDiscoveryContextFactory {
        StoppableKafkaMetadataServiceDiscoveryContext create(
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext);

        static StoppableKafkaMetadataServiceDiscoveryContextFactory getDefaultFactory() {
            return StoppableKafkaMetadataServiceDiscoveryContext::new;
        }

        static StoppableKafkaMetadataServiceDiscoveryContextFactory
                getSplitEnumeratorContextFactory() {
            return enumContext ->
                    new StoppableKafkaMetadataServiceDiscoveryContext(enumContext) {
                        @Override
                        public <T> void callAsync(
                                Callable<T> callable, BiConsumer<T, Throwable> handler) {
                            enumContext.callAsync(callable, handler);
                        }

                        @Override
                        public <T> void callAsync(
                                Callable<T> callable,
                                BiConsumer<T, Throwable> handler,
                                long initialDelay,
                                long period) {
                            enumContext.callAsync(callable, handler, initialDelay, period);
                        }
                    };
        }
    }
}
