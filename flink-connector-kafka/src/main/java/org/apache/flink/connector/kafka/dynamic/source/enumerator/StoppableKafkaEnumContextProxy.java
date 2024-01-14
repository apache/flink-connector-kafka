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
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A proxy enumerator context that supports life cycle management of underlying threads related to a
 * sub {@link org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator}. This is
 * motivated by the need to cancel the periodic partition discovery in scheduled tasks when sub
 * Kafka Enumerators are restarted. The worker thread pool in {@link
 * org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext} should not contain tasks of
 * inactive KafkaSourceEnumerators, after source restart.
 *
 * <p>Due to the inability to cancel scheduled tasks from {@link
 * org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext}, this enumerator context
 * will safely catch exceptions during enumerator restart and use a closeable proxy scheduler to
 * invoke tasks on the coordinator main thread to maintain the single threaded property.
 */
@Internal
public class StoppableKafkaEnumContextProxy
        implements SplitEnumeratorContext<KafkaPartitionSplit>, AutoCloseable {
    private static final Logger logger =
            LoggerFactory.getLogger(StoppableKafkaEnumContextProxy.class);

    private final String kafkaClusterId;
    private final KafkaMetadataService kafkaMetadataService;
    private final SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext;
    private final ScheduledExecutorService subEnumeratorWorker;
    private final Runnable signalNoMoreSplitsCallback;
    private boolean noMoreSplits = false;
    private volatile boolean isClosing;

    /**
     * Constructor for the enumerator context.
     *
     * @param kafkaClusterId The Kafka cluster id in order to maintain the mapping to the sub
     *     KafkaSourceEnumerator
     * @param kafkaMetadataService the Kafka metadata service to facilitate error handling
     * @param enumContext the underlying enumerator context
     * @param signalNoMoreSplitsCallback the callback when signal no more splits is invoked
     */
    public StoppableKafkaEnumContextProxy(
            String kafkaClusterId,
            KafkaMetadataService kafkaMetadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
            @Nullable Runnable signalNoMoreSplitsCallback) {
        this.kafkaClusterId = kafkaClusterId;
        this.kafkaMetadataService = kafkaMetadataService;
        this.enumContext = enumContext;
        this.subEnumeratorWorker =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory(kafkaClusterId + "-enum-worker"));
        this.signalNoMoreSplitsCallback = signalNoMoreSplitsCallback;
        this.isClosing = false;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return enumContext.metricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        enumContext.sendEventToSourceReader(subtaskId, event);
    }

    @Override
    public int currentParallelism() {
        return enumContext.currentParallelism();
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        return enumContext.registeredReaders();
    }

    /** Wrap splits with cluster metadata. */
    @Override
    public void assignSplits(SplitsAssignment<KafkaPartitionSplit> newSplitAssignments) {
        if (logger.isInfoEnabled()) {
            logger.info(
                    "Assigning {} splits for cluster {}: {}",
                    newSplitAssignments.assignment().values().stream()
                            .mapToLong(Collection::size)
                            .sum(),
                    kafkaClusterId,
                    newSplitAssignments);
        }

        Map<Integer, List<DynamicKafkaSourceSplit>> readerToSplitsMap = new HashMap<>();
        newSplitAssignments
                .assignment()
                .forEach(
                        (subtask, splits) ->
                                readerToSplitsMap.put(
                                        subtask,
                                        splits.stream()
                                                .map(
                                                        split ->
                                                                new DynamicKafkaSourceSplit(
                                                                        kafkaClusterId, split))
                                                .collect(Collectors.toList())));

        if (!readerToSplitsMap.isEmpty()) {
            enumContext.assignSplits(new SplitsAssignment<>(readerToSplitsMap));
        }
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        // There are no more splits for this cluster, but we need to wait until all clusters are
        // finished with their respective split discoveries. In the Kafka Source, this is called in
        // the coordinator thread, ensuring thread safety, for all source readers at the same time.
        noMoreSplits = true;
        if (signalNoMoreSplitsCallback != null) {
            // Thread safe idempotent callback
            signalNoMoreSplitsCallback.run();
        }
    }

    /** Execute the one time callables in the coordinator. */
    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        enumContext.callAsync(
                wrapCallAsyncCallable(callable), wrapCallAsyncCallableHandler(handler));
    }

    /**
     * Schedule task via internal thread pool to proxy task so that the task handler callback can
     * execute in the single threaded source coordinator thread pool to avoid synchronization needs.
     *
     * <p>Having the scheduled task in the internal thread pool also allows us to cancel the task
     * when the context needs to close due to dynamic enumerator restart.
     *
     * <p>In the case of KafkaEnumerator partition discovery, the callback modifies KafkaEnumerator
     * object state.
     */
    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelay,
            long period) {
        subEnumeratorWorker.scheduleAtFixedRate(
                () -> callAsync(callable, handler), initialDelay, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        enumContext.runInCoordinatorThread(runnable);
    }

    public boolean isNoMoreSplits() {
        return noMoreSplits;
    }

    /**
     * Note that we can't close the source coordinator here, because these contexts can be closed
     * during metadata change when the coordinator still needs to continue to run. We can only close
     * the coordinator context in Flink job shutdown, which Flink will do for us. That's why there
     * is the complexity of the internal thread pools in this class.
     *
     * <p>TODO: Attach Flink JIRA ticket -- discuss with upstream how to cancel scheduled tasks
     * belonging to enumerator.
     */
    @Override
    public void close() throws Exception {
        logger.info("Closing enum context for {}", kafkaClusterId);
        if (subEnumeratorWorker != null) {
            // KafkaSubscriber worker thread will fail if admin client is closed in the middle.
            // Swallow the error and set the context to closed state.
            isClosing = true;
            subEnumeratorWorker.shutdown();
            subEnumeratorWorker.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Wraps callable in call async executed in worker thread pool with exception propagation to
     * optimize on doing IO in non-coordinator thread.
     */
    protected <T> Callable<T> wrapCallAsyncCallable(Callable<T> callable) {
        return () -> {
            try {
                return callable.call();
            } catch (Exception e) {
                if (isClosing) {
                    throw new HandledFlinkKafkaException(e, kafkaClusterId);
                }

                Optional<KafkaException> throwable =
                        ExceptionUtils.findThrowable(e, KafkaException.class);
                // check if Kafka related and if Kafka cluster is inactive
                if (throwable.isPresent()
                        && !kafkaMetadataService.isClusterActive(kafkaClusterId)) {
                    throw new HandledFlinkKafkaException(throwable.get(), kafkaClusterId);
                }

                throw e;
            }
        };
    }

    /**
     * Handle exception that is propagated by a callable, executed on coordinator thread. Depending
     * on condition(s) the exception may be swallowed or forwarded. This is the Kafka topic
     * partition discovery callable handler.
     */
    protected <T> BiConsumer<T, Throwable> wrapCallAsyncCallableHandler(
            BiConsumer<T, Throwable> mainHandler) {
        return (result, t) -> {
            // check if exception is handled
            Optional<HandledFlinkKafkaException> throwable =
                    ExceptionUtils.findThrowable(t, HandledFlinkKafkaException.class);
            if (throwable.isPresent()) {
                logger.warn("Swallowed handled exception for {}.", kafkaClusterId, throwable.get());
                return;
            }

            // let the main handler deal with the potential exception
            mainHandler.accept(result, t);
        };
    }

    /**
     * General exception to signal to internal exception handling mechanisms that a benign error
     * occurred.
     */
    @Internal
    public static class HandledFlinkKafkaException extends RuntimeException {
        private static final String ERROR_MESSAGE = "An error occurred with %s";

        private final String kafkaClusterId;

        public HandledFlinkKafkaException(Throwable cause, String kafkaClusterId) {
            super(cause);
            this.kafkaClusterId = kafkaClusterId;
        }

        public String getMessage() {
            return String.format(ERROR_MESSAGE, kafkaClusterId);
        }
    }

    /**
     * This factory exposes a way to override the {@link StoppableKafkaEnumContextProxy} used in the
     * enumerator. This pluggable factory is extended in unit tests to facilitate invoking the
     * periodic discovery loops on demand.
     */
    @Internal
    public interface StoppableKafkaEnumContextProxyFactory {

        StoppableKafkaEnumContextProxy create(
                SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
                String kafkaClusterId,
                KafkaMetadataService kafkaMetadataService,
                Runnable signalNoMoreSplitsCallback);

        static StoppableKafkaEnumContextProxyFactory getDefaultFactory() {
            return (enumContext,
                    kafkaClusterId,
                    kafkaMetadataService,
                    signalNoMoreSplitsCallback) ->
                    new StoppableKafkaEnumContextProxy(
                            kafkaClusterId,
                            kafkaMetadataService,
                            enumContext,
                            signalNoMoreSplitsCallback);
        }
    }
}
