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

package org.apache.flink.connector.kafka.dynamic.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.metrics.KafkaClusterMetricGroup;
import org.apache.flink.connector.kafka.dynamic.source.metrics.KafkaClusterMetricGroupManager;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaRecordEmitter;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Manages state about underlying {@link KafkaSourceReader} to collect records and commit offsets
 * from multiple Kafka clusters. This reader also handles changes to Kafka topology by reacting to
 * restart sequence initiated by the enumerator and suspending inconsistent sub readers.
 *
 * <p>First, in the restart sequence, we will receive the {@link MetadataUpdateEvent} from the
 * enumerator, stop all KafkaSourceReaders, and retain the relevant splits. Second, enumerator will
 * send all new splits that readers should work on (old splits will not be sent again).
 */
@Internal
public class DynamicKafkaSourceReader<T> implements SourceReader<T, DynamicKafkaSourceSplit> {
    private static final Logger logger = LoggerFactory.getLogger(DynamicKafkaSourceReader.class);
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final Properties properties;
    private final MetricGroup dynamicKafkaSourceMetricGroup;
    private final Gauge<Integer> kafkaClusterCount;
    private final SourceReaderContext readerContext;
    private final KafkaClusterMetricGroupManager kafkaClusterMetricGroupManager;

    // needs have a strict ordering for readers to guarantee availability future consistency
    private final NavigableMap<String, KafkaSourceReader<T>> clusterReaderMap;
    private final Map<String, Properties> clustersProperties;
    private final List<DynamicKafkaSourceSplit> pendingSplits;

    private MultipleFuturesAvailabilityHelper availabilityHelper;
    private int availabilityHelperSize;
    private boolean isActivelyConsumingSplits;
    private boolean isNoMoreSplits;
    private AtomicBoolean restartingReaders;

    public DynamicKafkaSourceReader(
            SourceReaderContext readerContext,
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            Properties properties) {
        this.readerContext = readerContext;
        this.clusterReaderMap = new TreeMap<>();
        this.deserializationSchema = deserializationSchema;
        this.properties = properties;
        this.kafkaClusterCount = clusterReaderMap::size;
        this.dynamicKafkaSourceMetricGroup =
                readerContext
                        .metricGroup()
                        .addGroup(KafkaClusterMetricGroup.DYNAMIC_KAFKA_SOURCE_METRIC_GROUP);
        this.kafkaClusterMetricGroupManager = new KafkaClusterMetricGroupManager();
        this.pendingSplits = new ArrayList<>();
        this.availabilityHelper =
                new MultipleFuturesAvailabilityHelper(this.availabilityHelperSize = 0);
        this.isNoMoreSplits = false;
        this.isActivelyConsumingSplits = false;
        this.restartingReaders = new AtomicBoolean();
        this.clustersProperties = new HashMap<>();
    }

    /**
     * This is invoked first only in reader startup without state. In stateful startup, splits are
     * added before this method is invoked.
     */
    @Override
    public void start() {
        logger.trace("Starting reader for subtask index={}", readerContext.getIndexOfSubtask());
        // metrics cannot be registered in the enumerator
        readerContext.metricGroup().gauge("kafkaClusterCount", kafkaClusterCount);
        readerContext.sendSourceEventToCoordinator(new GetMetadataUpdateEvent());
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> readerOutput) throws Exception {
        // at startup, do not return end of input if metadata event has not been received
        if (clusterReaderMap.isEmpty()) {
            return logAndReturnInputStatus(InputStatus.NOTHING_AVAILABLE);
        }

        if (restartingReaders.get()) {
            logger.debug("Poll next invoked while restarting readers");
            return logAndReturnInputStatus(InputStatus.NOTHING_AVAILABLE);
        }

        boolean isMoreAvailable = false;
        boolean isNothingAvailable = false;

        for (Map.Entry<String, KafkaSourceReader<T>> clusterReader : clusterReaderMap.entrySet()) {
            InputStatus inputStatus = clusterReader.getValue().pollNext(readerOutput);
            switch (inputStatus) {
                case MORE_AVAILABLE:
                    isMoreAvailable = true;
                    break;
                case NOTHING_AVAILABLE:
                    isNothingAvailable = true;
                    break;
            }
        }

        return logAndReturnInputStatus(consolidateInputStatus(isMoreAvailable, isNothingAvailable));
    }

    private InputStatus consolidateInputStatus(
            boolean atLeastOneMoreAvailable, boolean atLeastOneNothingAvailable) {
        final InputStatus inputStatus;
        if (atLeastOneMoreAvailable) {
            inputStatus = InputStatus.MORE_AVAILABLE;
        } else if (atLeastOneNothingAvailable) {
            inputStatus = InputStatus.NOTHING_AVAILABLE;
        } else {
            inputStatus = InputStatus.END_OF_INPUT;
        }
        return inputStatus;
    }

    // we also need to filter splits at startup in case checkpoint is not consistent bwtn enumerator
    // and reader
    @Override
    public void addSplits(List<DynamicKafkaSourceSplit> splits) {
        logger.info("Adding splits to reader {}: {}", readerContext.getIndexOfSubtask(), splits);
        // at startup, don't add splits until we get confirmation from enumerator of the current
        // metadata
        if (!isActivelyConsumingSplits) {
            pendingSplits.addAll(splits);
            return;
        }

        Map<String, List<KafkaPartitionSplit>> clusterSplitsMap = new HashMap<>();
        for (DynamicKafkaSourceSplit split : splits) {
            clusterSplitsMap
                    .computeIfAbsent(split.getKafkaClusterId(), unused -> new ArrayList<>())
                    .add(split);
        }

        Set<String> kafkaClusterIds = clusterSplitsMap.keySet();

        boolean newCluster = false;
        for (String kafkaClusterId : kafkaClusterIds) {
            // if a reader corresponding to the split doesn't exist, create it
            // it is possible that the splits come before the source event
            if (!clusterReaderMap.containsKey(kafkaClusterId)) {
                try {
                    KafkaSourceReader<T> kafkaSourceReader = createReader(kafkaClusterId);
                    clusterReaderMap.put(kafkaClusterId, kafkaSourceReader);
                    kafkaSourceReader.start();
                    newCluster = true;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            // add splits
            KafkaSourceReader<T> reader = clusterReaderMap.get(kafkaClusterId);
            reader.addSplits(clusterSplitsMap.get(kafkaClusterId));
        }

        // reset the availability future to also depend on the new sub readers
        if (newCluster) {
            completeAndResetAvailabilityHelper();
        }
    }

    /**
     * Duplicate source events are handled with idempotency. No metadata change means we simply skip
     * the restart logic.
     */
    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        Preconditions.checkArgument(
                sourceEvent instanceof MetadataUpdateEvent,
                "Received invalid source event: " + sourceEvent);

        logger.info(
                "Received source event {}: subtask={}",
                sourceEvent,
                readerContext.getIndexOfSubtask());
        Set<KafkaStream> newKafkaStreams = ((MetadataUpdateEvent) sourceEvent).getKafkaStreams();
        Map<String, Set<String>> newClustersAndTopics = new HashMap<>();
        Map<String, Properties> newClustersProperties = new HashMap<>();
        for (KafkaStream kafkaStream : newKafkaStreams) {
            for (Map.Entry<String, ClusterMetadata> clusterMetadataMapEntry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                newClustersAndTopics
                        .computeIfAbsent(
                                clusterMetadataMapEntry.getKey(), (unused) -> new HashSet<>())
                        .addAll(clusterMetadataMapEntry.getValue().getTopics());

                Properties clusterProperties = new Properties();
                KafkaPropertiesUtil.copyProperties(
                        clusterMetadataMapEntry.getValue().getProperties(), clusterProperties);
                OffsetsInitializer startingOffsetsInitializer =
                        clusterMetadataMapEntry.getValue().getStartingOffsetsInitializer();
                if (startingOffsetsInitializer != null) {
                    clusterProperties.setProperty(
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                            startingOffsetsInitializer
                                    .getAutoOffsetResetStrategy()
                                    .name()
                                    .toLowerCase());
                }
                newClustersProperties.put(clusterMetadataMapEntry.getKey(), clusterProperties);
            }
        }

        // filter current splits with the metadata update
        List<DynamicKafkaSourceSplit> currentSplitState = snapshotStateFromAllReaders(-1);
        logger.info(
                "Snapshotting split state for reader {}: {}",
                readerContext.getIndexOfSubtask(),
                currentSplitState);
        Map<String, Set<String>> currentMetadataFromState = new HashMap<>();
        Map<String, List<KafkaPartitionSplit>> filteredNewClusterSplitStateMap = new HashMap<>();

        // the data structures above
        for (DynamicKafkaSourceSplit split : currentSplitState) {
            currentMetadataFromState
                    .computeIfAbsent(split.getKafkaClusterId(), (ignore) -> new HashSet<>())
                    .add(split.getKafkaPartitionSplit().getTopic());
            // check if cluster topic exists in the metadata update
            if (newClustersAndTopics.containsKey(split.getKafkaClusterId())
                    && newClustersAndTopics
                            .get(split.getKafkaClusterId())
                            .contains(split.getKafkaPartitionSplit().getTopic())) {
                filteredNewClusterSplitStateMap
                        .computeIfAbsent(split.getKafkaClusterId(), (ignore) -> new ArrayList<>())
                        .add(split);
            } else {
                logger.info("Skipping outdated split due to metadata changes: {}", split);
            }
        }

        // only restart if there was metadata change to handle duplicate MetadataUpdateEvent from
        // enumerator. We can possibly only restart the readers whose metadata has changed but that
        // comes at the cost of complexity and it is an optimization for a corner case. We can
        // revisit if necessary.
        if (!newClustersAndTopics.equals(currentMetadataFromState)) {
            restartingReaders.set(true);
            closeAllReadersAndClearState();

            clustersProperties.putAll(newClustersProperties);
            for (String kafkaClusterId : newClustersAndTopics.keySet()) {
                try {
                    // restart kafka source readers with the relevant state
                    KafkaSourceReader<T> kafkaSourceReader = createReader(kafkaClusterId);
                    clusterReaderMap.put(kafkaClusterId, kafkaSourceReader);
                    if (filteredNewClusterSplitStateMap.containsKey(kafkaClusterId)) {
                        kafkaSourceReader.addSplits(
                                filteredNewClusterSplitStateMap.get(kafkaClusterId));
                    }
                    kafkaSourceReader.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            // reset the availability future to also depend on the new sub readers
            completeAndResetAvailabilityHelper();
        } else {
            // update properties even on no metadata change
            clustersProperties.clear();
            clustersProperties.putAll(newClustersProperties);
        }

        // finally mark the reader as active, if not already and add pending splits
        if (!isActivelyConsumingSplits) {
            isActivelyConsumingSplits = true;
        }

        if (!pendingSplits.isEmpty()) {
            List<DynamicKafkaSourceSplit> validPendingSplits =
                    pendingSplits.stream()
                            // Pending splits is used to cache splits at startup, before metadata
                            // update event arrives. Splits in state could be old and it's possible
                            // to not have another metadata update event, so need to filter the
                            // splits at this point.
                            .filter(
                                    pendingSplit -> {
                                        boolean splitValid =
                                                isSplitForActiveClusters(
                                                        pendingSplit, newClustersAndTopics);
                                        if (!splitValid) {
                                            logger.info(
                                                    "Removing invalid split for reader: {}",
                                                    pendingSplit);
                                        }
                                        return splitValid;
                                    })
                            .collect(Collectors.toList());

            addSplits(validPendingSplits);
            pendingSplits.clear();
            if (isNoMoreSplits) {
                notifyNoMoreSplits();
            }
        }
    }

    private static boolean isSplitForActiveClusters(
            DynamicKafkaSourceSplit split, Map<String, Set<String>> metadata) {
        return metadata.containsKey(split.getKafkaClusterId())
                && metadata.get(split.getKafkaClusterId())
                        .contains(split.getKafkaPartitionSplit().getTopic());
    }

    @Override
    public List<DynamicKafkaSourceSplit> snapshotState(long checkpointId) {
        List<DynamicKafkaSourceSplit> splits = snapshotStateFromAllReaders(checkpointId);

        // pending splits should be typically empty, since we do not add splits to pending splits if
        // reader has started
        splits.addAll(pendingSplits);
        return splits;
    }

    private List<DynamicKafkaSourceSplit> snapshotStateFromAllReaders(long checkpointId) {
        List<DynamicKafkaSourceSplit> splits = new ArrayList<>();
        for (Map.Entry<String, KafkaSourceReader<T>> clusterReader : clusterReaderMap.entrySet()) {
            clusterReader
                    .getValue()
                    .snapshotState(checkpointId)
                    .forEach(
                            kafkaPartitionSplit ->
                                    splits.add(
                                            new DynamicKafkaSourceSplit(
                                                    clusterReader.getKey(), kafkaPartitionSplit)));
        }

        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        availabilityHelper.resetToUnAvailable();
        syncAvailabilityHelperWithReaders();
        return (CompletableFuture<Void>) availabilityHelper.getAvailableFuture();
    }

    @Override
    public void notifyNoMoreSplits() {
        logger.info("notify no more splits for reader {}", readerContext.getIndexOfSubtask());
        if (pendingSplits.isEmpty()) {
            clusterReaderMap.values().forEach(KafkaSourceReader::notifyNoMoreSplits);
        }

        isNoMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        logger.debug("Notify checkpoint complete for {}", clusterReaderMap.keySet());
        for (KafkaSourceReader<T> subReader : clusterReaderMap.values()) {
            subReader.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        for (KafkaSourceReader<T> subReader : clusterReaderMap.values()) {
            subReader.close();
        }
        kafkaClusterMetricGroupManager.close();
    }

    private KafkaSourceReader<T> createReader(String kafkaClusterId) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        Properties readerSpecificProperties = new Properties();
        KafkaPropertiesUtil.copyProperties(properties, readerSpecificProperties);
        KafkaPropertiesUtil.copyProperties(
                Preconditions.checkNotNull(
                        clustersProperties.get(kafkaClusterId),
                        "Properties for cluster %s is not found. Current Kafka cluster ids: %s",
                        kafkaClusterId,
                        clustersProperties.keySet()),
                readerSpecificProperties);
        KafkaPropertiesUtil.setClientIdPrefix(readerSpecificProperties, kafkaClusterId);

        // layer a kafka cluster group to distinguish metrics by cluster
        KafkaClusterMetricGroup kafkaClusterMetricGroup =
                new KafkaClusterMetricGroup(
                        dynamicKafkaSourceMetricGroup, readerContext.metricGroup(), kafkaClusterId);
        kafkaClusterMetricGroupManager.register(kafkaClusterId, kafkaClusterMetricGroup);
        KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(kafkaClusterMetricGroup);

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        // adding kafkaClusterMetricGroup instead of the sourceReaderMetricGroup
                        // since there could be metric collision, so `kafkaCluster` group is
                        // necessary to
                        // distinguish between instances of this metric
                        return kafkaClusterMetricGroup.addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });

        KafkaRecordEmitter<T> recordEmitter = new KafkaRecordEmitter<>(deserializationSchema);
        return new KafkaSourceReader<>(
                elementsQueue,
                new KafkaSourceFetcherManager(
                        elementsQueue,
                        () ->
                                new KafkaPartitionSplitReaderWrapper(
                                        readerSpecificProperties,
                                        readerContext,
                                        kafkaSourceReaderMetrics,
                                        kafkaClusterId),
                        (ignore) -> {}),
                recordEmitter,
                toConfiguration(readerSpecificProperties),
                readerContext,
                kafkaSourceReaderMetrics);
    }

    /**
     * In metadata change, we need to reset the availability helper since the number of Kafka source
     * readers could have changed.
     */
    private void completeAndResetAvailabilityHelper() {
        CompletableFuture<?> cachedPreviousFuture = availabilityHelper.getAvailableFuture();
        availabilityHelper =
                new MultipleFuturesAvailabilityHelper(
                        this.availabilityHelperSize = clusterReaderMap.size());
        syncAvailabilityHelperWithReaders();

        // We cannot immediately complete the previous future here. We must complete it only when
        // the new readers have finished handling the split assignment. Completing the future too
        // early can cause WakeupException (implicitly woken up by invocation to pollNext()) if the
        // reader has not finished resolving the positions of the splits, as seen in flaky unit test
        // errors. There is no error handling for WakeupException in SplitReader's
        // handleSplitChanges.
        availabilityHelper
                .getAvailableFuture()
                .whenComplete(
                        (ignore, t) -> {
                            restartingReaders.set(false);
                            cachedPreviousFuture.complete(null);
                        });
    }

    private void syncAvailabilityHelperWithReaders() {
        int i = 0;
        for (String kafkaClusterId : clusterReaderMap.navigableKeySet()) {
            availabilityHelper.anyOf(i, clusterReaderMap.get(kafkaClusterId).isAvailable());
            i++;
        }
    }

    private void closeAllReadersAndClearState() {
        for (Map.Entry<String, KafkaSourceReader<T>> entry : clusterReaderMap.entrySet()) {
            try {
                logger.info(
                        "Closing sub reader in reader {} for cluster: {}",
                        readerContext.getIndexOfSubtask(),
                        entry.getKey());
                entry.getValue().close();
                kafkaClusterMetricGroupManager.close(entry.getKey());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        clusterReaderMap.clear();
        clustersProperties.clear();
    }

    static Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }

    private InputStatus logAndReturnInputStatus(InputStatus inputStatus) {
        if (InputStatus.END_OF_INPUT.equals(inputStatus)) {
            logger.info(
                    "inputStatus={}, subtaskIndex={}",
                    inputStatus,
                    readerContext.getIndexOfSubtask());
        } else {
            logger.trace(
                    "inputStatus={}, subtaskIndex={}",
                    inputStatus,
                    readerContext.getIndexOfSubtask());
        }

        return inputStatus;
    }

    @VisibleForTesting
    public int getAvailabilityHelperSize() {
        return availabilityHelperSize;
    }

    @VisibleForTesting
    public boolean isActivelyConsumingSplits() {
        return isActivelyConsumingSplits;
    }
}
