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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.dynamic.source.GetMetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.MetadataUpdateEvent;
import org.apache.flink.connector.kafka.dynamic.source.RequestRetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.RetainedSplitOffsetsEvent;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.SplitAndAssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This enumerator manages multiple {@link KafkaSourceEnumerator}'s, which does not have any
 * synchronization since it assumes single threaded execution.
 */
@Internal
public class DynamicKafkaSourceEnumerator
        implements SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState> {
    private static final Logger logger =
            LoggerFactory.getLogger(DynamicKafkaSourceEnumerator.class);

    // Each cluster will have its own sub enumerator
    private final Map<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
            clusterEnumeratorMap;

    // The mapping that the split enumerator context needs to be able to forward certain requests.
    private final Map<String, StoppableKafkaEnumContextProxy> clusterEnumContextMap;
    private final SplitAssignmentStrategy splitAssignmentStrategy;
    private final KafkaStreamSubscriber kafkaStreamSubscriber;
    private final SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext;
    private final KafkaMetadataService kafkaMetadataService;
    private final Properties properties;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final Boundedness boundedness;
    private final StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory
            stoppableKafkaEnumContextProxyFactory;
    private final StoppableKafkaMetadataServiceDiscoveryContext
            kafkaMetadataServiceDiscoveryContext;
    private final ExecutorService enumeratorClosingExecutor;
    private final AtomicReference<Throwable> asynchronousEnumeratorCloseFailure;

    // options
    private final long kafkaMetadataServiceDiscoveryIntervalMs;
    private final int kafkaMetadataServiceDiscoveryFailureThreshold;
    private final long removedClusterStateRetentionMs;

    // state
    private int kafkaMetadataServiceDiscoveryFailureCount;
    private Map<String, Set<String>> latestClusterTopicsMap;
    private Set<KafkaStream> latestKafkaStreams;
    private Map<String, DynamicKafkaSourceEnumState.RetainedClusterState>
            retainedClusterEnumeratorStates;
    private final Map<String, RetainedSplitOffsetHandoff> retainedSplitOffsetHandoffs;
    private long nextRetainedSplitOffsetHandoffId;
    private boolean firstDiscoveryComplete;
    private boolean initialReaderRegistrationPending;
    private final Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader;
    private final Map<String, ReportedSplit> splitOwners = new HashMap<>();
    private final Map<String, DynamicKafkaSourceSplit> restoredAssignedSplits = new HashMap<>();
    private long lastRetentionDeadline;

    private static class RetainedSplitOffsetHandoff {
        private final long handoffId;
        private final Map<Integer, Map<String, Long>> offsetsByReader = new HashMap<>();
        @Nullable private KafkaSourceEnumState preparedState;
        // An aborted checkpoint leaves immutable reports eligible for a later completed checkpoint.
        private long eligibleCheckpointId = Long.MAX_VALUE;

        private RetainedSplitOffsetHandoff(long handoffId) {
            this.handoffId = handoffId;
        }

        private Map<String, Long> mergedOffsets() {
            Map<String, Long> offsets = new HashMap<>();
            for (Map<String, Long> reportedOffsets : offsetsByReader.values()) {
                reportedOffsets.forEach(
                        (splitId, offset) -> offsets.merge(splitId, offset, Math::max));
            }
            return offsets;
        }
    }

    public DynamicKafkaSourceEnumerator(
            KafkaStreamSubscriber kafkaStreamSubscriber,
            KafkaMetadataService kafkaMetadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            Boundedness boundedness,
            DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState) {
        this(
                kafkaStreamSubscriber,
                kafkaMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetInitializer,
                properties,
                boundedness,
                dynamicKafkaSourceEnumState,
                StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory
                        .getDefaultFactory(),
                StoppableKafkaMetadataServiceDiscoveryContext
                        .StoppableKafkaMetadataServiceDiscoveryContextFactory.getDefaultFactory());
    }

    @VisibleForTesting
    DynamicKafkaSourceEnumerator(
            KafkaStreamSubscriber kafkaStreamSubscriber,
            KafkaMetadataService kafkaMetadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            Boundedness boundedness,
            DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState,
            StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory
                    stoppableKafkaEnumContextProxyFactory) {
        this(
                kafkaStreamSubscriber,
                kafkaMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetInitializer,
                properties,
                boundedness,
                dynamicKafkaSourceEnumState,
                stoppableKafkaEnumContextProxyFactory,
                StoppableKafkaMetadataServiceDiscoveryContext
                        .StoppableKafkaMetadataServiceDiscoveryContextFactory
                        .getSplitEnumeratorContextFactory());
    }

    DynamicKafkaSourceEnumerator(
            KafkaStreamSubscriber kafkaStreamSubscriber,
            KafkaMetadataService kafkaMetadataService,
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            Boundedness boundedness,
            DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState,
            StoppableKafkaEnumContextProxy.StoppableKafkaEnumContextProxyFactory
                    stoppableKafkaEnumContextProxyFactory,
            StoppableKafkaMetadataServiceDiscoveryContext
                            .StoppableKafkaMetadataServiceDiscoveryContextFactory
                    kafkaMetadataServiceDiscoveryContextFactory) {
        this.kafkaStreamSubscriber = kafkaStreamSubscriber;
        this.boundedness = boundedness;

        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.properties = properties;
        this.enumContext = enumContext;

        // options
        this.kafkaMetadataServiceDiscoveryIntervalMs =
                DynamicKafkaSourceOptions.getOption(
                        properties,
                        DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.kafkaMetadataServiceDiscoveryFailureThreshold =
                DynamicKafkaSourceOptions.getOption(
                        properties,
                        DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD,
                        Integer::parseInt);
        this.removedClusterStateRetentionMs =
                DynamicKafkaSourceOptions.getRemovedClusterStateRetentionMs(properties);
        this.kafkaMetadataServiceDiscoveryFailureCount = 0;
        this.firstDiscoveryComplete = false;

        this.kafkaMetadataService = new SynchronizedKafkaMetadataService(kafkaMetadataService);
        this.stoppableKafkaEnumContextProxyFactory = stoppableKafkaEnumContextProxyFactory;
        this.kafkaMetadataServiceDiscoveryContext =
                kafkaMetadataServiceDiscoveryContextFactory.create(enumContext);
        this.enumeratorClosingExecutor =
                Executors.newSingleThreadExecutor(
                        runnable ->
                                createDaemonThread(
                                        runnable, "dynamic-kafka-enumerator-closing-worker"));
        this.asynchronousEnumeratorCloseFailure = new AtomicReference<>();
        this.splitAssignmentStrategy = createSplitAssignmentStrategy(properties);
        this.initialReaderRegistrationPending =
                hasRestoredEnumeratorState(dynamicKafkaSourceEnumState);
        this.pendingReportedSplitsByReader =
                restorePendingReportedSplits(
                        dynamicKafkaSourceEnumState.getPendingReportedSplitsByReader());

        if (!dynamicKafkaSourceEnumState.getClusterEnumeratorStates().isEmpty()) {
            logger.info("Dynamic Kafka source restored from checkpointed enumerator state");
        }

        // handle checkpoint state and rebuild contexts
        this.clusterEnumeratorMap = new HashMap<>();
        this.clusterEnumContextMap = new HashMap<>();
        this.latestKafkaStreams = dynamicKafkaSourceEnumState.getKafkaStreams();
        if (!this.latestKafkaStreams.isEmpty()) {
            this.latestKafkaStreams =
                    refreshRestoredClusterPropertiesFromMetadataService(this.latestKafkaStreams);
        }
        this.retainedClusterEnumeratorStates =
                removedClusterStateRetentionMs > 0
                        ? new HashMap<>(
                                dynamicKafkaSourceEnumState.getRetainedClusterEnumeratorStates())
                        : new HashMap<>();
        this.retainedSplitOffsetHandoffs = new HashMap<>();
        this.nextRetainedSplitOffsetHandoffId = 0L;
        retainedClusterEnumeratorStates.forEach(
                (cluster, retained) -> {
                    lastRetentionDeadline =
                            Math.max(lastRetentionDeadline, retained.getRetainedUntilMs());
                    rememberAssignedSplits(cluster, retained.getKafkaSourceEnumState());
                });
        dynamicKafkaSourceEnumState
                .getClusterEnumeratorStates()
                .forEach(this::rememberAssignedSplits);
        pruneExpiredRetainedClusterEnumeratorStates();

        Map<String, Properties> clusterProperties = new HashMap<>();
        Map<String, OffsetsInitializer> clusterStartingOffsets = new HashMap<>();
        Map<String, OffsetsInitializer> clusterStoppingOffsets = new HashMap<>();
        for (KafkaStream kafkaStream : latestKafkaStreams) {
            for (Entry<String, ClusterMetadata> entry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                ClusterMetadata clusterMetadata = entry.getValue();
                clusterProperties.put(entry.getKey(), clusterMetadata.getProperties());
                if (clusterMetadata.getStartingOffsetsInitializer() != null) {
                    clusterStartingOffsets.put(
                            entry.getKey(), clusterMetadata.getStartingOffsetsInitializer());
                }
                if (clusterMetadata.getStoppingOffsetsInitializer() != null) {
                    clusterStoppingOffsets.put(
                            entry.getKey(), clusterMetadata.getStoppingOffsetsInitializer());
                }
            }
        }

        this.latestClusterTopicsMap = new HashMap<>();
        Set<String> activeSplitIds = new HashSet<>();
        for (Entry<String, KafkaSourceEnumState> clusterEnumState :
                dynamicKafkaSourceEnumState.getClusterEnumeratorStates().entrySet()) {
            String clusterId = clusterEnumState.getKey();
            KafkaSourceEnumState state = clusterEnumState.getValue();
            if (!state.assignedSplits().isEmpty() || !state.unassignedSplits().isEmpty()) {
                logger.debug(
                        "Restored enumerator startup offsets for cluster {} assigned={} unassigned={}",
                        clusterId,
                        summarizeSplitOffsets(state.assignedSplits()),
                        summarizeSplitOffsets(state.unassignedSplits()));
            }
            this.latestClusterTopicsMap.put(
                    clusterId,
                    state.assignedSplits().stream()
                            .map(KafkaPartitionSplit::getTopic)
                            .collect(Collectors.toSet()));
            clusterEnumState
                    .getValue()
                    .splits()
                    .forEach(
                            splitStatus ->
                                    activeSplitIds.add(
                                            toDynamicSplitId(clusterId, splitStatus.split())));

            createEnumeratorWithAssignedTopicPartitions(
                    clusterId,
                    this.latestClusterTopicsMap.get(clusterId),
                    state,
                    clusterProperties.get(clusterId),
                    clusterStartingOffsets.get(clusterId),
                    clusterStoppingOffsets.get(clusterId));
        }
        splitAssignmentStrategy.onMetadataRefresh(activeSplitIds);
    }

    /**
     * Restores reported splits that a checkpoint captured before recovery reassignment ran. Reader
     * ids from a checkpoint taken at a different parallelism are remapped onto the current one, and
     * entries that collapse onto the same reader are merged.
     */
    private Map<Integer, List<DynamicKafkaSourceSplit>> restorePendingReportedSplits(
            Map<Integer, List<DynamicKafkaSourceSplit>> restoredPendingReportedSplits) {
        Map<Integer, List<DynamicKafkaSourceSplit>> remappedSplitsByReader = new HashMap<>();
        if (restoredPendingReportedSplits.isEmpty()) {
            return remappedSplitsByReader;
        }
        int parallelism = enumContext.currentParallelism();
        for (Entry<Integer, List<DynamicKafkaSourceSplit>> readerSplits :
                restoredPendingReportedSplits.entrySet()) {
            int readerId = Math.floorMod(readerSplits.getKey(), parallelism);
            remappedSplitsByReader
                    .computeIfAbsent(readerId, ignored -> new ArrayList<>())
                    .addAll(readerSplits.getValue());
        }
        logger.info(
                "Restored {} reported splits that were pending reassignment when the checkpoint"
                        + " was taken",
                restoredPendingReportedSplits.values().stream().mapToInt(List::size).sum());
        return remappedSplitsByReader;
    }

    private Set<KafkaStream> refreshRestoredClusterPropertiesFromMetadataService(
            Set<KafkaStream> restoredKafkaStreams) {
        Set<KafkaStream> fetchedKafkaStreams =
                kafkaStreamSubscriber.getSubscribedStreams(kafkaMetadataService);

        Map<String, Properties> fetchedClusterPropertiesById =
                extractClusterPropertiesById(fetchedKafkaStreams);
        Set<KafkaStream> mergedKafkaStreams = new HashSet<>();
        for (KafkaStream restoredKafkaStream : restoredKafkaStreams) {
            Map<String, ClusterMetadata> mergedClusterMetadataMap = new HashMap<>();
            for (Entry<String, ClusterMetadata> restoredClusterEntry :
                    restoredKafkaStream.getClusterMetadataMap().entrySet()) {
                String kafkaClusterId = restoredClusterEntry.getKey();
                ClusterMetadata restoredClusterMetadata = restoredClusterEntry.getValue();

                Properties mergedProperties = new Properties();
                Properties fetchedProperties = fetchedClusterPropertiesById.get(kafkaClusterId);
                if (fetchedProperties != null) {
                    KafkaPropertiesUtil.copyProperties(fetchedProperties, mergedProperties);
                }

                String restoredBootstrapServers =
                        restoredClusterMetadata
                                .getProperties()
                                .getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
                if (restoredBootstrapServers != null) {
                    mergedProperties.setProperty(
                            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, restoredBootstrapServers);
                }
                if (mergedProperties.isEmpty()) {
                    KafkaPropertiesUtil.copyProperties(
                            restoredClusterMetadata.getProperties(), mergedProperties);
                }

                mergedClusterMetadataMap.put(
                        kafkaClusterId,
                        new ClusterMetadata(
                                restoredClusterMetadata.getTopics(),
                                mergedProperties,
                                restoredClusterMetadata.getStartingOffsetsInitializer(),
                                restoredClusterMetadata.getStoppingOffsetsInitializer()));
            }
            mergedKafkaStreams.add(
                    new KafkaStream(restoredKafkaStream.getStreamId(), mergedClusterMetadataMap));
        }

        return mergedKafkaStreams;
    }

    private static Map<String, Properties> extractClusterPropertiesById(
            Set<KafkaStream> kafkaStreams) {
        Map<String, Properties> clusterPropertiesById = new HashMap<>();
        for (KafkaStream kafkaStream : kafkaStreams) {
            for (Entry<String, ClusterMetadata> clusterEntry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                clusterPropertiesById.put(
                        clusterEntry.getKey(), clusterEntry.getValue().getProperties());
            }
        }
        return clusterPropertiesById;
    }

    /**
     * Discover Kafka clusters and initialize sub enumerators. Bypass kafka metadata service
     * discovery if there exists prior state. Exceptions with initializing Kafka source are treated
     * the same as Kafka state and metadata inconsistency.
     */
    @Override
    public void start() {
        // if there is checkpoint state, start all enumerators first.
        if (!clusterEnumeratorMap.isEmpty()) {
            startAllEnumerators();
        }

        if (kafkaMetadataServiceDiscoveryIntervalMs <= 0) {
            logger.info("Scheduling one-time dynamic Kafka metadata refresh");
            kafkaMetadataServiceDiscoveryContext.callAsync(
                    this::fetchSubscribedKafkaStreams, this::onHandleSubscribedStreamsFetch);
        } else {
            logger.info(
                    "Scheduling dynamic Kafka metadata refresh every {} ms",
                    kafkaMetadataServiceDiscoveryIntervalMs);
            kafkaMetadataServiceDiscoveryContext.callAsync(
                    this::fetchSubscribedKafkaStreams,
                    this::onHandleSubscribedStreamsFetch,
                    0,
                    kafkaMetadataServiceDiscoveryIntervalMs);
        }
    }

    private void handleNoMoreSplits() {
        if (Boundedness.BOUNDED.equals(boundedness)) {
            boolean allEnumeratorsHaveSignalledNoMoreSplits =
                    clusterEnumContextMap.keySet().containsAll(latestClusterTopicsMap.keySet());
            for (StoppableKafkaEnumContextProxy context : clusterEnumContextMap.values()) {
                allEnumeratorsHaveSignalledNoMoreSplits =
                        allEnumeratorsHaveSignalledNoMoreSplits && context.isNoMoreSplits();
            }

            if (firstDiscoveryComplete && allEnumeratorsHaveSignalledNoMoreSplits) {
                logger.info(
                        "Signal no more splits to all readers: {}",
                        enumContext.registeredReaders().keySet());
                enumContext.registeredReaders().keySet().forEach(enumContext::signalNoMoreSplits);
            } else {
                logger.info("Not ready to notify no more splits to readers.");
            }
        }
    }

    // --------------- private methods for metadata discovery ---------------

    private Set<KafkaStream> fetchSubscribedKafkaStreams() {
        logger.debug("Fetching subscribed Kafka streams for metadata refresh");
        Set<KafkaStream> fetchedKafkaStreams =
                kafkaStreamSubscriber.getSubscribedStreams(kafkaMetadataService);
        logger.debug(
                "Fetched {} subscribed Kafka streams for metadata refresh",
                fetchedKafkaStreams.size());
        return fetchedKafkaStreams;
    }

    private static Thread createDaemonThread(Runnable runnable, String threadName) {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(true);
        return thread;
    }

    private void onHandleSubscribedStreamsFetch(Set<KafkaStream> fetchedKafkaStreams, Throwable t) {
        logger.debug("Handling subscribed Kafka streams fetched by metadata refresh");
        firstDiscoveryComplete = true;
        Set<KafkaStream> handledFetchKafkaStreams =
                handleFetchSubscribedStreamsError(fetchedKafkaStreams, t);
        pruneExpiredRetainedClusterEnumeratorStates();

        Map<String, Set<String>> newClustersTopicsMap = new HashMap<>();
        Map<String, Properties> clusterProperties = new HashMap<>();
        Map<String, OffsetsInitializer> clusterStartingOffsets = new HashMap<>();
        Map<String, OffsetsInitializer> clusterStoppingOffsets = new HashMap<>();
        for (KafkaStream kafkaStream : handledFetchKafkaStreams) {
            for (Entry<String, ClusterMetadata> entry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                String kafkaClusterId = entry.getKey();
                ClusterMetadata clusterMetadata = entry.getValue();

                newClustersTopicsMap
                        .computeIfAbsent(kafkaClusterId, (unused) -> new HashSet<>())
                        .addAll(clusterMetadata.getTopics());
                clusterProperties.put(kafkaClusterId, clusterMetadata.getProperties());
                if (clusterMetadata.getStartingOffsetsInitializer() != null) {
                    clusterStartingOffsets.put(
                            kafkaClusterId, clusterMetadata.getStartingOffsetsInitializer());
                }
                if (clusterMetadata.getStoppingOffsetsInitializer() != null) {
                    clusterStoppingOffsets.put(
                            kafkaClusterId, clusterMetadata.getStoppingOffsetsInitializer());
                }
            }
        }

        // An unchanged refresh can still unblock deferred recovery registration.
        if (latestClusterTopicsMap.equals(newClustersTopicsMap)) {
            Set<KafkaStream> activeKafkaStreams =
                    refreshActiveKafkaStreams(
                            handledFetchKafkaStreams, newClustersTopicsMap.keySet());
            boolean kafkaStreamsChanged = !latestKafkaStreams.equals(activeKafkaStreams);
            latestKafkaStreams = activeKafkaStreams;
            if (kafkaStreamsChanged) {
                sendMetadataUpdateEventToAvailableReaders();
            }
            tryCompletePendingReaderRegistration();
            if (!shouldDeferMetadataUpdateEvents()) {
                maybeStartReadyRetainedClusterEnumerators();
            }
            return;
        }

        if (logger.isInfoEnabled()) {
            // log the maps in a sorted fashion so it's easy to see the changes
            logger.info(
                    "Detected changed cluster topics after metadata refresh:\nPrevious: {}\nNew: {}",
                    new TreeMap<>(latestClusterTopicsMap),
                    new TreeMap<>(newClustersTopicsMap));
        }

        DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState;
        try {
            dynamicKafkaSourceEnumState = snapshotState(-1);
        } catch (Exception e) {
            throw new RuntimeException("unable to snapshot state in metadata change", e);
        }

        latestClusterTopicsMap = newClustersTopicsMap;
        latestKafkaStreams = handledFetchKafkaStreams;

        logger.info("Closing enumerators due to metadata change");

        retainRemovedClusterEnumeratorStates(
                dynamicKafkaSourceEnumState.getClusterEnumeratorStates(),
                latestClusterTopicsMap.keySet());
        // A topology change starts a fresh report/fence round. Previously frozen offsets remain
        // in reader state; no report may authorize a different topology's handoff.
        retainedSplitOffsetHandoffs.clear();
        splitOwners.values().removeIf(owner -> !isKnownSplit(owner.split));
        sendMetadataUpdateEventToAvailableReaders();
        closeAllEnumeratorsAndContexts();

        // create enumerators
        Set<String> activeSplitIds = new HashSet<>();
        for (Entry<String, Set<String>> activeClusterTopics : latestClusterTopicsMap.entrySet()) {
            String kafkaClusterId = activeClusterTopics.getKey();
            KafkaSourceEnumState kafkaSourceEnumState =
                    dynamicKafkaSourceEnumState.getClusterEnumeratorStates().get(kafkaClusterId);
            if (kafkaSourceEnumState == null
                    && retainedClusterEnumeratorStates.containsKey(kafkaClusterId)) {
                continue;
            }
            if (kafkaSourceEnumState != null) {
                retainedClusterEnumeratorStates.remove(kafkaClusterId);
                retainedSplitOffsetHandoffs.remove(kafkaClusterId);
            }

            final KafkaSourceEnumState newKafkaSourceEnumState;
            if (kafkaSourceEnumState != null) {
                Set<SplitAndAssignmentStatus> partitions =
                        filterStateByTopics(kafkaSourceEnumState, activeClusterTopics.getValue());
                partitions.forEach(
                        splitStatus ->
                                activeSplitIds.add(
                                        toDynamicSplitId(kafkaClusterId, splitStatus.split())));
                newKafkaSourceEnumState =
                        new KafkaSourceEnumState(
                                partitions, kafkaSourceEnumState.initialDiscoveryFinished());
            } else {
                newKafkaSourceEnumState = new KafkaSourceEnumState(Collections.emptySet(), false);
            }

            // Restart the enumerator from active topic partitions already known in state. Retained
            // clusters are started separately after their reader offsets have been handed off.
            createEnumeratorWithAssignedTopicPartitions(
                    kafkaClusterId,
                    activeClusterTopics.getValue(),
                    newKafkaSourceEnumState,
                    clusterProperties.get(kafkaClusterId),
                    clusterStartingOffsets.get(kafkaClusterId),
                    clusterStoppingOffsets.get(kafkaClusterId));
        }

        activeSplitIds.retainAll(splitOwners.keySet());
        splitAssignmentStrategy.onMetadataRefresh(activeSplitIds);
        startAllEnumerators();
        tryCompletePendingReaderRegistration();
        if (!shouldDeferMetadataUpdateEvents()) {
            maybeStartReadyRetainedClusterEnumerators();
        }
    }

    private Set<KafkaStream> refreshActiveKafkaStreams(
            Set<KafkaStream> fetchedKafkaStreams, Set<String> activeKafkaClusterIds) {
        Map<String, Properties> previousClusterPropertiesById =
                extractClusterPropertiesById(latestKafkaStreams);
        Set<KafkaStream> activeKafkaStreams = new HashSet<>();
        for (KafkaStream kafkaStream : fetchedKafkaStreams) {
            Map<String, ClusterMetadata> activeClusterMetadata = new HashMap<>();
            for (Entry<String, ClusterMetadata> clusterMetadata :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                if (activeKafkaClusterIds.contains(clusterMetadata.getKey())) {
                    Properties mergedProperties = new Properties();
                    KafkaPropertiesUtil.copyProperties(
                            clusterMetadata.getValue().getProperties(), mergedProperties);
                    Properties previousProperties =
                            previousClusterPropertiesById.get(clusterMetadata.getKey());
                    if (previousProperties != null) {
                        String previousBootstrapServers =
                                previousProperties.getProperty(
                                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
                        if (previousBootstrapServers != null) {
                            mergedProperties.setProperty(
                                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                    previousBootstrapServers);
                        }
                    }
                    activeClusterMetadata.put(
                            clusterMetadata.getKey(),
                            new ClusterMetadata(
                                    clusterMetadata.getValue().getTopics(),
                                    mergedProperties,
                                    clusterMetadata.getValue().getStartingOffsetsInitializer(),
                                    clusterMetadata.getValue().getStoppingOffsetsInitializer()));
                }
            }
            if (!activeClusterMetadata.isEmpty()) {
                activeKafkaStreams.add(
                        new KafkaStream(kafkaStream.getStreamId(), activeClusterMetadata));
            }
        }
        return activeKafkaStreams;
    }

    private Set<KafkaStream> handleFetchSubscribedStreamsError(
            Set<KafkaStream> fetchedKafkaStreams, @Nullable Throwable t) {
        if (t != null) {
            if (!latestKafkaStreams.isEmpty()
                    && ++kafkaMetadataServiceDiscoveryFailureCount
                            <= kafkaMetadataServiceDiscoveryFailureThreshold) {
                logger.warn("Swallowing metadata service error", t);
                // reuse state
                return latestKafkaStreams;
            } else {
                throw new RuntimeException(
                        "Fetching subscribed Kafka streams failed and no metadata to fallback", t);
            }
        } else {
            // reset count in absence of failure
            kafkaMetadataServiceDiscoveryFailureCount = 0;
            return fetchedKafkaStreams;
        }
    }

    /** NOTE: Must run on coordinator thread. */
    private void sendMetadataUpdateEventToAvailableReaders() {
        if (shouldDeferMetadataUpdateEvents()) {
            return;
        }

        for (int readerId : enumContext.registeredReaders().keySet()) {
            sendMetadataUpdateEvent(readerId);
        }
    }

    /**
     * Initialize KafkaEnumerators, maybe with the topic partitions that are already assigned to by
     * readers, to avoid duplicate re-assignment of splits. This is especially important in the
     * restart mechanism when duplicate split assignment can cause undesired starting offsets (e.g.
     * not assigning to the offsets prior to reader restart). Split offset resolution is mostly
     * managed by the readers.
     *
     * <p>NOTE: Must run on coordinator thread
     */
    private KafkaSourceEnumerator createEnumeratorWithAssignedTopicPartitions(
            String kafkaClusterId,
            Set<String> topics,
            KafkaSourceEnumState kafkaSourceEnumState,
            Properties fetchedProperties,
            @Nullable OffsetsInitializer clusterStartingOffsetsInitializer,
            @Nullable OffsetsInitializer clusterStoppingOffsetsInitializer) {
        OffsetsInitializer effectiveStartingOffsetsInitializer =
                clusterStartingOffsetsInitializer != null
                        ? clusterStartingOffsetsInitializer
                        : startingOffsetsInitializer;
        OffsetsInitializer effectiveStoppingOffsetsInitializer =
                clusterStoppingOffsetsInitializer != null
                        ? clusterStoppingOffsetsInitializer
                        : stoppingOffsetInitializer;

        final Runnable signalNoMoreSplitsCallback;
        if (Boundedness.BOUNDED.equals(boundedness)) {
            signalNoMoreSplitsCallback = this::handleNoMoreSplits;
        } else {
            signalNoMoreSplitsCallback = null;
        }

        StoppableKafkaEnumContextProxy context =
                stoppableKafkaEnumContextProxyFactory.create(
                        enumContext,
                        kafkaClusterId,
                        kafkaMetadataService,
                        signalNoMoreSplitsCallback);
        context.setAssignmentListener(this::recordAssignments);
        KafkaSourceEnumerator.SplitOwnerSelector splitOwnerSelector =
                splitAssignmentStrategy.createSplitOwnerSelector(kafkaClusterId);

        Properties consumerProps = new Properties();
        KafkaPropertiesUtil.copyProperties(fetchedProperties, consumerProps);
        KafkaPropertiesUtil.copyProperties(properties, consumerProps);
        DynamicKafkaSourceOptions.removeRemovedClusterRetentionOption(consumerProps);
        KafkaPropertiesUtil.setClientIdPrefix(consumerProps, kafkaClusterId);
        OffsetResetStrategy effectiveOffsetResetStrategy =
                KafkaPropertiesUtil.resolveAutoOffsetResetStrategy(
                        properties, fetchedProperties, effectiveStartingOffsetsInitializer);
        consumerProps.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                effectiveOffsetResetStrategy.name().toLowerCase());

        KafkaSourceEnumerator enumerator =
                new KafkaSourceEnumerator(
                        KafkaSubscriber.getTopicListSubscriber(new ArrayList<>(topics)),
                        effectiveStartingOffsetsInitializer,
                        effectiveStoppingOffsetsInitializer,
                        consumerProps,
                        context,
                        boundedness,
                        kafkaSourceEnumState,
                        splitOwnerSelector);

        clusterEnumContextMap.put(kafkaClusterId, context);
        clusterEnumeratorMap.put(kafkaClusterId, enumerator);

        return enumerator;
    }

    private void startAllEnumerators() {
        if (initialReaderRegistrationPending) {
            return;
        }
        for (String kafkaClusterId : latestClusterTopicsMap.keySet()) {
            if (clusterEnumeratorMap.containsKey(kafkaClusterId)) {
                startEnumerator(kafkaClusterId);
            }
        }
    }

    private void startEnumerator(String kafkaClusterId) {
        try {
            // starts enumerators and handles split discovery and assignment
            clusterEnumeratorMap.get(kafkaClusterId).start();
        } catch (KafkaException e) {
            if (retainedClusterEnumeratorStates.containsKey(kafkaClusterId)
                    || splitOwners.values().stream()
                            .anyMatch(
                                    owner -> owner.split.getKafkaClusterId().equals(kafkaClusterId))
                    || kafkaMetadataService.isClusterActive(kafkaClusterId)) {
                throw new RuntimeException(
                        String.format("Failed to create enumerator for %s", kafkaClusterId), e);
            } else {
                logger.info(
                        "Found inactive cluster {} while initializing, removing enumerator",
                        kafkaClusterId,
                        e);
                try {
                    clusterEnumContextMap.remove(kafkaClusterId).close();
                    clusterEnumeratorMap.remove(kafkaClusterId).close();
                } catch (Exception ex) {
                    // closing enumerator throws an exception, let error propagate and restart
                    // the job
                    throw new RuntimeException(
                            "Failed to close enum context for " + kafkaClusterId, ex);
                }
            }
        }
    }

    private void closeAllEnumeratorsAndContexts() {
        Map<String, StoppableKafkaEnumContextProxy> closingClusterEnumContextMap =
                new HashMap<>(clusterEnumContextMap);
        Map<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
                closingClusterEnumeratorMap = new HashMap<>(clusterEnumeratorMap);
        closingClusterEnumContextMap
                .values()
                .forEach(StoppableKafkaEnumContextProxy::prepareForClose);
        clusterEnumContextMap.clear();
        clusterEnumeratorMap.clear();

        enumeratorClosingExecutor.execute(
                () ->
                        closeEnumeratorsAndContexts(
                                closingClusterEnumContextMap, closingClusterEnumeratorMap));
    }

    private void closeEnumeratorsAndContexts(
            Map<String, StoppableKafkaEnumContextProxy> closingClusterEnumContextMap,
            Map<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
                    closingClusterEnumeratorMap) {
        closingClusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> {
                    try {
                        closingClusterEnumContextMap.get(cluster).close();
                        subEnumerator.close();
                    } catch (Exception e) {
                        handleAsynchronousEnumeratorCloseFailure(e);
                    }
                });
    }

    private void handleAsynchronousEnumeratorCloseFailure(Exception e) {
        asynchronousEnumeratorCloseFailure.compareAndSet(null, e);
        try {
            enumContext.runInCoordinatorThread(
                    () -> {
                        throw new RuntimeException(e);
                    });
        } catch (Throwable coordinatorFailure) {
            logger.warn(
                    "Unable to propagate asynchronous dynamic Kafka enumerator close failure to "
                            + "the coordinator thread. The failure will be rethrown during close.",
                    coordinatorFailure);
        }
    }

    /**
     * Multi cluster Kafka source readers will not request splits. Splits will be pushed to them,
     * similarly for the sub enumerators.
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        throw new UnsupportedOperationException("Kafka enumerators only assign splits to readers.");
    }

    @Override
    public void addSplitsBack(List<DynamicKafkaSourceSplit> splits, int subtaskId) {
        logger.debug("Adding splits back for {}", subtaskId);
        // Even an empty return invalidates reports from the failed execution attempt.
        invalidateRetainedSplitOffsetHandoffsOnReaderRegistration();
        pendingReportedSplitsByReader.put(
                subtaskId,
                mergeReportedSplits(pendingReportedSplitsByReader.get(subtaskId), splits));
        handleNoMoreSplits();
    }

    private void addSplitsBackToClusterEnumerators(
            List<DynamicKafkaSourceSplit> splits, int subtaskId) {
        // separate splits by cluster
        Map<String, List<KafkaPartitionSplit>> kafkaPartitionSplits = new HashMap<>();
        for (DynamicKafkaSourceSplit split : splits) {
            kafkaPartitionSplits
                    .computeIfAbsent(split.getKafkaClusterId(), unused -> new ArrayList<>())
                    .add(split.getKafkaPartitionSplit());
        }

        // add splits back and assign pending splits for all enumerators
        for (String kafkaClusterId : kafkaPartitionSplits.keySet()) {
            if (clusterEnumeratorMap.containsKey(kafkaClusterId)) {
                clusterEnumeratorMap
                        .get(kafkaClusterId)
                        .addSplitsBack(kafkaPartitionSplits.get(kafkaClusterId), subtaskId);
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Cannot reassign split for active cluster %s because its"
                                        + " enumerator is unavailable",
                                kafkaClusterId));
            }
        }
    }

    /** NOTE: this happens at startup and failover. */
    @Override
    public void addReader(int subtaskId) {
        logger.debug("Adding reader {}", subtaskId);
        ReaderInfo readerInfo = enumContext.registeredReaders().get(subtaskId);
        if (readerInfo != null) {
            List<DynamicKafkaSourceSplit> reportedSplits =
                    readerInfo.getReportedSplitsOnRegistration();
            if (!reportedSplits.isEmpty()) {
                pendingReportedSplitsByReader.put(
                        subtaskId,
                        mergeReportedSplits(
                                pendingReportedSplitsByReader.get(subtaskId), reportedSplits));
            }
        }

        boolean invalidatedRetainedSplitOffsetHandoff =
                invalidateRetainedSplitOffsetHandoffsOnReaderRegistration();
        if (tryCompletePendingReaderRegistration()) {
            return;
        }

        addReaderToClusterEnumerators(subtaskId);
        if (invalidatedRetainedSplitOffsetHandoff) {
            sendMetadataUpdateEvent(subtaskId);
            maybeStartReadyRetainedClusterEnumerators();
        }
        handleNoMoreSplits();
    }

    /**
     * Merges a registering reader's report with any pending entry for the same reader id. The
     * pending entry can come from an earlier registration or from remapping checkpointed splits
     * after rescaling. Merging by split id, preferring the current report, avoids both losing and
     * duplicating splits.
     */
    private static List<DynamicKafkaSourceSplit> mergeReportedSplits(
            @Nullable List<DynamicKafkaSourceSplit> previousReportedSplits,
            List<DynamicKafkaSourceSplit> reportedSplits) {
        Map<Entry<String, Long>, DynamicKafkaSourceSplit> merged = new LinkedHashMap<>();
        List<DynamicKafkaSourceSplit> all = new ArrayList<>();
        if (previousReportedSplits != null) {
            all.addAll(previousReportedSplits);
        }
        all.addAll(reportedSplits);
        for (DynamicKafkaSourceSplit split : all) {
            // Validate different retention epochs before deduplication. For active progress the
            // new registration can legitimately roll back an uncheckpointed assignment.
            merged.merge(
                    new AbstractMap.SimpleImmutableEntry<>(
                            split.splitId(), split.getRetainedUntilMs()),
                    split,
                    (old, current) ->
                            current.isRetained()
                                            && old.getStartingOffset() > current.getStartingOffset()
                                    ? old
                                    : current);
        }
        return new ArrayList<>(merged.values());
    }

    private boolean tryCompletePendingReaderRegistration() {
        boolean hasPendingRecovery =
                initialReaderRegistrationPending || !pendingReportedSplitsByReader.isEmpty();
        if (!hasPendingRecovery) {
            return false;
        }
        if (!firstDiscoveryComplete
                || (initialReaderRegistrationPending && !allReadersRegistered())) {
            return true;
        }

        if (initialReaderRegistrationPending) {
            reassignReportedSplits();
            initialReaderRegistrationPending = false;
            restoredAssignedSplits.clear();
            startAllEnumerators();
        } else {
            restoreFailedReaders();
        }
        flushPendingSplitAssignmentsForRegisteredReaders();
        handleNoMoreSplits();
        // The last registering reader may not have requested metadata yet. Authorize retained
        // state on every reader before a handoff can ask any reader to report its offsets.
        sendMetadataUpdateEventToAvailableReaders();
        maybeStartReadyRetainedClusterEnumerators();
        return true;
    }

    private boolean allReadersRegistered() {
        return enumContext.registeredReaders().size() == enumContext.currentParallelism();
    }

    private void addReaderToClusterEnumerators(int subtaskId) {
        clusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> subEnumerator.addReader(subtaskId));
    }

    private void flushPendingSplitAssignmentsForRegisteredReaders() {
        List<Integer> registeredReaders = new ArrayList<>(enumContext.registeredReaders().keySet());
        Collections.sort(registeredReaders);
        for (int readerId : registeredReaders) {
            addReaderToClusterEnumerators(readerId);
        }
    }

    private void reassignReportedSplits() {
        // Full restore reconciles a completed checkpoint. Older writers calculated reader
        // deadlines independently, so normalize known retained reports here without a format tag.
        // Live handoff and local recovery still require the current coordinator retention epoch.
        Map<String, ReportedSplit> reports = new TreeMap<>();
        for (Entry<Integer, List<DynamicKafkaSourceSplit>> batch :
                new TreeMap<>(pendingReportedSplitsByReader).entrySet()) {
            for (DynamicKafkaSourceSplit split : batch.getValue()) {
                DynamicKafkaSourceSplit normalized =
                        retainedClusterEnumeratorStates.containsKey(split.getKafkaClusterId())
                                ? getRetainedReportedSplit(split)
                                : isSplitActive(split)
                                                && (!split.isRetained()
                                                        || restoredAssignedSplits.containsKey(
                                                                split.splitId()))
                                        ? split.clearRetention()
                                        : null;
                if (normalized == null) {
                    continue;
                }
                // Every reader is restoring the same completed checkpoint. A returning cluster
                // can join ordinary recovery; only a live handoff needs another checkpoint fence.
                if (isSplitActive(normalized)) {
                    normalized = normalized.clearRetention();
                }
                ReportedSplit previous = reports.get(split.splitId());
                if (previous != null) {
                    Preconditions.checkState(
                            previous.retained || split.isRetained(),
                            "Split %s was reported by both reader %s and reader %s",
                            split.splitId(),
                            previous.readerId,
                            batch.getKey());
                    // Full recovery has all reader reports: active progress wins over a shadow;
                    // validated dormant copies from the same lifecycle keep the furthest offset.
                    if (!previous.retained
                            || (split.isRetained()
                                    && previous.split.getStartingOffset()
                                            >= normalized.getStartingOffset())) {
                        continue;
                    }
                }
                reports.put(
                        split.splitId(),
                        new ReportedSplit(normalized, batch.getKey(), split.isRetained()));
            }
        }
        for (DynamicKafkaSourceSplit expected : restoredAssignedSplits.values()) {
            Preconditions.checkState(
                    !isKnownSplit(expected)
                            || expected.getStoppingOffset().isPresent()
                            || reports.containsKey(expected.splitId()),
                    "Missing restored reader offset for assigned split %s",
                    expected.splitId());
        }
        for (String cluster : new HashSet<>(retainedClusterEnumeratorStates.keySet())) {
            if (latestClusterTopicsMap.containsKey(cluster)) {
                KafkaSourceEnumState retainedState =
                        retainedClusterEnumeratorStates.remove(cluster).getKafkaSourceEnumState();
                createClusterEnumerator(
                        cluster,
                        new KafkaSourceEnumState(
                                filterStateByTopics(
                                        retainedState, latestClusterTopicsMap.get(cluster)),
                                retainedState.initialDiscoveryFinished()));
            }
        }
        splitOwners.clear();
        List<DynamicKafkaSourceSplit> active =
                reports.values().stream()
                        .map(report -> report.split)
                        .filter(split -> !split.isRetained())
                        .collect(Collectors.toList());
        splitAssignmentStrategy.onRecoveredSplits(active, enumContext.currentParallelism());
        Map<Integer, List<DynamicKafkaSourceSplit>> dormant = new TreeMap<>();
        for (ReportedSplit report : reports.values()) {
            if (report.split.isRetained()) {
                dormant.computeIfAbsent(report.readerId, ignored -> new ArrayList<>())
                        .add(report.split);
            } else {
                addSplitsBackToClusterEnumerators(
                        Collections.singletonList(report.split), report.readerId);
            }
        }
        flushPendingSplitAssignmentsForRegisteredReaders();
        assignOwnedSplits(dormant);
        pendingReportedSplitsByReader.clear();
    }

    private boolean shouldDeferMetadataUpdateEvents() {
        return initialReaderRegistrationPending
                || (!pendingReportedSplitsByReader.isEmpty() && !allReadersRegistered());
    }

    private void sendMetadataUpdateEvent(int readerId) {
        Map<String, Long> retainedDeadlines = new HashMap<>();
        retainedClusterEnumeratorStates.forEach(
                (cluster, state) -> retainedDeadlines.put(cluster, state.getRetainedUntilMs()));
        MetadataUpdateEvent metadataUpdateEvent =
                new MetadataUpdateEvent(latestKafkaStreams, retainedDeadlines);
        logger.debug("sending metadata update to reader {}: {}", readerId, metadataUpdateEvent);
        enumContext.sendEventToSourceReader(readerId, metadataUpdateEvent);
    }

    private boolean isSplitActive(DynamicKafkaSourceSplit split) {
        return latestClusterTopicsMap
                .getOrDefault(split.getKafkaClusterId(), Collections.emptySet())
                .contains(split.getTopic());
    }

    @Nullable
    private DynamicKafkaSourceSplit getRetainedReportedSplit(DynamicKafkaSourceSplit split) {
        DynamicKafkaSourceEnumState.RetainedClusterState retained =
                retainedClusterEnumeratorStates.get(split.getKafkaClusterId());
        if (retained == null || !retained.containsPartition(split.getTopicPartition())) {
            return null;
        }
        // If an earlier checkpoint still contains a previous retention epoch, a subsequent
        // checkpoint-fenced handoff is returned by Flink as an uncheckpointed active assignment.
        // Its offset, rather than the old retained shadow, authorizes local recovery.
        if (split.isRetained()
                && split.getRetainedUntilMs() != retained.getRetainedUntilMs()
                && !initialReaderRegistrationPending) {
            return null;
        }
        return split.retainUntil(retained.getRetainedUntilMs());
    }

    private boolean isKnownSplit(DynamicKafkaSourceSplit split) {
        DynamicKafkaSourceEnumState.RetainedClusterState retained =
                retainedClusterEnumeratorStates.get(split.getKafkaClusterId());
        return isSplitActive(split)
                || (retained != null && retained.containsPartition(split.getTopicPartition()));
    }

    private void rememberAssignedSplits(String cluster, KafkaSourceEnumState state) {
        state.assignedSplits()
                .forEach(
                        split ->
                                restoredAssignedSplits.put(
                                        toDynamicSplitId(cluster, split),
                                        new DynamicKafkaSourceSplit(cluster, split)));
    }

    private void recordAssignments(SplitsAssignment<DynamicKafkaSourceSplit> assignment) {
        assignment
                .assignment()
                .forEach(
                        (reader, splits) ->
                                splits.forEach(
                                        split -> {
                                            ReportedSplit previous =
                                                    splitOwners.get(split.splitId());
                                            Preconditions.checkState(
                                                    previous == null || previous.readerId == reader,
                                                    "Cannot transfer split %s from live reader %s to %s",
                                                    split.splitId(),
                                                    previous == null ? null : previous.readerId,
                                                    reader);
                                            splitOwners.put(
                                                    split.splitId(),
                                                    new ReportedSplit(
                                                            split, reader, split.isRetained()));
                                        }));
    }

    private void assignOwnedSplits(Map<Integer, List<DynamicKafkaSourceSplit>> splits) {
        if (!splits.isEmpty()) {
            SplitsAssignment<DynamicKafkaSourceSplit> assignment = new SplitsAssignment<>(splits);
            recordAssignments(assignment);
            enumContext.assignSplits(assignment);
        }
    }

    private Map<String, Integer> activeOwners() {
        Map<String, Integer> active = new HashMap<>();
        splitOwners.forEach(
                (id, owner) -> {
                    if (isSplitActive(owner.split)
                            && !retainedClusterEnumeratorStates.containsKey(
                                    owner.split.getKafkaClusterId())) {
                        active.put(id, owner.readerId);
                    }
                });
        return active;
    }

    private void restoreFailedReaders() {
        Map<Integer, List<DynamicKafkaSourceSplit>> assignments = new TreeMap<>();
        for (Entry<Integer, List<DynamicKafkaSourceSplit>> report :
                new TreeMap<>(pendingReportedSplitsByReader).entrySet()) {
            int reader = report.getKey();
            if (!enumContext.registeredReaders().containsKey(reader)) {
                continue;
            }
            Map<String, DynamicKafkaSourceSplit> valid = new TreeMap<>();
            for (DynamicKafkaSourceSplit split : report.getValue()) {
                ReportedSplit owner = splitOwners.get(split.splitId());
                if (owner != null && owner.readerId != reader) {
                    continue;
                }
                DynamicKafkaSourceSplit normalized =
                        retainedClusterEnumeratorStates.containsKey(split.getKafkaClusterId())
                                ? getRetainedReportedSplit(split)
                                : isSplitActive(split) && !split.isRetained() ? split : null;
                if (normalized != null) {
                    valid.merge(
                            split.splitId(),
                            normalized,
                            (old, current) ->
                                    current.isRetained()
                                                    && old.getStartingOffset()
                                                            > current.getStartingOffset()
                                            ? old
                                            : current);
                }
            }
            for (ReportedSplit owner : splitOwners.values()) {
                Preconditions.checkState(
                        owner.readerId != reader
                                || !isKnownSplit(owner.split)
                                || owner.split.getStoppingOffset().isPresent()
                                || valid.containsKey(owner.split.splitId()),
                        "Missing restored offset for owned split %s",
                        owner.split.splitId());
            }
            if (!valid.isEmpty()) {
                assignments.put(reader, new ArrayList<>(valid.values()));
            }
            pendingReportedSplitsByReader.remove(reader);
        }
        assignOwnedSplits(assignments);
    }

    private boolean isClusterActive(String kafkaClusterId) {
        for (KafkaStream kafkaStream : latestKafkaStreams) {
            if (kafkaStream.getClusterMetadataMap().containsKey(kafkaClusterId)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasRestoredEnumeratorState(
            DynamicKafkaSourceEnumState dynamicKafkaSourceEnumState) {
        return !dynamicKafkaSourceEnumState.getClusterEnumeratorStates().isEmpty()
                || !dynamicKafkaSourceEnumState.getRetainedClusterEnumeratorStates().isEmpty()
                || !dynamicKafkaSourceEnumState.getPendingReportedSplitsByReader().isEmpty();
    }

    private static class ReportedSplit {
        private final DynamicKafkaSourceSplit split;
        private final int readerId;
        private final boolean retained;

        private ReportedSplit(DynamicKafkaSourceSplit split, int readerId, boolean retained) {
            this.split = split;
            this.readerId = readerId;
            this.retained = retained;
        }
    }

    private boolean invalidateRetainedSplitOffsetHandoffsOnReaderRegistration() {
        if (retainedSplitOffsetHandoffs.isEmpty()) {
            return false;
        }

        // A reader can join while a re-added cluster is waiting for offset handoff. Restart the
        // attempt so delayed responses from the reader's previous attempt cannot count as its
        // replacement report.
        retainedSplitOffsetHandoffs.values().forEach(handoff -> handoff.offsetsByReader.clear());
        retainedSplitOffsetHandoffs.clear();
        return true;
    }

    /**
     * Besides for checkpointing, this method is used in the restart sequence to retain the relevant
     * assigned splits so that there is no reader duplicate split assignment. See {@link
     * #createEnumeratorWithAssignedTopicPartitions(String, Set, KafkaSourceEnumState, Properties,
     * OffsetsInitializer, OffsetsInitializer)}}
     */
    @Override
    public DynamicKafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        Preconditions.checkState(
                checkpointId < 0 || allReadersRegistered(),
                "Cannot checkpoint recovery before all reader state reports arrive");
        pruneExpiredRetainedClusterEnumeratorStates();
        if (checkpointId >= 0) {
            retainedSplitOffsetHandoffs.values().stream()
                    .filter(handoff -> handoff.preparedState != null)
                    .forEach(
                            handoff ->
                                    handoff.eligibleCheckpointId =
                                            Math.min(handoff.eligibleCheckpointId, checkpointId));
        }
        Map<String, KafkaSourceEnumState> subEnumeratorStateByCluster = new HashMap<>();
        boolean isCheckpointSnapshot = checkpointId >= 0;

        // populate map for all assigned splits
        for (Entry<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
                clusterEnumerator : clusterEnumeratorMap.entrySet()) {
            KafkaSourceEnumState state = clusterEnumerator.getValue().snapshotState(checkpointId);
            subEnumeratorStateByCluster.put(clusterEnumerator.getKey(), state);
            if (isCheckpointSnapshot) {
                logger.debug(
                        "Checkpoint {} cluster {} enumerator startup offsets for assigned splits {}",
                        checkpointId,
                        clusterEnumerator.getKey(),
                        summarizeSplitOffsets(state.assignedSplits()));
                logger.debug(
                        "Checkpoint {} cluster {} enumerator startup offsets for unassigned splits {}",
                        checkpointId,
                        clusterEnumerator.getKey(),
                        summarizeSplitOffsets(state.unassignedSplits()));
            }
        }

        if (isCheckpointSnapshot && !pendingReportedSplitsByReader.isEmpty()) {
            logger.debug(
                    "Checkpoint {} includes pending reported splits of readers {}",
                    checkpointId,
                    pendingReportedSplitsByReader.keySet());
        }

        // See DynamicKafkaSourceEnumState#getPendingReportedSplitsByReader() for why the
        // pending splits are checkpointed.
        return new DynamicKafkaSourceEnumState(
                latestKafkaStreams,
                subEnumeratorStateByCluster,
                new HashMap<>(retainedClusterEnumeratorStates),
                new HashMap<>(pendingReportedSplitsByReader));
    }

    private void retainRemovedClusterEnumeratorStates(
            Map<String, KafkaSourceEnumState> activeClusterEnumeratorStates,
            Set<String> activeKafkaClusterIds) {
        if (removedClusterStateRetentionMs <= 0) {
            return;
        }

        long retainedUntilMs =
                Math.max(
                        System.currentTimeMillis() + removedClusterStateRetentionMs,
                        lastRetentionDeadline + 1);
        lastRetentionDeadline = retainedUntilMs;
        for (Entry<String, KafkaSourceEnumState> entry : activeClusterEnumeratorStates.entrySet()) {
            if (activeKafkaClusterIds.contains(entry.getKey())) {
                continue;
            }
            retainedClusterEnumeratorStates.put(
                    entry.getKey(),
                    new DynamicKafkaSourceEnumState.RetainedClusterState(
                            entry.getValue(), retainedUntilMs));
            retainedSplitOffsetHandoffs.remove(entry.getKey());
        }
    }

    private void pruneExpiredRetainedClusterEnumeratorStates() {
        if (removedClusterStateRetentionMs <= 0) {
            retainedClusterEnumeratorStates.clear();
            retainedSplitOffsetHandoffs.clear();
            return;
        }

        long currentTimeMillis = System.currentTimeMillis();
        Set<String> expired =
                retainedClusterEnumeratorStates.entrySet().stream()
                        .filter(
                                entry ->
                                        entry.getValue().getRetainedUntilMs() <= currentTimeMillis
                                                && !isClusterActive(entry.getKey()))
                        .map(Entry::getKey)
                        .collect(Collectors.toSet());
        if (expired.isEmpty()) {
            return;
        }
        expired.forEach(retainedClusterEnumeratorStates::remove);
        splitOwners.values().removeIf(owner -> expired.contains(owner.split.getKafkaClusterId()));
        restoredAssignedSplits
                .values()
                .removeIf(split -> expired.contains(split.getKafkaClusterId()));
        retainedSplitOffsetHandoffs.keySet().retainAll(retainedClusterEnumeratorStates.keySet());
        sendMetadataUpdateEventToAvailableReaders();
    }

    private boolean isRetainedClusterReadyForAssignment(
            String kafkaClusterId,
            DynamicKafkaSourceEnumState.RetainedClusterState retainedClusterState) {
        Set<String> activeTopics =
                latestClusterTopicsMap.getOrDefault(kafkaClusterId, Collections.emptySet());
        return filterStateByTopics(retainedClusterState.getKafkaSourceEnumState(), activeTopics)
                .stream()
                .noneMatch(
                        splitStatus ->
                                splitStatus.assignmentStatus().equals(AssignmentStatus.ASSIGNED));
    }

    private void startRetainedSplitOffsetHandoff(String kafkaClusterId) {
        if (retainedSplitOffsetHandoffs.containsKey(kafkaClusterId)) {
            return;
        }

        // Flink fails the task if a source event is lost. Keep a slow live attempt intact;
        // reader registration and topology changes restart the handoff when necessary.
        RetainedSplitOffsetHandoff handoff =
                new RetainedSplitOffsetHandoff(++nextRetainedSplitOffsetHandoffId);
        retainedSplitOffsetHandoffs.put(kafkaClusterId, handoff);
        for (int readerId : enumContext.registeredReaders().keySet()) {
            sendRetainedSplitOffsetRequestToReader(kafkaClusterId, handoff, readerId);
        }
    }

    private void sendPendingRetainedSplitOffsetRequestsToReader(int readerId) {
        retainedSplitOffsetHandoffs.forEach(
                (kafkaClusterId, handoff) ->
                        sendRetainedSplitOffsetRequestToReader(kafkaClusterId, handoff, readerId));
    }

    private void sendRetainedSplitOffsetRequestToReader(
            String kafkaClusterId, RetainedSplitOffsetHandoff handoff, int readerId) {
        RequestRetainedSplitOffsetsEvent requestEvent =
                new RequestRetainedSplitOffsetsEvent(handoff.handoffId, kafkaClusterId);
        logger.debug(
                "Requesting retained split offsets from reader {}: {}", readerId, requestEvent);
        enumContext.sendEventToSourceReader(readerId, requestEvent);
    }

    private void handleRetainedSplitOffsetsEvent(
            int subtaskId, RetainedSplitOffsetsEvent retainedSplitOffsetsEvent) {
        if (!enumContext.registeredReaders().containsKey(subtaskId)) {
            logger.debug("Ignoring retained split offsets from unavailable reader {}", subtaskId);
            return;
        }
        pruneExpiredRetainedClusterEnumeratorStates();
        String kafkaClusterId = retainedSplitOffsetsEvent.getKafkaClusterId();
        RetainedSplitOffsetHandoff handoff = retainedSplitOffsetHandoffs.get(kafkaClusterId);
        if (handoff == null || handoff.handoffId != retainedSplitOffsetsEvent.getHandoffId()) {
            logger.debug(
                    "Ignoring stale retained split offsets from reader {}: {}",
                    subtaskId,
                    retainedSplitOffsetsEvent);
            return;
        }
        if (handoff.preparedState != null) {
            return;
        }

        handoff.offsetsByReader.putIfAbsent(
                subtaskId, new HashMap<>(retainedSplitOffsetsEvent.getRetainedSplitOffsets()));
        if (handoff.offsetsByReader.size() >= enumContext.currentParallelism()
                && handoff.offsetsByReader
                        .keySet()
                        .equals(enumContext.registeredReaders().keySet())) {
            handoff.preparedState =
                    prepareRetainedClusterState(kafkaClusterId, handoff.mergedOffsets());
            handoff.offsetsByReader.clear();
        }
        maybeStartReadyRetainedClusterEnumerators();
    }

    /** Prepares the handoff without changing the state captured by an in-flight checkpoint. */
    private KafkaSourceEnumState prepareRetainedClusterState(
            String kafkaClusterId, Map<String, Long> retainedSplitOffsets) {
        KafkaSourceEnumState kafkaSourceEnumState =
                retainedClusterEnumeratorStates.get(kafkaClusterId).getKafkaSourceEnumState();
        Set<String> activeTopics =
                latestClusterTopicsMap.getOrDefault(kafkaClusterId, Collections.emptySet());
        Set<SplitAndAssignmentStatus> updatedSplits = new HashSet<>();
        for (SplitAndAssignmentStatus splitStatus : kafkaSourceEnumState.splits()) {
            if (!activeTopics.contains(splitStatus.split().getTopic())
                    || splitStatus.assignmentStatus() != AssignmentStatus.ASSIGNED) {
                updatedSplits.add(splitStatus);
                continue;
            }
            Long retainedSplitOffset =
                    retainedSplitOffsets.get(toDynamicSplitId(kafkaClusterId, splitStatus.split()));
            Preconditions.checkState(
                    retainedSplitOffset != null
                            || splitStatus.split().getStoppingOffset().isPresent(),
                    "Missing retained reader offset for %s; refusing to use an initializer",
                    toDynamicSplitId(kafkaClusterId, splitStatus.split()));
            if (retainedSplitOffset == null) {
                // Keep completed bounded partitions as ASSIGNED tombstones against rediscovery.
                updatedSplits.add(splitStatus);
                continue;
            }
            updatedSplits.add(
                    new SplitAndAssignmentStatus(
                            new KafkaPartitionSplit(
                                    splitStatus.split().getTopicPartition(),
                                    retainedSplitOffset,
                                    splitStatus
                                            .split()
                                            .getStoppingOffset()
                                            .orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET)),
                            AssignmentStatus.UNASSIGNED));
        }
        return new KafkaSourceEnumState(
                updatedSplits, kafkaSourceEnumState.initialDiscoveryFinished());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        boolean assigned = false;
        for (Entry<String, RetainedSplitOffsetHandoff> entry :
                new TreeMap<>(retainedSplitOffsetHandoffs).entrySet()) {
            RetainedSplitOffsetHandoff handoff = entry.getValue();
            if (handoff.preparedState != null && checkpointId >= handoff.eligibleCheckpointId) {
                String cluster = entry.getKey();
                startClusterEnumerator(cluster, handoff.preparedState);
                assigned = true;
            }
        }
        if (assigned) {
            // Assignment and shadow cleanup share one coordinator action and the same ordered
            // operator-event checkpoint barrier. No intermediate checkpoint loses the offsets.
            sendMetadataUpdateEventToAvailableReaders();
            handleNoMoreSplits();
        }
    }

    private void maybeStartReadyRetainedClusterEnumerators() {
        for (String kafkaClusterId : latestClusterTopicsMap.keySet()) {
            if (clusterEnumeratorMap.containsKey(kafkaClusterId)) {
                continue;
            }

            DynamicKafkaSourceEnumState.RetainedClusterState retainedClusterState =
                    retainedClusterEnumeratorStates.get(kafkaClusterId);
            if (retainedClusterState == null) {
                startClusterEnumerator(
                        kafkaClusterId, new KafkaSourceEnumState(Collections.emptySet(), false));
            } else if (isRetainedClusterReadyForAssignment(kafkaClusterId, retainedClusterState)) {
                startClusterEnumerator(
                        kafkaClusterId, retainedClusterState.getKafkaSourceEnumState());
            } else {
                startRetainedSplitOffsetHandoff(kafkaClusterId);
            }
        }
    }

    private void startClusterEnumerator(String kafkaClusterId, KafkaSourceEnumState state) {
        Set<SplitAndAssignmentStatus> activeTopicSplits =
                filterStateByTopics(state, latestClusterTopicsMap.get(kafkaClusterId));
        if (retainedClusterEnumeratorStates.containsKey(kafkaClusterId)) {
            List<DynamicKafkaSourceSplit> returning =
                    activeTopicSplits.stream()
                            .filter(
                                    status ->
                                            status.assignmentStatus()
                                                    == AssignmentStatus.UNASSIGNED)
                            .map(
                                    status ->
                                            new DynamicKafkaSourceSplit(
                                                    kafkaClusterId, status.split()))
                            .collect(Collectors.toList());
            splitAssignmentStrategy.onRetainedSplitsReadded(
                    returning, activeOwners(), enumContext.currentParallelism());
            // The old owners are fenced by the completed checkpoint (or these splits were never
            // assigned). Normal assignment now establishes their new authoritative owners.
            splitOwners
                    .values()
                    .removeIf(owner -> owner.split.getKafkaClusterId().equals(kafkaClusterId));
        }

        createClusterEnumerator(
                kafkaClusterId,
                new KafkaSourceEnumState(activeTopicSplits, state.initialDiscoveryFinished()));
        startEnumerator(kafkaClusterId);
        retainedClusterEnumeratorStates.remove(kafkaClusterId);
        retainedSplitOffsetHandoffs.remove(kafkaClusterId);
        addRegisteredReadersToEnumerator(kafkaClusterId);
    }

    private KafkaSourceEnumerator createClusterEnumerator(
            String kafkaClusterId, KafkaSourceEnumState state) {
        ClusterMetadata metadata =
                Preconditions.checkNotNull(
                        findClusterMetadata(kafkaClusterId),
                        "Missing metadata for cluster %s",
                        kafkaClusterId);
        return createEnumeratorWithAssignedTopicPartitions(
                kafkaClusterId,
                latestClusterTopicsMap.get(kafkaClusterId),
                state,
                metadata.getProperties(),
                metadata.getStartingOffsetsInitializer(),
                metadata.getStoppingOffsetsInitializer());
    }

    @Nullable
    private ClusterMetadata findClusterMetadata(String kafkaClusterId) {
        for (KafkaStream kafkaStream : latestKafkaStreams) {
            ClusterMetadata clusterMetadata =
                    kafkaStream.getClusterMetadataMap().get(kafkaClusterId);
            if (clusterMetadata != null) {
                return clusterMetadata;
            }
        }
        return null;
    }

    private void addRegisteredReadersToEnumerator(String kafkaClusterId) {
        SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> enumerator =
                clusterEnumeratorMap.get(kafkaClusterId);
        if (enumerator == null) {
            return;
        }
        for (int reader : enumContext.registeredReaders().keySet()) {
            enumerator.addReader(reader);
        }
    }

    private Set<SplitAndAssignmentStatus> filterStateByTopics(
            KafkaSourceEnumState kafkaSourceEnumState, Set<String> activeTopics) {
        return kafkaSourceEnumState.splits().stream()
                .filter(splitStatus -> activeTopics.contains(splitStatus.split().getTopic()))
                .collect(Collectors.toSet());
    }

    private static String summarizeSplitOffsets(Collection<KafkaPartitionSplit> splits) {
        if (splits.isEmpty()) {
            return "[]";
        }
        return splits.stream()
                .sorted(Comparator.comparing(split -> split.getTopicPartition().toString()))
                .map(split -> split.getTopicPartition() + "=" + split.getStartingOffset())
                .collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof RetainedSplitOffsetsEvent) {
            handleRetainedSplitOffsetsEvent(subtaskId, (RetainedSplitOffsetsEvent) sourceEvent);
            return;
        }

        Preconditions.checkArgument(
                sourceEvent instanceof GetMetadataUpdateEvent,
                "Received invalid source event: " + sourceEvent);

        if (enumContext.registeredReaders().containsKey(subtaskId)) {
            if (!shouldDeferMetadataUpdateEvents()) {
                sendMetadataUpdateEvent(subtaskId);
                sendPendingRetainedSplitOffsetRequestsToReader(subtaskId);
            }
        } else {
            logger.warn("Got get metadata update but subtask was unavailable");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            kafkaMetadataServiceDiscoveryContext.prepareForClose();
            clusterEnumContextMap.values().forEach(StoppableKafkaEnumContextProxy::prepareForClose);

            // Metadata service close may unblock an in-flight metadata discovery call.
            kafkaMetadataService.close();
            kafkaMetadataServiceDiscoveryContext.close();

            // close contexts first since they may have running tasks
            for (StoppableKafkaEnumContextProxy subEnumContext : clusterEnumContextMap.values()) {
                subEnumContext.close();
            }

            for (Entry<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
                    clusterEnumerator : clusterEnumeratorMap.entrySet()) {
                clusterEnumerator.getValue().close();
            }

            enumeratorClosingExecutor.shutdown();
            enumeratorClosingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            Throwable asynchronousCloseFailure = asynchronousEnumeratorCloseFailure.get();
            if (asynchronousCloseFailure != null) {
                throw new RuntimeException(
                        "Failed to close stale dynamic Kafka enumerator", asynchronousCloseFailure);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SplitAssignmentStrategy createSplitAssignmentStrategy(Properties properties) {
        DynamicKafkaSourceOptions.EnumeratorMode enumeratorMode =
                DynamicKafkaSourceOptions.getEnumeratorMode(properties);
        logger.info("Using dynamic Kafka split enumerator mode: {}", enumeratorMode);

        switch (enumeratorMode) {
            case GLOBAL:
                return new GlobalSplitAssignmentStrategy();
            case PER_CLUSTER:
            default:
                return new PerClusterSplitAssignmentStrategy();
        }
    }

    private static String toDynamicSplitId(String kafkaClusterId, KafkaPartitionSplit split) {
        return kafkaClusterId + "-" + split.splitId();
    }

    private interface SplitAssignmentStrategy {
        @Nullable
        default KafkaSourceEnumerator.SplitOwnerSelector createSplitOwnerSelector(
                String kafkaClusterId) {
            return null;
        }

        default void onMetadataRefresh(Set<String> activeSplitIds) {}

        default void onRecoveredSplits(
                List<DynamicKafkaSourceSplit> splits, int currentParallelism) {}

        default void onRetainedSplitsReadded(
                List<DynamicKafkaSourceSplit> splits,
                Map<String, Integer> activeOwners,
                int currentParallelism) {}
    }

    private static class PerClusterSplitAssignmentStrategy implements SplitAssignmentStrategy {}

    private static class GlobalSplitAssignmentStrategy implements SplitAssignmentStrategy {
        private final GlobalSplitOwnerAssigner splitOwnerAssigner;

        private GlobalSplitAssignmentStrategy() {
            this.splitOwnerAssigner = new GlobalSplitOwnerAssigner();
        }

        @Override
        public KafkaSourceEnumerator.SplitOwnerSelector createSplitOwnerSelector(
                String kafkaClusterId) {
            return (split, numReaders) -> assignSplitOwner(kafkaClusterId, split, numReaders);
        }

        @Override
        public void onMetadataRefresh(Set<String> activeSplitIds) {
            splitOwnerAssigner.onMetadataRefresh(activeSplitIds);
        }

        @Override
        public void onRecoveredSplits(
                List<DynamicKafkaSourceSplit> splits, int currentParallelism) {
            splitOwnerAssigner.onRecoveredSplits(splits, currentParallelism);
        }

        @Override
        public void onRetainedSplitsReadded(
                List<DynamicKafkaSourceSplit> splits,
                Map<String, Integer> activeOwners,
                int currentParallelism) {
            splitOwnerAssigner.onRetainedSplitsReadded(splits, activeOwners, currentParallelism);
        }

        private int assignSplitOwner(
                String kafkaClusterId, KafkaPartitionSplit split, int numReaders) {
            final String splitId = toDynamicSplitId(kafkaClusterId, split);
            return splitOwnerAssigner.assignSplitOwner(splitId, numReaders);
        }
    }
}
