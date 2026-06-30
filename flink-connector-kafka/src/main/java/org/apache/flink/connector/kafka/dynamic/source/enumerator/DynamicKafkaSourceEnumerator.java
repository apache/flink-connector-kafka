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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
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
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
    private static final long RETAINED_SPLIT_OFFSET_HANDOFF_MIN_TIMEOUT_MS = 60_000L;
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
    private boolean retainedSplitOffsetHandoffRetryScheduled;
    private boolean firstDiscoveryComplete;

    private static class RetainedSplitOffsetHandoff {
        private final long handoffId;
        private final long deadlineMs;
        private final Map<Integer, Map<String, Long>> offsetsByReader = new HashMap<>();

        private RetainedSplitOffsetHandoff(long handoffId, long deadlineMs) {
            this.handoffId = handoffId;
            this.deadlineMs = deadlineMs;
        }

        private boolean isExpired(long currentTimeMillis) {
            return deadlineMs <= currentTimeMillis;
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
                new HashMap<>(dynamicKafkaSourceEnumState.getRetainedClusterEnumeratorStates());
        this.retainedSplitOffsetHandoffs = new HashMap<>();
        this.nextRetainedSplitOffsetHandoffId = 0L;
        this.retainedSplitOffsetHandoffRetryScheduled = false;
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
        pruneExpiredRetainedSplitOffsetHandoffs();

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

        // don't do anything if no change
        if (latestClusterTopicsMap.equals(newClustersTopicsMap)) {
            latestKafkaStreams = handledFetchKafkaStreams;
            maybeStartReadyRetainedClusterEnumerators();
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
        sendMetadataUpdateEventToAvailableReaders();

        logger.info("Closing enumerators due to metadata change");

        closeAllEnumeratorsAndContexts();
        retainRemovedClusterEnumeratorStates(
                dynamicKafkaSourceEnumState.getClusterEnumeratorStates(),
                latestClusterTopicsMap.keySet());
        retainedSplitOffsetHandoffs
                .keySet()
                .removeIf(kafkaClusterId -> !latestClusterTopicsMap.containsKey(kafkaClusterId));

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

        splitAssignmentStrategy.onMetadataRefresh(activeSplitIds);
        startAllEnumerators();
        maybeStartReadyRetainedClusterEnumerators();
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
        for (int readerId : enumContext.registeredReaders().keySet()) {
            sendMetadataUpdateEventToReader(readerId);
        }
    }

    private void sendMetadataUpdateEventToReader(int readerId) {
        MetadataUpdateEvent metadataUpdateEvent = new MetadataUpdateEvent(latestKafkaStreams);
        logger.debug("sending metadata update to reader {}: {}", readerId, metadataUpdateEvent);
        enumContext.sendEventToSourceReader(readerId, metadataUpdateEvent);
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
        KafkaSourceEnumerator.SplitOwnerSelector splitOwnerSelector =
                splitAssignmentStrategy.createSplitOwnerSelector(kafkaClusterId);
        SplitEnumeratorContext<KafkaPartitionSplit> assignmentContext =
                splitAssignmentStrategy.createAssignmentContext(kafkaClusterId, context);

        Properties consumerProps = new Properties();
        KafkaPropertiesUtil.copyProperties(fetchedProperties, consumerProps);
        KafkaPropertiesUtil.copyProperties(properties, consumerProps);
        DynamicKafkaSourceOptions.removeRemovedClusterRetentionOption(consumerProps);
        KafkaPropertiesUtil.setClientIdPrefix(consumerProps, kafkaClusterId);
        consumerProps.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                effectiveStartingOffsetsInitializer
                        .getAutoOffsetResetStrategy()
                        .name()
                        .toLowerCase());

        KafkaSourceEnumerator enumerator =
                new KafkaSourceEnumerator(
                        KafkaSubscriber.getTopicListSubscriber(new ArrayList<>(topics)),
                        effectiveStartingOffsetsInitializer,
                        effectiveStoppingOffsetsInitializer,
                        consumerProps,
                        assignmentContext,
                        boundedness,
                        kafkaSourceEnumState,
                        splitOwnerSelector);

        clusterEnumContextMap.put(kafkaClusterId, context);
        clusterEnumeratorMap.put(kafkaClusterId, enumerator);

        return enumerator;
    }

    private void startAllEnumerators() {
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
            if (kafkaMetadataService.isClusterActive(kafkaClusterId)) {
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
        splitAssignmentStrategy.onSplitsBack(splits, subtaskId);

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
                logger.warn(
                        "Split refers to inactive cluster {} with current clusters being {}",
                        kafkaClusterId,
                        clusterEnumeratorMap.keySet());
            }
        }

        handleNoMoreSplits();
    }

    /** NOTE: this happens at startup and failover. */
    @Override
    public void addReader(int subtaskId) {
        logger.debug("Adding reader {}", subtaskId);
        splitAssignmentStrategy.onReaderAdded(subtaskId);

        // assign pending splits from the sub enumerator
        clusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> subEnumerator.addReader(subtaskId));
        if (!retainedSplitOffsetHandoffs.isEmpty()) {
            pruneExpiredRetainedSplitOffsetHandoffs();
            // A reader can join while a re-added cluster is waiting for offset handoff. Send
            // metadata first so the reader has reconciled its local retained state before it
            // answers the request. Restart the attempt so delayed responses from the reader's
            // previous attempt cannot count as its replacement report.
            retainedSplitOffsetHandoffs
                    .values()
                    .forEach(handoff -> handoff.offsetsByReader.clear());
            retainedSplitOffsetHandoffs.clear();
            sendMetadataUpdateEventToReader(subtaskId);
            maybeStartReadyRetainedClusterEnumerators();
        }
        handleNoMoreSplits();
    }

    /**
     * Besides for checkpointing, this method is used in the restart sequence to retain the relevant
     * assigned splits so that there is no reader duplicate split assignment. See {@link
     * #createEnumeratorWithAssignedTopicPartitions(String, Set, KafkaSourceEnumState, Properties,
     * OffsetsInitializer, OffsetsInitializer)}}
     */
    @Override
    public DynamicKafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        pruneExpiredRetainedClusterEnumeratorStates();
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

        return new DynamicKafkaSourceEnumState(
                latestKafkaStreams,
                subEnumeratorStateByCluster,
                new HashMap<>(retainedClusterEnumeratorStates));
    }

    private void retainRemovedClusterEnumeratorStates(
            Map<String, KafkaSourceEnumState> activeClusterEnumeratorStates,
            Set<String> activeKafkaClusterIds) {
        if (removedClusterStateRetentionMs <= 0) {
            return;
        }

        long retainedUntilMs = System.currentTimeMillis() + removedClusterStateRetentionMs;
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
        retainedClusterEnumeratorStates
                .entrySet()
                .removeIf(entry -> entry.getValue().getRetainedUntilMs() <= currentTimeMillis);
        retainedSplitOffsetHandoffs.keySet().retainAll(retainedClusterEnumeratorStates.keySet());
    }

    private void pruneExpiredRetainedSplitOffsetHandoffs() {
        long currentTimeMillis = System.currentTimeMillis();
        retainedSplitOffsetHandoffs
                .entrySet()
                .removeIf(
                        entry -> {
                            RetainedSplitOffsetHandoff handoff = entry.getValue();
                            if (!handoff.isExpired(currentTimeMillis)) {
                                return false;
                            }
                            logger.debug(
                                    "Discarding timed out retained split offset handoff for cluster {}: handoffId={}",
                                    entry.getKey(),
                                    handoff.handoffId);
                            handoff.offsetsByReader.clear();
                            return true;
                        });
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

        // Keep the attempt bounded without making a fast metadata refresh interval shorter than a
        // reader source-event round trip.
        long handoffTimeoutMs =
                Math.max(
                        kafkaMetadataServiceDiscoveryIntervalMs,
                        RETAINED_SPLIT_OFFSET_HANDOFF_MIN_TIMEOUT_MS);
        long deadlineMs = System.currentTimeMillis() + handoffTimeoutMs;
        RetainedSplitOffsetHandoff handoff =
                new RetainedSplitOffsetHandoff(++nextRetainedSplitOffsetHandoffId, deadlineMs);
        retainedSplitOffsetHandoffs.put(kafkaClusterId, handoff);
        scheduleRetainedSplitOffsetHandoffRetryIfNeeded();
        for (int readerId : enumContext.registeredReaders().keySet()) {
            sendRetainedSplitOffsetRequestToReader(kafkaClusterId, handoff, readerId);
        }
    }

    private void scheduleRetainedSplitOffsetHandoffRetryIfNeeded() {
        if (kafkaMetadataServiceDiscoveryIntervalMs > 0
                || retainedSplitOffsetHandoffRetryScheduled) {
            return;
        }

        // One-time metadata discovery has no later refresh to discard an expired handoff. Keep a
        // single lightweight retry loop after the first handoff; it does not fetch metadata.
        retainedSplitOffsetHandoffRetryScheduled = true;
        kafkaMetadataServiceDiscoveryContext.<Void>callAsync(
                () -> null,
                (ignored, t) -> {
                    if (t != null) {
                        throw new RuntimeException("Retained split offset handoff retry failed", t);
                    }
                    pruneExpiredRetainedClusterEnumeratorStates();
                    pruneExpiredRetainedSplitOffsetHandoffs();
                    maybeStartReadyRetainedClusterEnumerators();
                },
                RETAINED_SPLIT_OFFSET_HANDOFF_MIN_TIMEOUT_MS,
                RETAINED_SPLIT_OFFSET_HANDOFF_MIN_TIMEOUT_MS);
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
        if (handoff.isExpired(System.currentTimeMillis())) {
            logger.debug(
                    "Ignoring retained split offsets from timed out handoff {}: {}",
                    handoff.handoffId,
                    retainedSplitOffsetsEvent);
            clearRetainedSplitOffsetHandoff(kafkaClusterId, handoff);
            return;
        }

        handoff.offsetsByReader.put(subtaskId, retainedSplitOffsetsEvent.getRetainedSplitOffsets());
        if (handoff.offsetsByReader.size() >= enumContext.currentParallelism()) {
            applyRetainedSplitOffsetHandoff(kafkaClusterId, handoff);
        }
        maybeStartReadyRetainedClusterEnumerators();
    }

    private void applyRetainedSplitOffsetHandoff(
            String kafkaClusterId, RetainedSplitOffsetHandoff handoff) {
        DynamicKafkaSourceEnumState.RetainedClusterState retainedClusterState =
                retainedClusterEnumeratorStates.get(kafkaClusterId);
        if (retainedClusterState == null) {
            clearRetainedSplitOffsetHandoff(kafkaClusterId, handoff);
            return;
        }

        KafkaSourceEnumState kafkaSourceEnumState = retainedClusterState.getKafkaSourceEnumState();
        Map<String, Long> retainedSplitOffsets = handoff.mergedOffsets();
        Set<String> activeTopics =
                latestClusterTopicsMap.getOrDefault(kafkaClusterId, Collections.emptySet());
        Set<SplitAndAssignmentStatus> updatedSplits = new HashSet<>();
        for (SplitAndAssignmentStatus splitStatus : kafkaSourceEnumState.splits()) {
            if (!activeTopics.contains(splitStatus.split().getTopic())) {
                updatedSplits.add(splitStatus);
                continue;
            }
            if (splitStatus.assignmentStatus().equals(AssignmentStatus.ASSIGNED)) {
                Long retainedSplitOffset =
                        retainedSplitOffsets.get(
                                toDynamicSplitId(kafkaClusterId, splitStatus.split()));
                if (retainedSplitOffset == null) {
                    // No reader retains this offset anymore; let normal discovery recreate it.
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
            } else {
                updatedSplits.add(splitStatus);
            }
        }

        retainedClusterEnumeratorStates.put(
                kafkaClusterId,
                new DynamicKafkaSourceEnumState.RetainedClusterState(
                        new KafkaSourceEnumState(
                                updatedSplits, kafkaSourceEnumState.initialDiscoveryFinished()),
                        retainedClusterState.getRetainedUntilMs()));
        clearRetainedSplitOffsetHandoff(kafkaClusterId, handoff);
    }

    private void clearRetainedSplitOffsetHandoff(
            String kafkaClusterId, RetainedSplitOffsetHandoff handoff) {
        if (retainedSplitOffsetHandoffs.remove(kafkaClusterId, handoff)) {
            handoff.offsetsByReader.clear();
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
                startFreshClusterEnumerator(kafkaClusterId);
            } else if (isRetainedClusterReadyForAssignment(kafkaClusterId, retainedClusterState)) {
                startReadyRetainedClusterEnumerator(kafkaClusterId, retainedClusterState);
            } else {
                startRetainedSplitOffsetHandoff(kafkaClusterId);
            }
        }
    }

    private void startFreshClusterEnumerator(String kafkaClusterId) {
        ClusterMetadata clusterMetadata = findClusterMetadata(kafkaClusterId);
        if (clusterMetadata == null) {
            return;
        }

        createEnumeratorWithAssignedTopicPartitions(
                kafkaClusterId,
                latestClusterTopicsMap.get(kafkaClusterId),
                new KafkaSourceEnumState(Collections.emptySet(), false),
                clusterMetadata.getProperties(),
                clusterMetadata.getStartingOffsetsInitializer(),
                clusterMetadata.getStoppingOffsetsInitializer());
        startEnumerator(kafkaClusterId);
        addRegisteredReadersToEnumerator(kafkaClusterId);
    }

    private void startReadyRetainedClusterEnumerator(
            String kafkaClusterId,
            DynamicKafkaSourceEnumState.RetainedClusterState retainedClusterState) {
        ClusterMetadata clusterMetadata = findClusterMetadata(kafkaClusterId);
        if (clusterMetadata == null) {
            logger.warn(
                    "Cannot start re-added retained cluster {} because metadata is unavailable",
                    kafkaClusterId);
            return;
        }

        KafkaSourceEnumState retainedState = retainedClusterState.getKafkaSourceEnumState();
        Set<SplitAndAssignmentStatus> activeTopicSplits =
                filterStateByTopics(retainedState, latestClusterTopicsMap.get(kafkaClusterId));
        List<KafkaPartitionSplit> retainedSplits =
                activeTopicSplits.stream()
                        .map(SplitAndAssignmentStatus::split)
                        .collect(Collectors.toList());
        retainedClusterEnumeratorStates.remove(kafkaClusterId);
        retainedSplitOffsetHandoffs.remove(kafkaClusterId);

        createEnumeratorWithAssignedTopicPartitions(
                kafkaClusterId,
                latestClusterTopicsMap.get(kafkaClusterId),
                new KafkaSourceEnumState(
                        Collections.emptyList(),
                        retainedSplits,
                        retainedState.initialDiscoveryFinished()),
                clusterMetadata.getProperties(),
                clusterMetadata.getStartingOffsetsInitializer(),
                clusterMetadata.getStoppingOffsetsInitializer());
        startEnumerator(kafkaClusterId);
        addRegisteredReadersToEnumerator(kafkaClusterId);
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
            sendMetadataUpdateEventToReader(subtaskId);
            pruneExpiredRetainedSplitOffsetHandoffs();
            sendPendingRetainedSplitOffsetRequestsToReader(subtaskId);
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

        SplitEnumeratorContext<KafkaPartitionSplit> createAssignmentContext(
                String kafkaClusterId, StoppableKafkaEnumContextProxy clusterContext);

        default void onReaderAdded(int subtaskId) {}

        default void onSplitsBack(List<DynamicKafkaSourceSplit> splits, int subtaskId) {}

        default void onMetadataRefresh(Set<String> activeSplitIds) {}
    }

    private static class PerClusterSplitAssignmentStrategy implements SplitAssignmentStrategy {
        @Override
        public SplitEnumeratorContext<KafkaPartitionSplit> createAssignmentContext(
                String kafkaClusterId, StoppableKafkaEnumContextProxy clusterContext) {
            return clusterContext;
        }
    }

    private static class GlobalSplitAssignmentStrategy implements SplitAssignmentStrategy {
        private final GlobalSplitOwnerAssigner splitOwnerAssigner;

        private GlobalSplitAssignmentStrategy() {
            this.splitOwnerAssigner = new GlobalSplitOwnerAssigner();
        }

        @Override
        public SplitEnumeratorContext<KafkaPartitionSplit> createAssignmentContext(
                String kafkaClusterId, StoppableKafkaEnumContextProxy clusterContext) {
            return clusterContext;
        }

        @Override
        public KafkaSourceEnumerator.SplitOwnerSelector createSplitOwnerSelector(
                String kafkaClusterId) {
            return (split, numReaders) -> assignSplitOwner(kafkaClusterId, split, numReaders);
        }

        @Override
        public void onReaderAdded(int subtaskId) {}

        @Override
        public void onSplitsBack(List<DynamicKafkaSourceSplit> splits, int subtaskId) {
            splitOwnerAssigner.onSplitsBack(splits, subtaskId);
        }

        @Override
        public void onMetadataRefresh(Set<String> activeSplitIds) {
            splitOwnerAssigner.onMetadataRefresh(activeSplitIds);
        }

        private int assignSplitOwner(
                String kafkaClusterId, KafkaPartitionSplit split, int numReaders) {
            final String splitId = toDynamicSplitId(kafkaClusterId, split);
            return splitOwnerAssigner.assignSplitOwner(splitId, numReaders);
        }
    }
}
