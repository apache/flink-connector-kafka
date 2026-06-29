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
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;
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
    private boolean firstDiscoveryComplete;
    private boolean initialReaderRegistrationPending;
    private final Map<Integer, List<DynamicKafkaSourceSplit>> pendingReportedSplitsByReader;
    private final Set<Integer> pendingMetadataUpdateReaders;

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
        this.pendingReportedSplitsByReader = new HashMap<>();
        this.pendingMetadataUpdateReaders = new HashSet<>();

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
            boolean allEnumeratorsHaveSignalledNoMoreSplits = true;
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

        // don't do anything if no change
        if (latestClusterTopicsMap.equals(newClustersTopicsMap)) {
            tryCompletePendingReaderRegistration();
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

        // create enumerators
        Set<String> activeSplitIds = new HashSet<>();
        for (Entry<String, Set<String>> activeClusterTopics : latestClusterTopicsMap.entrySet()) {
            String kafkaClusterId = activeClusterTopics.getKey();
            KafkaSourceEnumState kafkaSourceEnumState =
                    dynamicKafkaSourceEnumState.getClusterEnumeratorStates().get(kafkaClusterId);
            if (kafkaSourceEnumState == null) {
                DynamicKafkaSourceEnumState.RetainedClusterState retainedClusterState =
                        retainedClusterEnumeratorStates.remove(kafkaClusterId);
                if (retainedClusterState != null) {
                    kafkaSourceEnumState = retainedClusterState.getKafkaSourceEnumState();
                }
            } else {
                retainedClusterEnumeratorStates.remove(kafkaClusterId);
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

            // Restart the enumerator from the active topic partitions already known in state. The
            // reader restores those splits from its own checkpointed offsets during metadata
            // reconciliation, so the enumerator must not send them again as newly discovered
            // splits.
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
        tryCompletePendingReaderRegistration();
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
            pendingMetadataUpdateReaders.addAll(enumContext.registeredReaders().keySet());
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
        addSplitsBackToClusterEnumerators(splits, subtaskId, false);
        handleNoMoreSplits();
    }

    private void addSplitsBackToClusterEnumerators(
            List<DynamicKafkaSourceSplit> splits, int subtaskId, boolean failOnInactiveCluster) {
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
            } else if (failOnInactiveCluster) {
                throw new IllegalStateException(
                        String.format(
                                "Cannot reassign split for active cluster %s because its"
                                        + " enumerator is unavailable",
                                kafkaClusterId));
            } else {
                logger.warn(
                        "Split refers to inactive cluster {} with current clusters being {}",
                        kafkaClusterId,
                        clusterEnumeratorMap.keySet());
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
                pendingReportedSplitsByReader.put(subtaskId, new ArrayList<>(reportedSplits));
            }
        }

        if (tryCompletePendingReaderRegistration()) {
            return;
        }

        addReaderToClusterEnumerators(subtaskId);
        handleNoMoreSplits();
    }

    private boolean tryCompletePendingReaderRegistration() {
        boolean hasPendingRecovery =
                initialReaderRegistrationPending || !pendingReportedSplitsByReader.isEmpty();
        if (!hasPendingRecovery) {
            return false;
        }
        if (!firstDiscoveryComplete || !allReadersRegistered()) {
            return true;
        }

        if (initialReaderRegistrationPending) {
            initialReaderRegistrationPending = false;
        }
        if (!pendingReportedSplitsByReader.isEmpty()) {
            reassignReportedSplits();
        } else {
            flushPendingSplitAssignmentsForRegisteredReaders();
        }
        handleNoMoreSplits();
        flushPendingMetadataUpdateEvents();
        return true;
    }

    private boolean allReadersRegistered() {
        return enumContext.registeredReaders().size() == enumContext.currentParallelism();
    }

    private void addReaderToClusterEnumerators(int subtaskId) {
        splitAssignmentStrategy.onReaderAdded(subtaskId);
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
        Map<String, ReportedSplit> activeReportedSplits = new TreeMap<>();
        Map<Integer, List<DynamicKafkaSourceSplit>> retainedSplitsByReader = new TreeMap<>();
        long currentTimeMillis = System.currentTimeMillis();

        for (Entry<Integer, List<DynamicKafkaSourceSplit>> readerSplits :
                new TreeMap<>(pendingReportedSplitsByReader).entrySet()) {
            int readerId = readerSplits.getKey();
            for (DynamicKafkaSourceSplit split : readerSplits.getValue()) {
                if (isSplitActive(split)) {
                    DynamicKafkaSourceSplit activeSplit = split.clearRetention();
                    ReportedSplit previous =
                            activeReportedSplits.putIfAbsent(
                                    activeSplit.splitId(),
                                    new ReportedSplit(activeSplit, readerId));
                    if (previous != null) {
                        throw new IllegalStateException(
                                String.format(
                                        "Split %s was reported by both reader %d and reader %d",
                                        activeSplit.splitId(), previous.readerId, readerId));
                    }
                } else {
                    DynamicKafkaSourceSplit retainedSplit =
                            getRetainedReportedSplit(split, currentTimeMillis);
                    if (retainedSplit != null) {
                        retainedSplitsByReader
                                .computeIfAbsent(readerId, ignored -> new ArrayList<>())
                                .add(retainedSplit);
                    } else {
                        logger.info("Dropping inactive reported split on recovery: {}", split);
                    }
                }
            }
        }

        List<DynamicKafkaSourceSplit> activeSplits =
                activeReportedSplits.values().stream()
                        .map(reportedSplit -> reportedSplit.split)
                        .collect(Collectors.toList());
        splitAssignmentStrategy.onRecoveredSplits(activeSplits, enumContext.currentParallelism());

        for (ReportedSplit reportedSplit : activeReportedSplits.values()) {
            addSplitsBackToClusterEnumerators(
                    Collections.singletonList(reportedSplit.split), reportedSplit.readerId, true);
        }

        flushPendingSplitAssignmentsForRegisteredReaders();

        if (!retainedSplitsByReader.isEmpty()) {
            enumContext.assignSplits(new SplitsAssignment<>(retainedSplitsByReader));
        }
        pendingReportedSplitsByReader.clear();
    }

    private boolean shouldDeferMetadataUpdateEvents() {
        return initialReaderRegistrationPending
                || (!pendingReportedSplitsByReader.isEmpty() && !allReadersRegistered());
    }

    private void flushPendingMetadataUpdateEvents() {
        List<Integer> readers = new ArrayList<>(pendingMetadataUpdateReaders);
        Collections.sort(readers);
        pendingMetadataUpdateReaders.clear();
        for (int readerId : readers) {
            if (enumContext.registeredReaders().containsKey(readerId)) {
                sendMetadataUpdateEvent(readerId);
            }
        }
    }

    private void sendMetadataUpdateEvent(int readerId) {
        MetadataUpdateEvent metadataUpdateEvent = new MetadataUpdateEvent(latestKafkaStreams);
        logger.debug("sending metadata update to reader {}: {}", readerId, metadataUpdateEvent);
        enumContext.sendEventToSourceReader(readerId, metadataUpdateEvent);
    }

    private boolean isSplitActive(DynamicKafkaSourceSplit split) {
        for (KafkaStream kafkaStream : latestKafkaStreams) {
            ClusterMetadata clusterMetadata =
                    kafkaStream.getClusterMetadataMap().get(split.getKafkaClusterId());
            if (clusterMetadata != null
                    && clusterMetadata
                            .getTopics()
                            .contains(split.getKafkaPartitionSplit().getTopic())) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    private DynamicKafkaSourceSplit getRetainedReportedSplit(
            DynamicKafkaSourceSplit split, long currentTimeMillis) {
        if (split.isRetained()) {
            return split.isRetained(currentTimeMillis) ? split : null;
        }
        if (removedClusterStateRetentionMs > 0 && !isClusterActive(split.getKafkaClusterId())) {
            return split.retainUntil(currentTimeMillis + removedClusterStateRetentionMs);
        }
        return null;
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
                || !dynamicKafkaSourceEnumState.getRetainedClusterEnumeratorStates().isEmpty();
    }

    private static class ReportedSplit {
        private final DynamicKafkaSourceSplit split;
        private final int readerId;

        private ReportedSplit(DynamicKafkaSourceSplit split, int readerId) {
            this.split = split;
            this.readerId = readerId;
        }
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
        activeClusterEnumeratorStates.entrySet().stream()
                .filter(entry -> !activeKafkaClusterIds.contains(entry.getKey()))
                .forEach(
                        entry ->
                                retainedClusterEnumeratorStates.put(
                                        entry.getKey(),
                                        new DynamicKafkaSourceEnumState.RetainedClusterState(
                                                entry.getValue(), retainedUntilMs)));
    }

    private void pruneExpiredRetainedClusterEnumeratorStates() {
        if (removedClusterStateRetentionMs <= 0) {
            retainedClusterEnumeratorStates.clear();
            return;
        }

        long currentTimeMillis = System.currentTimeMillis();
        retainedClusterEnumeratorStates
                .entrySet()
                .removeIf(entry -> entry.getValue().getRetainedUntilMs() <= currentTimeMillis);
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
        Preconditions.checkArgument(
                sourceEvent instanceof GetMetadataUpdateEvent,
                "Received invalid source event: " + sourceEvent);

        if (enumContext.registeredReaders().containsKey(subtaskId)) {
            if (shouldDeferMetadataUpdateEvents()) {
                pendingMetadataUpdateReaders.add(subtaskId);
            } else {
                sendMetadataUpdateEvent(subtaskId);
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

        SplitEnumeratorContext<KafkaPartitionSplit> createAssignmentContext(
                String kafkaClusterId, StoppableKafkaEnumContextProxy clusterContext);

        default void onReaderAdded(int subtaskId) {}

        default void onSplitsBack(List<DynamicKafkaSourceSplit> splits, int subtaskId) {}

        default void onMetadataRefresh(Set<String> activeSplitIds) {}

        default void onRecoveredSplits(
                List<DynamicKafkaSourceSplit> splits, int currentParallelism) {}
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

        @Override
        public void onRecoveredSplits(
                List<DynamicKafkaSourceSplit> splits, int currentParallelism) {
            splitOwnerAssigner.onRecoveredSplits(splits, currentParallelism);
        }

        private int assignSplitOwner(
                String kafkaClusterId, KafkaPartitionSplit split, int numReaders) {
            final String splitId = toDynamicSplitId(kafkaClusterId, split);
            return splitOwnerAssigner.assignSplitOwner(splitId, numReaders);
        }
    }
}
