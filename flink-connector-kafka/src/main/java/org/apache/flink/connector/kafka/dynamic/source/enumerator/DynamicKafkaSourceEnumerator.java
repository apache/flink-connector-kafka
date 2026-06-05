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
                        .getDefaultFactory());
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

        this.kafkaMetadataService = kafkaMetadataService;
        this.stoppableKafkaEnumContextProxyFactory = stoppableKafkaEnumContextProxyFactory;
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
            enumContext.callAsync(
                    () -> kafkaStreamSubscriber.getSubscribedStreams(kafkaMetadataService),
                    this::onHandleSubscribedStreamsFetch);
        } else {
            enumContext.callAsync(
                    () -> kafkaStreamSubscriber.getSubscribedStreams(kafkaMetadataService),
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

    private void onHandleSubscribedStreamsFetch(Set<KafkaStream> fetchedKafkaStreams, Throwable t) {
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

        logger.info("Closing enumerators due to metadata change");

        closeAllEnumeratorsAndContexts();
        latestClusterTopicsMap = newClustersTopicsMap;
        latestKafkaStreams = handledFetchKafkaStreams;
        sendMetadataUpdateEventToAvailableReaders();

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
            MetadataUpdateEvent metadataUpdateEvent = new MetadataUpdateEvent(latestKafkaStreams);
            logger.debug("sending metadata update to reader {}: {}", readerId, metadataUpdateEvent);
            enumContext.sendEventToSourceReader(readerId, metadataUpdateEvent);
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
        clusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> {
                    try {
                        clusterEnumContextMap.get(cluster).close();
                        subEnumerator.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        clusterEnumContextMap.clear();
        clusterEnumeratorMap.clear();
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
            MetadataUpdateEvent metadataUpdateEvent = new MetadataUpdateEvent(latestKafkaStreams);
            logger.debug(
                    "sending metadata update to reader {}: {}", subtaskId, metadataUpdateEvent);
            enumContext.sendEventToSourceReader(subtaskId, metadataUpdateEvent);
        } else {
            logger.warn("Got get metadata update but subtask was unavailable");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // close contexts first since they may have running tasks
            for (StoppableKafkaEnumContextProxy subEnumContext : clusterEnumContextMap.values()) {
                subEnumContext.close();
            }

            for (Entry<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
                    clusterEnumerator : clusterEnumeratorMap.entrySet()) {
                clusterEnumerator.getValue().close();
            }

            kafkaMetadataService.close();
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
