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
import org.apache.flink.connector.kafka.source.enumerator.TopicPartitionAndAssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.Preconditions;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
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

    // state
    private int kafkaMetadataServiceDiscoveryFailureCount;
    private Map<String, Set<String>> latestClusterTopicsMap;
    private Set<KafkaStream> latestKafkaStreams;
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
        this.kafkaMetadataServiceDiscoveryFailureCount = 0;
        this.firstDiscoveryComplete = false;

        this.kafkaMetadataService = kafkaMetadataService;
        this.stoppableKafkaEnumContextProxyFactory = stoppableKafkaEnumContextProxyFactory;

        // handle checkpoint state and rebuild contexts
        this.clusterEnumeratorMap = new HashMap<>();
        this.clusterEnumContextMap = new HashMap<>();
        this.latestKafkaStreams = dynamicKafkaSourceEnumState.getKafkaStreams();

        Map<String, Properties> clusterProperties = new HashMap<>();
        for (KafkaStream kafkaStream : latestKafkaStreams) {
            for (Entry<String, ClusterMetadata> entry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                clusterProperties.put(entry.getKey(), entry.getValue().getProperties());
            }
        }

        this.latestClusterTopicsMap = new HashMap<>();
        for (Entry<String, KafkaSourceEnumState> clusterEnumState :
                dynamicKafkaSourceEnumState.getClusterEnumeratorStates().entrySet()) {
            this.latestClusterTopicsMap.put(
                    clusterEnumState.getKey(),
                    clusterEnumState.getValue().assignedPartitions().stream()
                            .map(TopicPartition::topic)
                            .collect(Collectors.toSet()));

            createEnumeratorWithAssignedTopicPartitions(
                    clusterEnumState.getKey(),
                    this.latestClusterTopicsMap.get(clusterEnumState.getKey()),
                    clusterEnumState.getValue(),
                    clusterProperties.get(clusterEnumState.getKey()));
        }
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

        Map<String, Set<String>> newClustersTopicsMap = new HashMap<>();
        Map<String, Properties> clusterProperties = new HashMap<>();
        for (KafkaStream kafkaStream : handledFetchKafkaStreams) {
            for (Entry<String, ClusterMetadata> entry :
                    kafkaStream.getClusterMetadataMap().entrySet()) {
                String kafkaClusterId = entry.getKey();
                ClusterMetadata clusterMetadata = entry.getValue();

                newClustersTopicsMap
                        .computeIfAbsent(kafkaClusterId, (unused) -> new HashSet<>())
                        .addAll(clusterMetadata.getTopics());
                clusterProperties.put(kafkaClusterId, clusterMetadata.getProperties());
            }
        }

        // don't do anything if no change
        if (latestClusterTopicsMap.equals(newClustersTopicsMap)) {
            return;
        }

        if (logger.isInfoEnabled()) {
            MapDifference<String, Set<String>> metadataDifference =
                    Maps.difference(latestClusterTopicsMap, newClustersTopicsMap);
            logger.info(
                    "Common cluster topics after metadata refresh: {}",
                    metadataDifference.entriesInCommon());
            logger.info(
                    "Removed cluster topics after metadata refresh: {}",
                    metadataDifference.entriesOnlyOnLeft());
            logger.info(
                    "Additional cluster topics after metadata refresh: {}",
                    metadataDifference.entriesOnlyOnRight());
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

        // create enumerators
        for (Entry<String, Set<String>> activeClusterTopics : latestClusterTopicsMap.entrySet()) {
            KafkaSourceEnumState kafkaSourceEnumState =
                    dynamicKafkaSourceEnumState
                            .getClusterEnumeratorStates()
                            .get(activeClusterTopics.getKey());

            final KafkaSourceEnumState newKafkaSourceEnumState;
            if (kafkaSourceEnumState != null) {
                final Set<String> activeTopics = activeClusterTopics.getValue();

                // filter out removed topics
                Set<TopicPartitionAndAssignmentStatus> partitions =
                        kafkaSourceEnumState.partitions().stream()
                                .filter(tp -> activeTopics.contains(tp.topicPartition().topic()))
                                .collect(Collectors.toSet());

                newKafkaSourceEnumState =
                        new KafkaSourceEnumState(
                                partitions, kafkaSourceEnumState.initialDiscoveryFinished());
            } else {
                newKafkaSourceEnumState = new KafkaSourceEnumState(Collections.emptySet(), false);
            }

            // restarts enumerator from state using only the active topic partitions, to avoid
            // sending duplicate splits from enumerator
            createEnumeratorWithAssignedTopicPartitions(
                    activeClusterTopics.getKey(),
                    activeClusterTopics.getValue(),
                    newKafkaSourceEnumState,
                    clusterProperties.get(activeClusterTopics.getKey()));
        }

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
            Properties fetchedProperties) {
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

        Properties consumerProps = new Properties();
        KafkaPropertiesUtil.copyProperties(fetchedProperties, consumerProps);
        KafkaPropertiesUtil.copyProperties(properties, consumerProps);
        KafkaPropertiesUtil.setClientIdPrefix(consumerProps, kafkaClusterId);

        KafkaSourceEnumerator enumerator =
                new KafkaSourceEnumerator(
                        KafkaSubscriber.getTopicListSubscriber(new ArrayList<>(topics)),
                        startingOffsetsInitializer,
                        stoppingOffsetInitializer,
                        consumerProps,
                        context,
                        boundedness,
                        kafkaSourceEnumState);

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
        // separate splits by cluster
        ArrayListMultimap<String, KafkaPartitionSplit> kafkaPartitionSplits =
                ArrayListMultimap.create();
        for (DynamicKafkaSourceSplit split : splits) {
            kafkaPartitionSplits.put(split.getKafkaClusterId(), split.getKafkaPartitionSplit());
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
        // assign pending splits from the sub enumerator
        clusterEnumeratorMap.forEach(
                (cluster, subEnumerator) -> subEnumerator.addReader(subtaskId));
        handleNoMoreSplits();
    }

    /**
     * Besides for checkpointing, this method is used in the restart sequence to retain the relevant
     * assigned splits so that there is no reader duplicate split assignment. See {@link
     * #createEnumeratorWithAssignedTopicPartitions(String, Set, KafkaSourceEnumState, Properties)}}
     */
    @Override
    public DynamicKafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        Map<String, KafkaSourceEnumState> subEnumeratorStateByCluster = new HashMap<>();

        // populate map for all assigned splits
        for (Entry<String, SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState>>
                clusterEnumerator : clusterEnumeratorMap.entrySet()) {
            subEnumeratorStateByCluster.put(
                    clusterEnumerator.getKey(),
                    clusterEnumerator.getValue().snapshotState(checkpointId));
        }

        return new DynamicKafkaSourceEnumState(latestKafkaStreams, subEnumeratorStateByCluster);
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
}
