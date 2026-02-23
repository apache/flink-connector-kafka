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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.CollectionUtil.newLinkedHashMapWithExpectedSize;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The enumerator class for Kafka source.
 *
 * <p>A core part of the enumerator is handling discovered splits. The following lifecycle applies
 * to splits:
 *
 * <ol>
 *   <li>{@link #getSubscribedTopicPartitions()} initially or periodically retrieves a list of topic
 *       partitions in the worker thread.
 *   <li>Partitions are consolidated in {@link #checkPartitionChanges(Set, Throwable)} in the main
 *       thread.
 *   <li>New partitions will result in new splits, which are initialized through {@link
 *       #initializePartitionSplits(PartitionChange)} where start/offsets are resolved in the worker
 *       thread. Offset resolution happens through the {@link PartitionOffsetsRetrieverImpl} which
 *       communicates with the broker.
 *   <li>The new, initialized splits are put into {@link #unassignedSplits} and {@link
 *       #pendingPartitionSplitAssignment} in {@link
 *       #handlePartitionSplitChanges(PartitionSplitChange, Throwable)} in the main thread.
 *   <li>{@link #assignPendingPartitionSplits(Set)} eventually assigns the pending splits to readers
 *       at which point there are removed from {@link #unassignedSplits} and {@link
 *       #pendingPartitionSplitAssignment} and moved into {@link #assignedSplits} in the main
 *       thread.
 *   <li>Checkpointing is performed in the main thread on {@link #unassignedSplits} and {@link
 *       #assignedSplits}. Information in {@link #pendingPartitionSplitAssignment} is ephemeral
 *       because of FLINK-21817 (pretty much the actual assignment should be transient).
 *   <li>In case of state migration, the start offset of {@link #unassignedSplits} may not be
 *       initialized, so these partitions are reinjected into the discovery process during {@link
 *       #checkPartitionChanges(Set, Throwable)}.
 * </ol>
 */
@Internal
public class KafkaSourceEnumerator
        implements SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceEnumerator.class);

    /**
     * Selects the target reader for a split while it is enqueued in pending assignments.
     *
     * <p>The selector is invoked on the coordinator thread.
     */
    @FunctionalInterface
    public interface SplitOwnerSelector {
        int getSplitOwner(KafkaPartitionSplit split, int numReaders);
    }

    private final KafkaSubscriber subscriber;
    private final OffsetsInitializer startingOffsetInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final OffsetsInitializer newDiscoveryOffsetsInitializer;
    private final Properties properties;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<KafkaPartitionSplit> context;
    private final Boundedness boundedness;

    /** Partitions that have been assigned to readers. */
    private final Map<TopicPartition, KafkaPartitionSplit> assignedSplits;

    /**
     * The splits that have been discovered during initialization but not assigned to readers yet.
     */
    private final Map<TopicPartition, KafkaPartitionSplit> unassignedSplits;

    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be
     * ready.
     */
    private final Map<Integer, Set<KafkaPartitionSplit>> pendingPartitionSplitAssignment =
            new HashMap<>();

    private final SplitOwnerSelector splitOwnerSelector;

    /** The consumer group id used for this KafkaSource. */
    private final String consumerGroupId;

    // Lazily instantiated or mutable fields.
    private AdminClient adminClient;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;
    // this flag will be marked as true if initial partitions are discovered after enumerator starts
    private boolean initialDiscoveryFinished;

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness) {
        this(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                new KafkaSourceEnumState(Collections.emptySet(), false),
                null);
    }

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            KafkaSourceEnumState kafkaSourceEnumState) {
        this(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                kafkaSourceEnumState,
                null);
    }

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            KafkaSourceEnumState kafkaSourceEnumState,
            @Nullable SplitOwnerSelector splitOwnerSelector) {
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest();
        this.properties = properties;
        this.context = context;
        this.boundedness = boundedness;
        this.splitOwnerSelector =
                splitOwnerSelector != null
                        ? splitOwnerSelector
                        : (split, numReaders) ->
                                getSplitOwner(split.getTopicPartition(), numReaders);

        this.partitionDiscoveryIntervalMs =
                KafkaSourceOptions.getOption(
                        properties,
                        KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.consumerGroupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        this.initialDiscoveryFinished = kafkaSourceEnumState.initialDiscoveryFinished();
        this.assignedSplits = indexByPartition(kafkaSourceEnumState.assignedSplits());
        this.unassignedSplits = indexByPartition(kafkaSourceEnumState.unassignedSplits());
    }

    private static Map<TopicPartition, KafkaPartitionSplit> indexByPartition(
            Collection<KafkaPartitionSplit> splits) {
        return splits.stream()
                .collect(Collectors.toMap(KafkaPartitionSplit::getTopicPartition, e -> e));
    }

    /**
     * Start the enumerator.
     *
     * <p>Depending on {@link #partitionDiscoveryIntervalMs}, the enumerator will trigger a one-time
     * partition discovery, or schedule a callable for discover partitions periodically.
     */
    @Override
    public void start() {
        adminClient = getKafkaAdminClient();

        // Find splits where the start offset has been initialized but not yet assigned to readers.
        // These splits must not be reinitialized to keep offsets consistent with first discovery.
        final List<KafkaPartitionSplit> preinitializedSplits =
                unassignedSplits.values().stream()
                        .filter(split -> !split.isMigrated())
                        .collect(Collectors.toList());
        if (!preinitializedSplits.isEmpty()) {
            addPartitionSplitChangeToPendingAssignments(preinitializedSplits);
        }

        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    consumerGroupId,
                    partitionDiscoveryIntervalMs);
            context.callAsync(
                    this::getSubscribedTopicPartitions,
                    this::checkPartitionChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the kafka source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<KafkaPartitionSplit> splits, int subtaskId) {
        for (KafkaPartitionSplit split : splits) {
            unassignedSplits.put(split.getTopicPartition(), split);
            assignedSplits.remove(split.getTopicPartition());
        }
        addPartitionSplitChangeToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to KafkaSourceEnumerator for consumer group {}.",
                subtaskId,
                consumerGroupId);
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public KafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new KafkaSourceEnumState(
                assignedSplits.values(), unassignedSplits.values(), initialDiscoveryFinished);
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    // ----------------- private methods -------------------

    /**
     * List subscribed topic partitions on Kafka brokers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with Kafka brokers.
     *
     * @return Set of subscribed {@link TopicPartition}s
     */
    private Set<TopicPartition> getSubscribedTopicPartitions() {
        return subscriber.getSubscribedTopicPartitions(adminClient);
    }

    /**
     * Check if there's any partition changes within subscribed topic partitions fetched by worker
     * thread, and invoke {@link KafkaSourceEnumerator#initializePartitionSplits(PartitionChange)}
     * in worker thread to initialize splits for new partitions.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param fetchedPartitions Map from topic name to its description
     * @param t Exception in worker thread
     */
    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException(
                    "Failed to list subscribed topic partitions due to ", t);
        }

        final PartitionChange partitionChange =
                getPartitionChange(fetchedPartitions, !initialDiscoveryFinished);
        if (partitionChange.isEmpty()) {
            return;
        }
        context.callAsync(
                () -> initializePartitionSplits(partitionChange),
                this::handlePartitionSplitChanges);
    }

    /**
     * Initialize splits for newly discovered partitions.
     *
     * <p>Enumerator will be responsible for fetching offsets when initializing splits if:
     *
     * <ul>
     *   <li>using timestamp for initializing offset
     *   <li>or using specified offset, but the offset is not provided for the newly discovered
     *       partitions
     * </ul>
     *
     * <p>Otherwise offsets will be initialized by readers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * potentially requires network I/O with Kafka brokers for fetching offsets.
     *
     * @param partitionChange Newly discovered and removed partitions
     * @return {@link KafkaPartitionSplit} of new partitions and {@link TopicPartition} of removed
     *     partitions
     */
    private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        Set<TopicPartition> newPartitions = partitionChange.getNewPartitions();
        Set<TopicPartition> initialPartitions = partitionChange.getInitialPartitions();

        // initial partitions use OffsetsInitializer specified by the user while new partitions use
        // EARLIEST
        Map<TopicPartition, Long> startingOffsets =
                newLinkedHashMapWithExpectedSize(newPartitions.size() + initialPartitions.size());
        Map<TopicPartition, Long> stoppingOffsets =
                newLinkedHashMapWithExpectedSize(newPartitions.size() + initialPartitions.size());
        if (!newPartitions.isEmpty()) {
            initOffsets(
                    newPartitions,
                    newDiscoveryOffsetsInitializer,
                    startingOffsets,
                    stoppingOffsets);
        }
        if (!initialPartitions.isEmpty()) {
            initOffsets(
                    initialPartitions, startingOffsetInitializer, startingOffsets, stoppingOffsets);
        }

        Set<KafkaPartitionSplit> partitionSplits =
                new HashSet<>(newPartitions.size() + initialPartitions.size());
        for (Entry<TopicPartition, Long> tpAndStartingOffset : startingOffsets.entrySet()) {
            TopicPartition tp = tpAndStartingOffset.getKey();
            long startingOffset = tpAndStartingOffset.getValue();
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(tp, KafkaPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new KafkaPartitionSplit(tp, startingOffset, stoppingOffset));
        }
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }

    private void initOffsets(
            Set<TopicPartition> partitions,
            OffsetsInitializer startOffsetInitializer,
            Map<TopicPartition, Long> startingOffsets,
            Map<TopicPartition, Long> stoppingOffsets) {
        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();
        startingOffsets.putAll(
                startOffsetInitializer.getPartitionOffsets(partitions, offsetsRetriever));
        stoppingOffsets.putAll(
                stoppingOffsetInitializer.getPartitionOffsets(partitions, offsetsRetriever));
    }

    /**
     * Mark partition splits initialized by {@link
     * KafkaSourceEnumerator#initializePartitionSplits(PartitionChange)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param partitionSplitChange Partition split changes
     * @param t Exception in worker thread
     */
    private void handlePartitionSplitChanges(
            PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        initialDiscoveryFinished = true;
        if (partitionDiscoveryIntervalMs <= 0) {
            LOG.debug("Partition discovery is disabled.");
            noMoreNewPartitionSplits = true;
        }
        for (KafkaPartitionSplit split : partitionSplitChange.newPartitionSplits) {
            unassignedSplits.put(split.getTopicPartition(), split);
        }
        LOG.info("Partition split changes: {}", partitionSplitChange);
        // TODO: Handle removed partitions.
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<KafkaPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        List<KafkaPartitionSplit> sortedSplits = new ArrayList<>(newPartitionSplits);
        sortedSplits.sort(
                Comparator.comparing(
                                (KafkaPartitionSplit split) -> split.getTopicPartition().topic())
                        .thenComparingInt(split -> split.getTopicPartition().partition()));
        for (KafkaPartitionSplit split : sortedSplits) {
            int ownerReader = splitOwnerSelector.getSplitOwner(split, numReaders);
            checkState(
                    ownerReader >= 0 && ownerReader < numReaders,
                    "Invalid split owner %s for split %s with current parallelism %s",
                    ownerReader,
                    split,
                    numReaders);
            pendingPartitionSplitAssignment
                    .computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
        }
        LOG.debug(
                "Assigned {} to {} readers of consumer group {}.",
                newPartitionSplits,
                numReaders,
                consumerGroupId);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<KafkaPartitionSplit>> incrementalAssignment = new HashMap<>();

        // Check if there's any pending splits for given readers
        for (int pendingReader : pendingReaders) {
            checkReaderRegistered(pendingReader);

            // Remove pending assignment for the reader
            final Set<KafkaPartitionSplit> pendingAssignmentForReader =
                    pendingPartitionSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // Mark pending partitions as already assigned
                pendingAssignmentForReader.forEach(
                        split -> {
                            assignedSplits.put(split.getTopicPartition(), split);
                            unassignedSplits.remove(split.getTopicPartition());
                        });
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
            LOG.debug(
                    "No more KafkaPartitionSplits to assign. Sending NoMoreSplitsEvent to reader {}"
                            + " in consumer group {}.",
                    pendingReaders,
                    consumerGroupId);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @VisibleForTesting
    PartitionChange getPartitionChange(
            Set<TopicPartition> fetchedPartitions, boolean initialDiscovery) {
        final Set<TopicPartition> removedPartitions = new HashSet<>();
        Set<TopicPartition> newPartitions = new HashSet<>(fetchedPartitions);
        Consumer<TopicPartition> dedupOrMarkAsRemoved =
                (tp) -> {
                    if (!newPartitions.remove(tp)) {
                        removedPartitions.add(tp);
                    }
                };

        assignedSplits.keySet().forEach(dedupOrMarkAsRemoved);
        pendingPartitionSplitAssignment.forEach(
                (reader, splits) ->
                        splits.forEach(
                                split -> dedupOrMarkAsRemoved.accept(split.getTopicPartition())));

        if (!newPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", newPartitions);
        }
        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }

        Set<TopicPartition> initialPartitions = new HashSet<>();
        if (initialDiscovery) {
            initialPartitions.addAll(newPartitions);
            newPartitions.clear();
        }
        // migration path, ensure that partitions without offset are properly initialized
        for (KafkaPartitionSplit split : unassignedSplits.values()) {
            if (split.isMigrated()) {
                initialPartitions.add(split.getTopicPartition());
            }
        }
        return new PartitionChange(initialPartitions, newPartitions, removedPartitions);
    }

    private AdminClient getKafkaAdminClient() {
        Properties adminClientProps = new Properties();
        deepCopyProperties(properties, adminClientProps);
        // set client id prefix
        String clientIdPrefix =
                adminClientProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        adminClientProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-enumerator-admin-client");
        return AdminClient.create(adminClientProps);
    }

    private OffsetsInitializer.PartitionOffsetsRetriever getOffsetsRetriever() {
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        return new PartitionOffsetsRetrieverImpl(adminClient, groupId);
    }

    @VisibleForTesting
    Map<Integer, Set<KafkaPartitionSplit>> getPendingPartitionSplitAssignment() {
        return pendingPartitionSplitAssignment;
    }

    /**
     * Returns the index of the target subtask that a specific Kafka partition should be assigned
     * to.
     *
     * <p>The resulting distribution of partitions of a single topic has the following contract:
     *
     * <ul>
     *   <li>1. Uniformly distributed across subtasks
     *   <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending subtask
     *       indices) by using the partition id as the offset from a starting index (i.e., the index
     *       of the subtask which partition 0 of the topic will be assigned to, determined using the
     *       topic name).
     * </ul>
     *
     * @param tp the Kafka partition to assign.
     * @param numReaders the total number of readers.
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of Kafka partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + tp.partition()) % numReaders;
    }

    @VisibleForTesting
    static void deepCopyProperties(Properties from, Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    // --------------- private class ---------------

    /** A container class to hold the newly added partitions and removed partitions. */
    @VisibleForTesting
    static class PartitionChange {
        private final Set<TopicPartition> initialPartitions;
        private final Set<TopicPartition> newPartitions;
        private final Set<TopicPartition> removedPartitions;

        PartitionChange(
                Set<TopicPartition> initialPartitions,
                Set<TopicPartition> newPartitions,
                Set<TopicPartition> removedPartitions) {
            this.initialPartitions = initialPartitions;
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<TopicPartition> getInitialPartitions() {
            return initialPartitions;
        }

        public Set<TopicPartition> getNewPartitions() {
            return newPartitions;
        }

        public Set<TopicPartition> getRemovedPartitions() {
            return removedPartitions;
        }

        public boolean isEmpty() {
            return initialPartitions.isEmpty()
                    && newPartitions.isEmpty()
                    && removedPartitions.isEmpty();
        }
    }

    private static class PartitionSplitChange {
        private final Set<KafkaPartitionSplit> newPartitionSplits;
        private final Set<TopicPartition> removedPartitions;

        private PartitionSplitChange(
                Set<KafkaPartitionSplit> newPartitionSplits,
                Set<TopicPartition> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }

        @Override
        public String toString() {
            return "PartitionSplitChange{"
                    + "newPartitionSplits="
                    + newPartitionSplits
                    + ", removedPartitions="
                    + removedPartitions
                    + '}';
        }
    }

    /** The implementation for offsets retriever with a consumer and an admin client. */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements OffsetsInitializer.PartitionOffsetsRetriever {
        private final AdminClient adminClient;
        private final String groupId;

        public PartitionOffsetsRetrieverImpl(AdminClient adminClient, String groupId) {
            this.adminClient = checkNotNull(adminClient);
            this.groupId = groupId;
        }

        @Override
        public Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions) {
            ListConsumerGroupOffsetsSpec offsetsSpec =
                    new ListConsumerGroupOffsetsSpec().topicPartitions(partitions);
            try {
                return adminClient
                        .listConsumerGroupOffsets(Collections.singletonMap(groupId, offsetsSpec))
                        .partitionsToOffsetAndMetadata()
                        .thenApply(
                                result -> {
                                    Map<TopicPartition, Long> offsets = new HashMap<>();
                                    result.forEach(
                                            (tp, oam) -> {
                                                if (oam != null) {
                                                    offsets.put(tp, oam.offset());
                                                }
                                            });
                                    return offsets;
                                })
                        .get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for consumer group " + groupId, e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to fetch committed offsets for consumer group "
                                + groupId
                                + " due to",
                        e);
            }
        }

        /**
         * List offsets for the specified partitions and OffsetSpec. This operation enables to find
         * the beginning offset, end offset as well as the offset matching a timestamp in
         * partitions.
         *
         * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
         * @return The list offsets result.
         * @see KafkaAdminClient#listOffsets(Map)
         */
        private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets(
                Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
            try {
                return adminClient
                        .listOffsets(topicPartitionOffsets)
                        .all()
                        .thenApply(
                                result -> {
                                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
                                            offsets = new HashMap<>();
                                    result.forEach(
                                            (tp, listOffsetsResultInfo) -> {
                                                if (listOffsetsResultInfo != null) {
                                                    offsets.put(tp, listOffsetsResultInfo);
                                                }
                                            });
                                    return offsets;
                                })
                        .get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for topic partitions: "
                                + topicPartitionOffsets,
                        e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to list offsets for topic partitions: "
                                + topicPartitionOffsets
                                + " due to",
                        e);
            }
        }

        private Map<TopicPartition, Long> listOffsets(
                Collection<TopicPartition> partitions, OffsetSpec offsetSpec) {
            return listOffsets(
                            partitions.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    partition -> partition, __ -> offsetSpec)))
                    .entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey, entry -> entry.getValue().offset()));
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            return listOffsets(partitions, OffsetSpec.latest());
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return listOffsets(partitions, OffsetSpec.earliest());
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                Map<TopicPartition, Long> timestampsToSearch) {
            return listOffsets(
                            timestampsToSearch.entrySet().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    Map.Entry::getKey,
                                                    entry ->
                                                            OffsetSpec.forTimestamp(
                                                                    entry.getValue()))))
                    .entrySet().stream()
                    // OffsetAndTimestamp cannot be initialized with a negative offset, which is
                    // possible if the timestamp does not correspond to an offset and the topic
                    // partition is empty
                    .filter(entry -> entry.getValue().offset() >= 0)
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry ->
                                            new OffsetAndTimestamp(
                                                    entry.getValue().offset(),
                                                    entry.getValue().timestamp(),
                                                    entry.getValue().leaderEpoch())));
        }
    }
}
