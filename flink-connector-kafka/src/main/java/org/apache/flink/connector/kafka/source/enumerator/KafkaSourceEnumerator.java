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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkState;

/** The enumerator class for Kafka source. */
@Internal
public class KafkaSourceEnumerator
        implements SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceEnumerator.class);
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
    private final Map<Integer, Set<KafkaPartitionSplit>> pendingPartitionSplitAssignment;

    /** The consumer group id used for this KafkaSource. */
    private final String consumerGroupId;

    // Lazily instantiated or mutable fields.
    private AdminClient adminClient;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;
    // this flag will be marked as true if initial partitions are discovered after enumerator starts
    // the flag is read and set in main thread but also read in worker thread
    private volatile boolean initialDiscoveryFinished;

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
                new KafkaSourceEnumState(Collections.emptySet(), false));
    }

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            KafkaSourceEnumState kafkaSourceEnumState) {
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest();
        this.properties = properties;
        this.context = context;
        this.boundedness = boundedness;

        Map<AssignmentStatus, List<KafkaPartitionSplit>> splits =
                initializeMigratedSplits(kafkaSourceEnumState.splits());
        this.assignedSplits = indexByPartition(splits.get(AssignmentStatus.ASSIGNED));
        this.unassignedSplits = indexByPartition(splits.get(AssignmentStatus.UNASSIGNED));
        this.pendingPartitionSplitAssignment = new HashMap<>();
        this.partitionDiscoveryIntervalMs =
                KafkaSourceOptions.getOption(
                        properties,
                        KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                        Long::parseLong);
        this.consumerGroupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        this.initialDiscoveryFinished = kafkaSourceEnumState.initialDiscoveryFinished();
    }

    /**
     * Initialize migrated splits to splits with concrete starting offsets. This method ensures that
     * the costly offset resolution is performed only when there are splits that have been
     * checkpointed with previous enumerator versions.
     *
     * <p>Note that this method is deliberately performed in the main thread to avoid a checkpoint
     * of the splits without starting offset.
     */
    private Map<AssignmentStatus, List<KafkaPartitionSplit>> initializeMigratedSplits(
            Set<SplitAndAssignmentStatus> splits) {
        final Set<TopicPartition> migratedPartitions =
                splits.stream()
                        .filter(
                                splitStatus ->
                                        splitStatus.split().getStartingOffset()
                                                == KafkaPartitionSplit.MIGRATED)
                        .map(splitStatus -> splitStatus.split().getTopicPartition())
                        .collect(Collectors.toSet());

        if (migratedPartitions.isEmpty()) {
            return splitByAssignmentStatus(splits.stream());
        }

        final Map<TopicPartition, Long> startOffsets =
                startingOffsetInitializer.getPartitionOffsets(
                        migratedPartitions, getOffsetsRetriever());
        return splitByAssignmentStatus(
                splits.stream()
                        .map(splitStatus -> resolveMigratedSplit(splitStatus, startOffsets)));
    }

    private static Map<AssignmentStatus, List<KafkaPartitionSplit>> splitByAssignmentStatus(
            Stream<SplitAndAssignmentStatus> stream) {
        return stream.collect(
                Collectors.groupingBy(
                        SplitAndAssignmentStatus::assignmentStatus,
                        Collectors.mapping(SplitAndAssignmentStatus::split, Collectors.toList())));
    }

    private static SplitAndAssignmentStatus resolveMigratedSplit(
            SplitAndAssignmentStatus splitStatus, Map<TopicPartition, Long> startOffsets) {
        final KafkaPartitionSplit split = splitStatus.split();
        if (split.getStartingOffset() != KafkaPartitionSplit.MIGRATED) {
            return splitStatus;
        }
        final Long startOffset = startOffsets.get(split.getTopicPartition());
        checkState(
                startOffset != null,
                "Cannot find starting offset for migrated partition %s",
                split.getTopicPartition());
        return new SplitAndAssignmentStatus(
                new KafkaPartitionSplit(split.getTopicPartition(), startOffset),
                splitStatus.assignmentStatus());
    }

    private Map<TopicPartition, KafkaPartitionSplit> indexByPartition(
            List<KafkaPartitionSplit> splits) {
        if (splits == null) {
            return new HashMap<>();
        }
        return splits.stream()
                .collect(Collectors.toMap(KafkaPartitionSplit::getTopicPartition, split -> split));
    }

    /**
     * Start the enumerator.
     *
     * <p>Depending on {@link #partitionDiscoveryIntervalMs}, the enumerator will trigger a one-time
     * partition discovery, or schedule a callable for discover partitions periodically.
     *
     * <p>The invoking chain of partition discovery would be:
     *
     * <ol>
     *   <li>{@link #findNewPartitionSplits} in worker thread
     *   <li>{@link #handlePartitionSplitChanges} in coordinator thread
     * </ol>
     */
    @Override
    public void start() {
        adminClient = getKafkaAdminClient();
        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    consumerGroupId,
                    partitionDiscoveryIntervalMs);
            context.callAsync(
                    this::findNewPartitionSplits,
                    this::handlePartitionSplitChanges,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(this::findNewPartitionSplits, this::handlePartitionSplitChanges);
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
    private PartitionSplitChange findNewPartitionSplits() {
        final Set<TopicPartition> fetchedPartitions =
                subscriber.getSubscribedTopicPartitions(adminClient);

        final PartitionChange partitionChange = getPartitionChange(fetchedPartitions);
        if (partitionChange.isEmpty()) {
            return null;
        }

        return initializePartitionSplits(partitionChange);
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
        Set<TopicPartition> newPartitions =
                Collections.unmodifiableSet(partitionChange.getNewPartitions());

        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();
        // initial partitions use OffsetsInitializer specified by the user while new partitions use
        // EARLIEST
        final OffsetsInitializer initializer;
        if (!initialDiscoveryFinished) {
            initializer = startingOffsetInitializer;
        } else {
            initializer = newDiscoveryOffsetsInitializer;
        }
        Map<TopicPartition, Long> startingOffsets =
                initializer.getPartitionOffsets(newPartitions, offsetsRetriever);

        Map<TopicPartition, Long> stoppingOffsets =
                stoppingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);

        Set<KafkaPartitionSplit> partitionSplits = new HashSet<>(newPartitions.size());
        for (TopicPartition tp : newPartitions) {
            Long startingOffset = startingOffsets.get(tp);
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(tp, KafkaPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new KafkaPartitionSplit(tp, startingOffset, stoppingOffset));
        }
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
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
            @Nullable PartitionSplitChange partitionSplitChange, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        initialDiscoveryFinished = true;
        if (partitionDiscoveryIntervalMs <= 0) {
            LOG.debug("Partition discovery is disabled.");
            noMoreNewPartitionSplits = true;
        }
        if (partitionSplitChange == null) {
            return;
        }
        for (KafkaPartitionSplit split : partitionSplitChange.newPartitionSplits) {
            unassignedSplits.put(split.getTopicPartition(), split);
        }
        // TODO: Handle removed partitions.
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void addPartitionSplitChangeToPendingAssignments(
            Collection<KafkaPartitionSplit> newPartitionSplits) {
        int numReaders = context.currentParallelism();
        for (KafkaPartitionSplit split : newPartitionSplits) {
            int ownerReader = getSplitOwner(split.getTopicPartition(), numReaders);
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
    PartitionChange getPartitionChange(Set<TopicPartition> fetchedPartitions) {
        final Set<TopicPartition> removedPartitions = new HashSet<>();
        Consumer<TopicPartition> dedupOrMarkAsRemoved =
                (tp) -> {
                    if (!fetchedPartitions.remove(tp)) {
                        removedPartitions.add(tp);
                    }
                };

        assignedSplits.keySet().forEach(dedupOrMarkAsRemoved);
        pendingPartitionSplitAssignment.forEach(
                (reader, splits) ->
                        splits.forEach(
                                split -> dedupOrMarkAsRemoved.accept(split.getTopicPartition())));

        if (!fetchedPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", fetchedPartitions);
        }
        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }

        return new PartitionChange(fetchedPartitions, removedPartitions);
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
        private final Set<TopicPartition> newPartitions;
        private final Set<TopicPartition> removedPartitions;

        PartitionChange(Set<TopicPartition> newPartitions, Set<TopicPartition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<TopicPartition> getNewPartitions() {
            return newPartitions;
        }

        public Set<TopicPartition> getRemovedPartitions() {
            return removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
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
    }

    /** The implementation for offsets retriever with a consumer and an admin client. */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements OffsetsInitializer.PartitionOffsetsRetriever, AutoCloseable {
        private final AdminClient adminClient;
        private final String groupId;

        public PartitionOffsetsRetrieverImpl(AdminClient adminClient, String groupId) {
            this.adminClient = adminClient;
            this.groupId = groupId;
        }

        @Override
        public Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions) {
            ListConsumerGroupOffsetsSpec groupSpec = 
                    new ListConsumerGroupOffsetsSpec()
                            .topicPartitions(new ArrayList<>(partitions));
            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = Collections.singletonMap(groupId, groupSpec);
            ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
            try {
                return adminClient
                        .listConsumerGroupOffsets(groupSpecs, options)
                        .all()
                        .thenApply(
                                result -> {
                                    Map<TopicPartition, Long> offsets = new HashMap<>();
                                    result.get(groupId).forEach(
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

        @Override
        public void close() throws Exception {
            adminClient.close(Duration.ZERO);
        }
    }
}
