/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.ProducerPool;
import org.apache.flink.connector.kafka.sink.internal.ProducerPoolImpl;
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyContextImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyContextImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;
import org.apache.flink.connector.kafka.util.AdminUtils;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Exactly-once Kafka writer that writes records to Kafka in transactions.
 *
 * @param <IN> The type of the input elements.
 */
class ExactlyOnceKafkaWriter<IN> extends KafkaWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceKafkaWriter.class);
    /**
     * Prefix for the transactional id. Must be unique across all sinks writing to the same broker.
     */
    private final String transactionalIdPrefix;
    /**
     * Strategy to abort lingering transactions from previous executions during writer
     * initialization.
     */
    private final TransactionAbortStrategyImpl transactionAbortStrategy;
    /** Strategy to name transactions. */
    private final TransactionNamingStrategyImpl transactionNamingStrategy;

    private final Collection<KafkaWriterState> recoveredStates;
    private final long restoredCheckpointId;

    /**
     * The producer pool that manages all transactional producers. It keeps track of the producers
     * that have been recycled as well as producers that are currently in use (potentially forwarded
     * to committer).
     */
    private final ProducerPool producerPool;
    /**
     * Backchannel used to communicate committed transactions from the committer to this writer.
     * Establishing the channel happens during recovery. Thus, it is only safe to poll in checkpoint
     * related methods.
     */
    private final ReadableBackchannel<TransactionFinished> backchannel;
    /** The context used to name transactions. */
    private final TransactionNamingStrategyContextImpl namingContext;

    private final int totalNumberOfOwnedSubtasks;
    private final int[] ownedSubtaskIds;
    /** Lazily created admin client for {@link TransactionAbortStrategyImpl}. */
    private AdminClient adminClient;

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext,
     * KafkaRecordSerializationSchema.KafkaSinkContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param transactionalIdPrefix used to create the transactionalIds
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which was covered
     */
    ExactlyOnceKafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            WriterInitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            TransactionAbortStrategyImpl transactionAbortStrategy,
            TransactionNamingStrategyImpl transactionNamingStrategy,
            Collection<KafkaWriterState> recoveredStates) {
        super(
                deliveryGuarantee,
                kafkaProducerConfig,
                sinkInitContext,
                recordSerializer,
                schemaContext);
        this.transactionalIdPrefix =
                checkNotNull(transactionalIdPrefix, "transactionalIdPrefix must not be null");
        this.transactionAbortStrategy =
                checkNotNull(transactionAbortStrategy, "transactionAbortStrategy must not be null");
        this.transactionNamingStrategy =
                checkNotNull(
                        transactionNamingStrategy, "transactionNamingStrategy must not be null");

        try {
            recordSerializer.open(schemaContext, kafkaSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.recoveredStates = checkNotNull(recoveredStates, "recoveredStates");
        TaskInfo taskInfo = sinkInitContext.getTaskInfo();
        TransactionOwnership ownership = transactionNamingStrategy.getOwnership();
        int subtaskId = taskInfo.getIndexOfThisSubtask();
        int parallelism = taskInfo.getNumberOfParallelSubtasks();
        this.ownedSubtaskIds =
                ownership.getOwnedSubtaskIds(subtaskId, parallelism, recoveredStates);
        this.totalNumberOfOwnedSubtasks =
                ownership.getTotalNumberOfOwnedSubtasks(subtaskId, parallelism, recoveredStates);
        initFlinkMetrics();
        restoredCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        this.producerPool =
                new ProducerPoolImpl(
                        kafkaProducerConfig,
                        this::initKafkaMetrics,
                        recoveredStates.stream()
                                .flatMap(r -> r.getPrecommittedTransactionalIds().stream())
                                .collect(Collectors.toList()));
        this.backchannel =
                BackchannelFactory.getInstance()
                        .getReadableBackchannel(
                                subtaskId, taskInfo.getAttemptNumber(), transactionalIdPrefix);
        this.namingContext =
                new TransactionNamingStrategyContextImpl(
                        transactionalIdPrefix,
                        this.ownedSubtaskIds[0],
                        restoredCheckpointId,
                        producerPool);
    }

    @Override
    public void initialize() {
        // Workaround for FLINK-37612: ensure that we are not leaking producers
        try {
            abortLingeringTransactions(
                    checkNotNull(recoveredStates, "recoveredStates"), restoredCheckpointId + 1);
            this.currentProducer = startTransaction(restoredCheckpointId + 1);
        } catch (Throwable t) {
            try {
                close();
            } catch (Exception e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> startTransaction(long checkpointId) {
        namingContext.setNextCheckpointId(checkpointId);
        namingContext.setOngoingTransactions(
                producerPool.getOngoingTransactions().stream()
                        .map(CheckpointTransaction::getTransactionalId)
                        .collect(Collectors.toSet()));
        FlinkKafkaInternalProducer<byte[], byte[]> producer =
                transactionNamingStrategy.getTransactionalProducer(namingContext);
        namingContext.setLastCheckpointId(checkpointId);
        producer.beginTransaction();
        return producer;
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() {
        // only return a KafkaCommittable if the current transaction has been written some data
        if (currentProducer.hasRecordsInTransaction()) {
            KafkaCommittable committable = KafkaCommittable.of(currentProducer);
            LOG.debug("Prepare {}.", committable);
            return Collections.singletonList(committable);
        }

        // otherwise, we recycle the producer (the pool will reset the transaction state)
        producerPool.recycle(currentProducer);
        return Collections.emptyList();
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        // recycle committed producers
        TransactionFinished finishedTransaction;
        while ((finishedTransaction = backchannel.poll()) != null) {
            producerPool.recycleByTransactionId(
                    finishedTransaction.getTransactionId(), finishedTransaction.isSuccess());
        }
        // persist the ongoing transactions into the state; these will not be aborted on restart
        Collection<CheckpointTransaction> ongoingTransactions =
                producerPool.getOngoingTransactions();
        currentProducer = startTransaction(checkpointId + 1);
        return createSnapshots(ongoingTransactions);
    }

    private List<KafkaWriterState> createSnapshots(
            Collection<CheckpointTransaction> ongoingTransactions) {
        List<KafkaWriterState> states = new ArrayList<>();
        int[] subtaskIds = this.ownedSubtaskIds;
        for (int index = 0; index < subtaskIds.length; index++) {
            int ownedSubtask = subtaskIds[index];
            states.add(
                    new KafkaWriterState(
                            transactionalIdPrefix,
                            ownedSubtask,
                            totalNumberOfOwnedSubtasks,
                            transactionNamingStrategy.getOwnership(),
                            // new transactions are only created with the first owned subtask id
                            index == 0 ? ongoingTransactions : List.of()));
        }
        LOG.debug("Snapshotting state {}", states);
        return states;
    }

    @Override
    public void close() throws Exception {
        closeAll(
                this::abortCurrentProducer,
                () -> closeAll(producerPool),
                backchannel,
                super::close);
    }

    private void abortCurrentProducer() {
        // only abort if the transaction is known to the broker (needs to have at least one record
        // sent)
        if (currentProducer.isInTransaction() && currentProducer.hasRecordsInTransaction()) {
            try {
                currentProducer.abortTransaction();
            } catch (ProducerFencedException e) {
                LOG.debug(
                        "Producer {} fenced while aborting", currentProducer.getTransactionalId());
            }
        }
    }

    @VisibleForTesting
    ProducerPool getProducerPool() {
        return producerPool;
    }

    @VisibleForTesting
    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    private void abortLingeringTransactions(
            Collection<KafkaWriterState> recoveredStates, long startCheckpointId) {
        List<String> prefixesToAbort = new ArrayList<>();
        prefixesToAbort.add(transactionalIdPrefix);

        LOG.info(
                "Aborting lingering transactions from previous execution. Recovered states: {}.",
                recoveredStates);
        final Optional<KafkaWriterState> lastStateOpt = recoveredStates.stream().findFirst();
        if (lastStateOpt.isPresent()) {
            KafkaWriterState lastState = lastStateOpt.get();
            if (!lastState.getTransactionalIdPrefix().equals(transactionalIdPrefix)) {
                prefixesToAbort.add(lastState.getTransactionalIdPrefix());
                LOG.warn(
                        "Transactional id prefix from previous execution {} has changed to {}.",
                        lastState.getTransactionalIdPrefix(),
                        transactionalIdPrefix);
            }
        }

        LOG.info(
                "Aborting lingering transactions with prefixes {} using {}",
                prefixesToAbort,
                transactionAbortStrategy);
        TransactionAbortStrategyContextImpl context =
                getTransactionAbortStrategyContext(startCheckpointId, prefixesToAbort);
        transactionAbortStrategy.abortTransactions(context);
    }

    private TransactionAbortStrategyContextImpl getTransactionAbortStrategyContext(
            long startCheckpointId, List<String> prefixesToAbort) {
        TransactionAbortStrategyImpl.TransactionAborter aborter =
                transactionalId -> {
                    // getTransactionalProducer already calls initTransactions, which cancels the
                    // transaction
                    FlinkKafkaInternalProducer<byte[], byte[]> producer =
                            producerPool.getTransactionalProducer(transactionalId, 0);
                    LOG.debug("Aborting transaction {}", transactionalId);
                    producer.flush();
                    short epoch = producer.getEpoch();
                    producerPool.recycle(producer);
                    return epoch;
                };
        Set<String> precommittedTransactionalIds =
                recoveredStates.stream()
                        .flatMap(
                                s ->
                                        s.getPrecommittedTransactionalIds().stream()
                                                .map(CheckpointTransaction::getTransactionalId))
                        .collect(Collectors.toSet());
        return new TransactionAbortStrategyContextImpl(
                this::getTopicNames,
                kafkaSinkContext.getParallelInstanceId(),
                kafkaSinkContext.getNumberOfParallelInstances(),
                ownedSubtaskIds,
                totalNumberOfOwnedSubtasks,
                prefixesToAbort,
                startCheckpointId,
                aborter,
                this::getAdminClient,
                precommittedTransactionalIds);
    }

    private Collection<String> getTopicNames() {
        KafkaDatasetIdentifier identifier =
                getDatasetIdentifier()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The record serializer does not expose a static list of target topics."));
        if (identifier.getTopics() != null) {
            return identifier.getTopics();
        }
        return AdminUtils.getTopicsByPattern(getAdminClient(), identifier.getTopicPattern());
    }

    private Optional<KafkaDatasetIdentifier> getDatasetIdentifier() {
        if (recordSerializer instanceof KafkaDatasetFacetProvider) {
            Optional<KafkaDatasetFacet> kafkaDatasetFacet =
                    ((KafkaDatasetFacetProvider) recordSerializer).getKafkaDatasetFacet();

            return kafkaDatasetFacet.map(KafkaDatasetFacet::getTopicIdentifier);
        }
        return Optional.empty();
    }

    private Admin getAdminClient() {
        if (adminClient == null) {
            adminClient = AdminClient.create(kafkaProducerConfig);
        }
        return adminClient;
    }
}
