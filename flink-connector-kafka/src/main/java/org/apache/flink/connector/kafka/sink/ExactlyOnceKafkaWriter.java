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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.ProducerPool;
import org.apache.flink.connector.kafka.sink.internal.ProducerPoolImpl;
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.FlinkRuntimeException;

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

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Exactly-once Kafka writer that writes records to Kafka in transactions.
 *
 * @param <IN> The type of the input elements.
 */
class ExactlyOnceKafkaWriter<IN> extends KafkaWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceKafkaWriter.class);
    private final String transactionalIdPrefix;

    private final KafkaWriterState kafkaWriterState;
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
            Collection<KafkaWriterState> recoveredStates) {
        super(
                deliveryGuarantee,
                kafkaProducerConfig,
                sinkInitContext,
                recordSerializer,
                schemaContext);
        this.transactionalIdPrefix =
                checkNotNull(transactionalIdPrefix, "transactionalIdPrefix must not be null");

        try {
            recordSerializer.open(schemaContext, kafkaSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.kafkaWriterState = new KafkaWriterState(transactionalIdPrefix);

        this.recoveredStates = checkNotNull(recoveredStates, "recoveredStates");
        initFlinkMetrics();
        restoredCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        int subtaskId = sinkInitContext.getTaskInfo().getIndexOfThisSubtask();
        this.producerPool = new ProducerPoolImpl(kafkaProducerConfig, this::initKafkaMetrics);
        this.backchannel =
                BackchannelFactory.getInstance()
                        .getReadableBackchannel(
                                subtaskId,
                                sinkInitContext.getTaskInfo().getAttemptNumber(),
                                transactionalIdPrefix);
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
        FlinkKafkaInternalProducer<byte[], byte[]> producer =
                producerPool.getTransactionalProducer(
                        TransactionalIdFactory.buildTransactionalId(
                                transactionalIdPrefix,
                                kafkaSinkContext.getParallelInstanceId(),
                                checkpointId),
                        checkpointId);
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
        currentProducer = startTransaction(checkpointId + 1);
        return Collections.singletonList(kafkaWriterState);
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

        try (TransactionAborter transactionAborter =
                new TransactionAborter(
                        kafkaSinkContext.getParallelInstanceId(),
                        kafkaSinkContext.getNumberOfParallelInstances(),
                        id -> producerPool.getTransactionalProducer(id, startCheckpointId),
                        producerPool::recycle)) {
            transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
        }
    }
}
