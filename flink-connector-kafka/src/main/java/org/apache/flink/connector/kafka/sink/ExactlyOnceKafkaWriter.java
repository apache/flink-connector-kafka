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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Exactly-once Kafka writer that writes records to Kafka in transactions.
 *
 * @param <IN> The type of the input elements.
 */
class ExactlyOnceKafkaWriter<IN> extends KafkaWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceKafkaWriter.class);
    private final String transactionalIdPrefix;

    private final KafkaWriterState kafkaWriterState;
    // producer pool only used for exactly once
    private final Deque<FlinkKafkaInternalProducer<byte[], byte[]>> producerPool =
            new ArrayDeque<>();
    private final Collection<KafkaWriterState> recoveredStates;
    private long lastCheckpointId;

    private final Deque<Closeable> producerCloseables = new ArrayDeque<>();

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
            Sink.InitContext sinkInitContext,
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
        this.lastCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);

        this.recoveredStates = checkNotNull(recoveredStates, "recoveredStates");
        initFlinkMetrics();
    }

    @Override
    public void initialize() {
        abortLingeringTransactions(recoveredStates, lastCheckpointId + 1);
        this.currentProducer = getTransactionalProducer(lastCheckpointId + 1);
        this.currentProducer.beginTransaction();
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() {
        // only return a KafkaCommittable if the current transaction has been written some data
        if (currentProducer.hasRecordsInTransaction()) {
            final List<KafkaCommittable> committables =
                    Collections.singletonList(
                            KafkaCommittable.of(currentProducer, producerPool::add));
            LOG.debug("Committing {} committables.", committables);
            return committables;
        }

        // otherwise, we commit the empty transaction as is (no-op) and just recycle the producer
        currentProducer.commitTransaction();
        producerPool.add(currentProducer);
        return Collections.emptyList();
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        currentProducer = getTransactionalProducer(checkpointId + 1);
        currentProducer.beginTransaction();
        return Collections.singletonList(kafkaWriterState);
    }

    @Override
    public void close() throws Exception {
        closeAll(
                this::abortCurrentProducer,
                () -> closeAll(producerPool),
                () -> closeAll(producerCloseables),
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
    Collection<FlinkKafkaInternalProducer<byte[], byte[]>> getProducerPool() {
        return producerPool;
    }

    private void abortLingeringTransactions(
            Collection<KafkaWriterState> recoveredStates, long startCheckpointId) {
        List<String> prefixesToAbort = new ArrayList<>();
        prefixesToAbort.add(transactionalIdPrefix);

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
                        this::getOrCreateTransactionalProducer,
                        producerPool::add)) {
            transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
        }
    }

    /**
     * For each checkpoint we create new {@link FlinkKafkaInternalProducer} so that new transactions
     * will not clash with transactions created during previous checkpoints ({@code
     * producer.initTransactions()} assures that we obtain new producerId and epoch counters).
     *
     * <p>Ensures that all transaction ids in between lastCheckpointId and checkpointId are
     * initialized.
     */
    private FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(long checkpointId) {
        checkState(
                checkpointId > lastCheckpointId,
                "Expected %s > %s",
                checkpointId,
                lastCheckpointId);
        FlinkKafkaInternalProducer<byte[], byte[]> producer = null;
        // in case checkpoints have been aborted, Flink would create non-consecutive transaction ids
        // this loop ensures that all gaps are filled with initialized (empty) transactions
        for (long id = lastCheckpointId + 1; id <= checkpointId; id++) {
            String transactionalId =
                    TransactionalIdFactory.buildTransactionalId(
                            transactionalIdPrefix, kafkaSinkContext.getParallelInstanceId(), id);
            producer = getOrCreateTransactionalProducer(transactionalId);
        }
        this.lastCheckpointId = checkpointId;
        assert producer != null;
        LOG.info("Created new transactional producer {}", producer.getTransactionalId());
        return producer;
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> getOrCreateTransactionalProducer(
            String transactionalId) {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = producerPool.poll();
        if (producer == null) {
            producer = new FlinkKafkaInternalProducer<>(kafkaProducerConfig, transactionalId);
            producerCloseables.add(producer);
            producer.initTransactions();
            initKafkaMetrics(producer);
        } else {
            producer.initTransactionId(transactionalId);
        }
        return producer;
    }
}
