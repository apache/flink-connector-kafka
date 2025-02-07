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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Manages a pool of {@link FlinkKafkaInternalProducer} instances for reuse in the {@code
 * KafkaWriter} and keeps track of the used transactional ids.
 *
 * <p>Reusing the producers is important for performance reasons. The producer initialization
 * includes a few requests to the broker (e.g., ApiVersion), which can be avoided with reuse.
 *
 * <p>Tracking the transactional ids in use can be a tricky because the {@code KafkaCommitter} is
 * ultimately finishing the transactions. There are two major cases:
 *
 * <ul>
 *   <li>The committer is chained to the writer (common case): The {@code KafkaCommittable} contains
 *       the producer (in-memory transfer) and the producer is only returned to the producer pool
 *       upon completion by the committer. Thus, none of the producers in the pool have active
 *       transactions.
 *   <li>The committer is not chained: The {@code KafkaCommittableSerializer} will return the
 *       producer to this pool, but it still has an ongoing transaction. The producer will be
 *       "cloned" in the committer by using producer id and epoch. In this case, we rely on {@link
 *       org.apache.kafka.common.errors.ProducerFencedException} to test later if a producer in the
 *       pool is still in the transaction or not.
 * </ul>
 *
 * <p>This pool is not thread-safe and is only intended to be accessed from the writer, which owns
 * it.
 */
@NotThreadSafe
public class ProducerPoolImpl implements ProducerPool {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerPoolImpl.class);

    /**
     * The configuration for the Kafka producer. This is used to create new producers when the pool
     * is empty.
     */
    private final Properties kafkaProducerConfig;
    /** Callback to allow the writer to init metrics. */
    private final Consumer<FlinkKafkaInternalProducer<byte[], byte[]>> producerInit;
    /**
     * The pool of producers that are available for reuse. This pool is used to avoid creating new
     * producers for every transaction.
     */
    private final Deque<FlinkKafkaInternalProducer<byte[], byte[]>> producerPool =
            new ArrayDeque<>();
    /**
     * The map of ongoing transactions (id -> producer/CheckpointTransaction). This is used to keep
     * track of the transactions that are ongoing and the respective producers are not in the pool.
     */
    private final Map<String, ProducerEntry> producerByTransactionalId = new TreeMap<>();
    /**
     * A secondary tracking structure to quickly find transactions coming from an earlier
     * checkpoints.
     */
    private final NavigableMap<CheckpointTransaction, String> transactionalIdsByCheckpoint =
            new TreeMap<>(Comparator.comparing(CheckpointTransaction::getCheckpointId));

    /** Creates a new {@link ProducerPoolImpl}. */
    public ProducerPoolImpl(
            Properties kafkaProducerConfig,
            Consumer<FlinkKafkaInternalProducer<byte[], byte[]>> producerInit) {
        this.kafkaProducerConfig =
                checkNotNull(kafkaProducerConfig, "kafkaProducerConfig must not be null");
        this.producerInit = checkNotNull(producerInit, "producerInit must not be null");
    }

    @Override
    public void recycleByTransactionId(String transactionalId) {
        ProducerEntry producerEntry = producerByTransactionalId.remove(transactionalId);
        LOG.debug("Transaction {} finished, producer {}", transactionalId, producerEntry);
        if (producerEntry == null) {
            // during recovery, the committer may finish transactions that are not yet ongoing from
            // the writer's perspective
            return;
        }

        transactionalIdsByCheckpoint.remove(producerEntry.getCheckpointedTransaction());
        recycleProducer(producerEntry.getProducer());

        // In rare cases (only for non-chained committer), some transactions may not be detected to
        // be finished.
        // For example, a transaction may be committed at the same time the writer state is
        // snapshot. The writer contains the transaction as ongoing but the committer state will
        // later not contain it.
        // In these cases, we make use of the fact that committables are processed in order of the
        // checkpoint id.
        // That means a transaction state with checkpoint id C implies that all C' < C are finished.
        NavigableMap<CheckpointTransaction, String> earlierTransactions =
                transactionalIdsByCheckpoint.headMap(
                        producerEntry.getCheckpointedTransaction(), false);
        if (!earlierTransactions.isEmpty()) {
            for (String id : earlierTransactions.values()) {
                ProducerEntry entry = producerByTransactionalId.remove(id);
                recycleProducer(entry.getProducer());
            }
            earlierTransactions.clear();
        }
    }

    @Override
    public void recycle(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        recycleProducer(producer);
        ProducerEntry producerEntry =
                producerByTransactionalId.remove(producer.getTransactionalId());
        transactionalIdsByCheckpoint.remove(producerEntry.getCheckpointedTransaction());
    }

    private void recycleProducer(@Nullable FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        // In case of recovery, we don't create a producer for the ongoing transactions.
        // The producer is just initialized on committer side.
        if (producer == null) {
            return;
        }
        // For non-chained committer, we have a split brain scenario:
        // Both the writer and the committer have a producer representing the same transaction.
        // The committer producer has finished the transaction while the writer producer is still in
        // transaction. In this case, we forcibly complete the transaction, such that we can
        // initialize it.
        if (producer.isInTransaction()) {
            producer.resetTransactionState();
        }
        producerPool.add(producer);

        LOG.debug("Recycling {}, new pool size {}", producer, producerPool.size());
    }

    @Override
    public FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(
            String transactionalId, long checkpointId) {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = producerPool.poll();
        if (producer == null) {
            producer = new FlinkKafkaInternalProducer<>(kafkaProducerConfig, transactionalId);
            producerInit.accept(producer);
        } else if (transactionalId != null) {
            producer.setTransactionId(transactionalId);
        }
        if (transactionalId != null) {
            // first keep track of the transaction and producer because initTransaction may be
            // interrupted
            CheckpointTransaction checkpointedTransaction =
                    new CheckpointTransaction(transactionalId, checkpointId);
            ProducerEntry existing =
                    producerByTransactionalId.put(
                            transactionalId, new ProducerEntry(producer, checkpointedTransaction));
            transactionalIdsByCheckpoint.put(checkpointedTransaction, transactionalId);
            checkState(
                    existing == null,
                    "Transaction %s already ongoing existing producer %s; new producer %s",
                    transactionalId,
                    existing,
                    producer);
            producer.initTransactions();
        }
        LOG.debug("getProducer {}, new pool size {}", producer, producerPool.size());
        return producer;
    }

    @VisibleForTesting
    public Collection<FlinkKafkaInternalProducer<byte[], byte[]>> getProducers() {
        return producerPool;
    }

    @Override
    public void close() throws Exception {
        LOG.debug(
                "Closing used producers {} and free producers {}",
                producerByTransactionalId,
                producerPool);
        closeAll(
                () -> closeAll(producerPool),
                () ->
                        closeAll(
                                producerByTransactionalId.values().stream()
                                        .map(ProducerEntry::getProducer)
                                        .collect(Collectors.toList())),
                producerPool::clear,
                producerByTransactionalId::clear);
    }

    private static class ProducerEntry {
        private final FlinkKafkaInternalProducer<byte[], byte[]> producer;
        private final CheckpointTransaction checkpointedTransaction;

        private ProducerEntry(
                FlinkKafkaInternalProducer<byte[], byte[]> producer,
                CheckpointTransaction checkpointedTransaction) {
            this.producer = checkNotNull(producer, "producer must not be null");
            this.checkpointedTransaction =
                    checkNotNull(
                            checkpointedTransaction, "checkpointedTransaction must not be null");
        }

        public CheckpointTransaction getCheckpointedTransaction() {
            return checkpointedTransaction;
        }

        public FlinkKafkaInternalProducer<byte[], byte[]> getProducer() {
            return producer;
        }

        @Override
        public String toString() {
            return producer.toString();
        }
    }
}
