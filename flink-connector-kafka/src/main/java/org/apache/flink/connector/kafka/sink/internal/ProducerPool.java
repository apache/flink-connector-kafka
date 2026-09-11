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

import org.apache.flink.annotation.Internal;

import java.util.Collection;

/** A pool of producers that can be recycled. */
@Internal
public interface ProducerPool extends AutoCloseable {
    /**
     * Notify the pool that a transaction has finished. The producer with the given transactional id
     * can be recycled.
     */
    void recycleByTransactionId(String transactionalId, boolean success);

    /**
     * Get a producer for the given transactional id and checkpoint id. The producer is not recycled
     * until it is passed to the committer, the committer commits the transaction, and {@link
     * #recycleByTransactionId(String, boolean)} is called. Alternatively, the producer can be
     * recycled by {@link #recycle(FlinkKafkaInternalProducer)}.
     */
    FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(
            String transactionalId, long checkpointId);

    /**
     * Returns a snapshot of all ongoing transactions. Transactions opened by this pool carry the
     * producer id and epoch of their producer; transactions restored from state keep whatever the
     * state recorded.
     */
    Collection<CheckpointTransaction> getOngoingTransactions();

    /**
     * Stops tracking a transaction that was restored from state without touching a producer. Used
     * on recovery when the broker no longer holds the restored transaction under its transactional
     * id because the id was reused for a later checkpoint, so that the id can be aborted and
     * reused.
     *
     * @throws IllegalStateException if the id is not a restored transaction or has a live producer
     */
    void abandonTransaction(String transactionalId);

    /**
     * Explicitly recycle a producer. This is useful when the producer has not been passed to the
     * committer.
     */
    void recycle(FlinkKafkaInternalProducer<byte[], byte[]> producer);
}
