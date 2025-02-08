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
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;

import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** Implementation of {@link TransactionNamingStrategy}. */
@Internal
public enum TransactionNamingStrategyImpl {
    INCREMENTING(TransactionOwnership.IMPLICIT_BY_SUBTASK_ID, false) {
        /**
         * For each checkpoint we create new {@link FlinkKafkaInternalProducer} so that new
         * transactions will not clash with transactions created during previous checkpoints ({@code
         * producer.initTransactions()} assures that we obtain new producerId and epoch counters).
         *
         * <p>Ensures that all transaction ids in between lastCheckpointId and checkpointId are
         * initialized.
         */
        @Override
        public FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(
                Context context) {
            long expectedCheckpointId = context.getNextCheckpointId();
            long lastCheckpointId = context.getLastCheckpointId();
            checkState(
                    expectedCheckpointId > lastCheckpointId,
                    "Expected %s > %s",
                    expectedCheckpointId,
                    lastCheckpointId);
            // in case checkpoints have been aborted, Flink would create non-consecutive transaction
            // ids
            // this loop ensures that all gaps are filled with initialized (empty) transactions
            for (long checkpointId = lastCheckpointId + 1;
                    checkpointId < expectedCheckpointId;
                    checkpointId++) {
                context.recycle(context.getProducer(context.buildTransactionalId(checkpointId)));
            }
            return context.getProducer(context.buildTransactionalId(expectedCheckpointId));
        }
    },
    POOLING(TransactionOwnership.EXPLICIT_BY_WRITER_STATE, true) {
        @Override
        public FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(
                Context context) {
            Set<String> usedTransactionalIds = context.getOngoingTransactions();
            for (int offset = 0; ; offset++) {
                String transactionalIdCandidate = context.buildTransactionalId(offset);
                if (usedTransactionalIds.contains(transactionalIdCandidate)) {
                    continue;
                }
                return context.getProducer(transactionalIdCandidate);
            }
        }
    };

    private final TransactionOwnership ownership;
    private final boolean requiresKnownTopics;

    TransactionNamingStrategyImpl(TransactionOwnership ownership, boolean requiresKnownTopics) {
        this.ownership = ownership;
        this.requiresKnownTopics = requiresKnownTopics;
    }

    public boolean requiresKnownTopics() {
        return requiresKnownTopics;
    }

    public TransactionOwnership getOwnership() {
        return ownership;
    }

    /**
     * Returns a {@link FlinkKafkaInternalProducer} that will not clash with any ongoing
     * transactions.
     */
    public abstract FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(
            Context context);

    /** Context for the transaction naming strategy. */
    public interface Context {
        String buildTransactionalId(long offset);

        long getNextCheckpointId();

        Set<String> getOngoingTransactions();

        long getLastCheckpointId();

        FlinkKafkaInternalProducer<byte[], byte[]> getProducer(String transactionalId);

        void recycle(FlinkKafkaInternalProducer<byte[], byte[]> producer);
    }
}
