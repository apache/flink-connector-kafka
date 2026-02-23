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
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl.TransactionAborter;
import org.apache.flink.connector.kafka.util.AdminUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionListing;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.extractSubtaskId;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link TransactionAbortStrategyImpl.Context}. */
@Internal
public class TransactionAbortStrategyContextImpl implements TransactionAbortStrategyImpl.Context {
    private final Supplier<Admin> adminSupplier;
    private final Supplier<Collection<String>> topicNames;
    private final int currentSubtaskId;
    private final int currentParallelism;
    private final Set<Integer> ownedSubtaskIds;
    private final int totalNumberOfOwnedSubtasks;
    private final Set<String> prefixesToAbort;
    private final long startCheckpointId;
    private final TransactionAborter transactionAborter;

    /** Transactional ids that mustn't be aborted. */
    private final Set<String> precommittedTransactionIds;

    /** Creates a new {@link TransactionAbortStrategyContextImpl}. */
    public TransactionAbortStrategyContextImpl(
            Supplier<Collection<String>> topicNames,
            int currentSubtaskId,
            int currentParallelism,
            int[] ownedSubtaskIds,
            int totalNumberOfOwnedSubtasks,
            List<String> prefixesToAbort,
            long startCheckpointId,
            TransactionAborter transactionAborter,
            Supplier<Admin> adminSupplier,
            Set<String> precommittedTransactionIds) {
        this.topicNames = checkNotNull(topicNames, "topicNames must not be null");
        this.currentSubtaskId = currentSubtaskId;
        this.currentParallelism = currentParallelism;
        this.ownedSubtaskIds = Arrays.stream(ownedSubtaskIds).boxed().collect(Collectors.toSet());
        this.totalNumberOfOwnedSubtasks = totalNumberOfOwnedSubtasks;
        this.prefixesToAbort = Set.copyOf(prefixesToAbort);
        this.startCheckpointId = startCheckpointId;
        this.transactionAborter =
                checkNotNull(transactionAborter, "transactionAborter must not be null");
        this.adminSupplier = checkNotNull(adminSupplier, "adminSupplier must not be null");
        this.precommittedTransactionIds =
                checkNotNull(
                        precommittedTransactionIds, "transactionsToBeCommitted must not be null");
    }

    @Override
    public Collection<String> getOpenTransactionsForTopics() {
        return AdminUtils.getOpenTransactionsForTopics(adminSupplier.get(), topicNames.get())
                .stream()
                .map(TransactionListing::transactionalId)
                .collect(Collectors.toList());
    }

    @Override
    public boolean ownsTransactionalId(String transactionalId) {
        // Only the subtask that owns a respective state with oldSubtaskId can abort it
        // For upscaling: use the modulo operator to extrapolate ownership
        return ownedSubtaskIds.contains(
                extractSubtaskId(transactionalId) % totalNumberOfOwnedSubtasks);
    }

    @Override
    public int getCurrentSubtaskId() {
        return currentSubtaskId;
    }

    @Override
    public int getCurrentParallelism() {
        return currentParallelism;
    }

    @Override
    public Set<String> getPrefixesToAbort() {
        return prefixesToAbort;
    }

    @Override
    public Set<String> getPrecommittedTransactionalIds() {
        return precommittedTransactionIds;
    }

    @Override
    public long getStartCheckpointId() {
        return startCheckpointId;
    }

    public TransactionAborter getTransactionAborter() {
        return transactionAborter;
    }
}
