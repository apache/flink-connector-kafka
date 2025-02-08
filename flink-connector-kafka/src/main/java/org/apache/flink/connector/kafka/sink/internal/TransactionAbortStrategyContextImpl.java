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

import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link TransactionAbortStrategyImpl.Context}. */
@Internal
public class TransactionAbortStrategyContextImpl implements TransactionAbortStrategyImpl.Context {
    private final int currentSubtaskId;
    private final int currentParallelism;
    private final Set<String> prefixesToAbort;
    private final long startCheckpointId;
    private final TransactionAborter transactionAborter;

    /** Creates a new {@link TransactionAbortStrategyContextImpl}. */
    public TransactionAbortStrategyContextImpl(
            int currentSubtaskId,
            int currentParallelism,
            List<String> prefixesToAbort,
            long startCheckpointId,
            TransactionAborter transactionAborter) {
        this.currentSubtaskId = currentSubtaskId;
        this.currentParallelism = currentParallelism;
        this.prefixesToAbort = Set.copyOf(prefixesToAbort);
        this.startCheckpointId = startCheckpointId;
        this.transactionAborter =
                checkNotNull(transactionAborter, "transactionAborter must not be null");
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
    public long getStartCheckpointId() {
        return startCheckpointId;
    }

    public TransactionAborter getTransactionAborter() {
        return transactionAborter;
    }
}
