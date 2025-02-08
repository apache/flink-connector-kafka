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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;

/**
 * The strategy to abort a transactions left by ungraceful job termination, which is the usual case
 * for failures in Flink. On start of the sink, the strategy will be used to abort any ongoing
 * transactions left open while cautiously avoid to abort transactions that are still trying to
 * commit.
 */
@PublicEvolving
public enum TransactionAbortStrategy {
    /**
     * The probing strategy starts with aborting a set of known transactional ids from the recovered
     * state and then continues guessing if more transactions may have been opened between this run
     * and the last successful checkpoint. This also accounts for rescaling in between the attempts.
     *
     * <p>However, the probing is not side-effect free, which leads to an ever-increasing search
     * space for the next probing attempt in case of a restart loop. It will fix eventually on the
     * next successful checkpoints. It's recommended to use this strategy only with a strict restart
     * policy that prevents tight restart loops (e.g. incremental backoff or hard failure after X
     * attempts).
     *
     * <p>This is exactly the same behavior as in flink-connector-kafka 3.X.
     */
    PROBING(TransactionAbortStrategyImpl.PROBING),
    LISTING(TransactionAbortStrategyImpl.LISTING);

    /**
     * The default transaction abort strategy. Currently set to {@link #PROBING}, which is the same
     * behavior of flink-connector-kafka 3.X.
     */
    public static final TransactionAbortStrategy DEFAULT = PROBING;

    /**
     * The backing implementation of the transaction abort strategy. Separation allows to avoid
     * leaks of internal classes in signatures.
     */
    private final TransactionAbortStrategyImpl impl;

    TransactionAbortStrategy(TransactionAbortStrategyImpl impl) {
        this.impl = impl;
    }

    TransactionAbortStrategyImpl getImpl() {
        return impl;
    }
}
