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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/** Implementations of an abort strategy for transactions left over from previous runs. */
@Internal
public enum TransactionAbortStrategyImpl {
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
    PROBING {
        @Override
        public void abortTransactions(Context context) {
            for (String prefix : context.getPrefixesToAbort()) {
                abortTransactionsWithPrefix(prefix, context);
            }
        }

        /**
         * Aborts all transactions that have been created by this subtask in a previous run.
         *
         * <p>It also aborts transactions from subtasks that may have been removed because of
         * downscaling.
         *
         * <p>When Flink downscales X subtasks to Y subtasks, then subtask i is responsible for
         * cleaning all subtasks j in [0; X), where j % Y = i. For example, if we downscale to 2,
         * then subtask 0 is responsible for all even and subtask 1 for all odd subtasks.
         */
        private void abortTransactionsWithPrefix(String prefix, Context context) {
            for (int subtaskId = context.getCurrentSubtaskId();
                    ;
                    subtaskId += context.getCurrentParallelism()) {
                if (abortTransactionOfSubtask(prefix, subtaskId, context) == 0) {
                    // If Flink didn't abort any transaction for current subtask, then we assume
                    // that no
                    // such subtask existed and no subtask with a higher number as well.
                    break;
                }
            }
        }

        /**
         * Aborts all transactions that have been created by a subtask in a previous run after the
         * given checkpoint id.
         *
         * <p>We assume that transaction ids are consecutively used and thus Flink can stop aborting
         * as soon as Flink notices that a particular transaction id was unused.
         */
        private int abortTransactionOfSubtask(String prefix, int subtaskId, Context context) {
            int numTransactionAborted = 0;
            for (long checkpointId = context.getStartCheckpointId();
                    ;
                    checkpointId++, numTransactionAborted++) {
                // initTransactions fences all old transactions with the same id by bumping the
                // epoch
                String transactionalId =
                        TransactionalIdFactory.buildTransactionalId(
                                prefix, subtaskId, checkpointId);
                int epoch = context.getTransactionAborter().abortTransaction(transactionalId);
                // An epoch of 0 indicates that the id was unused before
                if (epoch == 0) {
                    // Note that the check works beyond transaction log timeouts and just depends on
                    // the
                    // retention of the transaction topic (typically 7d). Any transaction that is
                    // not in
                    // the that topic anymore is also not lingering (i.e., it will not block
                    // downstream
                    // from reading)
                    // This method will only cease to work if transaction log timeout = topic
                    // retention
                    // and a user didn't restart the application for that period of time. Then the
                    // first
                    // transactions would vanish from the topic while later transactions are still
                    // lingering until they are cleaned up by Kafka. Then the user has to wait until
                    // the
                    // other transactions are timed out (which shouldn't take too long).
                    break;
                }
            }
            return numTransactionAborted;
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(TransactionAbortStrategyImpl.class);

    /** Aborts all transactions that have been created by this subtask in a previous run. */
    public abstract void abortTransactions(Context context);

    /** Injects the actual abortion of the transactional id generated by one of the strategies. */
    public interface TransactionAborter {
        int abortTransaction(String transactionalId);
    }

    /** Context for the {@link TransactionAbortStrategyImpl}. */
    public interface Context {
        int getCurrentSubtaskId();

        int getCurrentParallelism();

        Set<String> getPrefixesToAbort();

        long getStartCheckpointId();

        TransactionAborter getTransactionAborter();
    }
}
