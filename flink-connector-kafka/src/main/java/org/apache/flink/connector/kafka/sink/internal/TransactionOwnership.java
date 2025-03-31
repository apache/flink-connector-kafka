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
import org.apache.flink.connector.kafka.sink.KafkaWriterState;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.sink.KafkaWriterState.UNKNOWN;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Describes the ownership model of transactional ids and with that ownership of the transactions.
 *
 * <p>A subtask that owns a transactional id is responsible for committing and aborting the
 * transactions having that id. Only that subtask may create new ids.
 *
 * <p>Transactional ids have the form <code>transactionalIdPrefix + "-" + subtaskId + "-" + counter
 * </code>. The prefix is given by the user, the subtask id is defined through the ownership model
 * and the counter through the {@link
 * org.apache.flink.connector.kafka.sink.TransactionNamingStrategy}.
 *
 * <p>For all strategies ownership is extrapolated for subtask ids beyond the currently known
 * subtasks. This is necessary to support cases of intermediate upscaling where no checkpoint has
 * been taken. Consider an application that runs with 3 subtasks and checkpointed. Later, its
 * upscaled to 5 but then a failure happens. We need to have at least 5 open transactions. If the
 * application is finally resumed from the checkpoint with 3 subtasks again. These 3 subtasks need
 * to assume ownership of the remaining 2.
 */
@Internal
public enum TransactionOwnership {
    /**
     * The ownership is determined by the current subtask ID. Ownership is extrapolated by
     * extracting the original subtask id of the ongoing transactions and applying modulo on the
     * current parallelism.
     */
    IMPLICIT_BY_SUBTASK_ID {
        @Override
        public int[] getOwnedSubtaskIds(
                int currentSubtaskId,
                int currentParallelism,
                Collection<KafkaWriterState> recoveredStates) {
            if (!recoveredStates.isEmpty()) {
                checkForMigration(recoveredStates);
            }

            return new int[] {currentSubtaskId};
        }

        private void checkForMigration(Collection<KafkaWriterState> recoveredStates) {
            TransactionOwnership oldOwnership =
                    recoveredStates.stream()
                            .map(KafkaWriterState::getTransactionOwnership)
                            .findFirst()
                            .orElseThrow();
            if (oldOwnership != this) {
                throw new IllegalStateException(
                        "Attempted to switch the transaction naming strategy back to INCREMENTING which may result in data loss.");
            }
        }

        @Override
        public int getTotalNumberOfOwnedSubtasks(
                int currentSubtaskId,
                int currentParallelism,
                Collection<KafkaWriterState> recoveredStates) {
            return currentParallelism;
        }
    },
    /**
     * The ownership is determined by the writer state that is recovered. Each writer may have
     * multiple states each with a different subtask id (e.g. when downscaling).
     *
     * <p>Additionally, the maximum parallelism that has been observed is stored in the state and
     * used to extrapolate ownership.
     *
     * <p>This ownership model has two assumption of the state assignment:
     *
     * <ul>
     *   <li>State is assigned first to lower subtask ids and then to higher ones. In the upscaling
     *       case, from oldP to newP, only tasks [0; oldP) are assigned. [oldP; newP) are not
     *       assigned any state.
     *   <li>State is uniformly assigned. In the upscaling case, none of the tasks have more than 1
     *       state assigned.
     * </ul>
     *
     * <p>Hence, the state is consecutively assigned to the subtasks from low to high.
     *
     * <p>With these assumption, this ownership model is able to recover from writer states with
     * subtask id + max parallelism:
     *
     * <ul>
     *   <li>If there is state, we can extract the owned subtask ids and the max parallelism from
     *       the state.
     *   <li>If there is no state, we can use the current subtask id and the max parallelism from
     *       the current parallelism. The current subtask id cannot possibly be owned already. The
     *       max parallelism in any state must be lower than the current max parallelism.
     *   <li>Hence, no subtask id is owned by more than one task and all tasks have the same max
     *       parallelism.
     *   <li>Since all tasks have shared knowledge, we can exclusively assign all transactional ids.
     *   <li>Since each subtask owns at least one transactional id, we can safely create new
     *       transactional ids while other subtasks are still aborting their transactions.
     * </ul>
     */
    EXPLICIT_BY_WRITER_STATE {
        @Override
        public int[] getOwnedSubtaskIds(
                int currentSubtaskId,
                int currentParallelism,
                Collection<KafkaWriterState> recoveredStates) {
            if (recoveredStates.isEmpty()) {
                return new int[] {currentSubtaskId};
            } else {
                int[] ownedSubtaskIds =
                        recoveredStates.stream()
                                .mapToInt(KafkaWriterState::getOwnedSubtaskId)
                                .sorted()
                                .toArray();
                assertKnown(ownedSubtaskIds[0]);

                int maxParallelism =
                        recoveredStates.iterator().next().getTotalNumberOfOwnedSubtasks();
                // Assumption of the ownership model: state is distributed consecutively across the
                // subtasks starting with subtask 0
                checkState(currentSubtaskId < maxParallelism, "State not consecutively assigned");

                return ownedSubtaskIds;
            }
        }

        @Override
        public int getTotalNumberOfOwnedSubtasks(
                int currentSubtaskId,
                int currentParallelism,
                Collection<KafkaWriterState> recoveredStates) {
            if (recoveredStates.isEmpty()) {
                return currentParallelism;
            }
            Set<Integer> numSubtasks =
                    recoveredStates.stream()
                            .map(KafkaWriterState::getTotalNumberOfOwnedSubtasks)
                            .collect(Collectors.toSet());
            checkState(numSubtasks.size() == 1, "Writer states not in sync %s", recoveredStates);
            int totalNumberOfOwnedSubtasks = numSubtasks.iterator().next();
            assertKnown(totalNumberOfOwnedSubtasks);

            if (currentParallelism >= totalNumberOfOwnedSubtasks) {
                // Assumption of the ownership model: state is distributed consecutively across the
                // subtasks starting with subtask 0
                checkState(recoveredStates.size() == 1, "Not uniformly assigned");
            }
            return Math.max(totalNumberOfOwnedSubtasks, currentParallelism);
        }

        private void assertKnown(int ownershipValue) {
            checkState(
                    ownershipValue != UNKNOWN,
                    "Attempted to migrate from flink-connector-kafka 3.X directly to a naming strategy that uses the new writer state. Please first migrate to a flink-connector-kafka 4.X with INCREMENTING.");
        }
    };

    /** Returns the owned subtask ids for this subtask. */
    public abstract int[] getOwnedSubtaskIds(
            int currentSubtaskId,
            int currentParallelism,
            Collection<KafkaWriterState> recoveredStates);

    /** Returns the total number of owned subtasks across all subtasks. */
    public abstract int getTotalNumberOfOwnedSubtasks(
            int currentSubtaskId,
            int currentParallelism,
            Collection<KafkaWriterState> recoveredStates);
}
