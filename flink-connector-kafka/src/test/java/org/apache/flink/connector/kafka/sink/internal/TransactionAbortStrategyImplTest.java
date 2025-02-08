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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.api.connector.sink2.InitContext.INITIAL_CHECKPOINT_ID;
import static org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl.LISTING;
import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.extractSubtaskId;
import static org.assertj.core.api.Assertions.assertThat;

class TransactionAbortStrategyImplTest {

    private static final String PREFIX = "prefix";

    @Nested
    class Listing {
        @Test
        void testDownscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 1, 4);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            testContext.addPrecommittedTransactionalIds(1, 1L);
            testContext.addOpenTransaction(2, 1L); // not owned
            String t02 = testContext.addOpenTransaction(0, 2L);
            String t12 = testContext.addOpenTransaction(1, 2L);
            testContext.addOpenTransaction(2, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t02, t12);
        }

        @Test
        void testDownscaleWithUnsafeTransactionalIds() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(1, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(2, 3, 4);

            String t11 = testContext.addOpenTransaction(1, 1L); // not owned
            testContext.addPrecommittedTransactionalIds(2, 1L);
            testContext.addPrecommittedTransactionalIds(3, 1L);
            String t12 = testContext.addOpenTransaction(1, 2L); // not owned
            String t22 = testContext.addOpenTransaction(2, 2L);
            String t32 = testContext.addOpenTransaction(3, 2L);

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t22, t32);
        }

        @Test
        void testDownscaleWithIntermediateUpscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 2);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 1, 4);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            testContext.addPrecommittedTransactionalIds(1, 1L);
            String t02 = testContext.addOpenTransaction(0, 2L);
            String t12 = testContext.addOpenTransaction(1, 2L);
            String t52 = testContext.addOpenTransaction(5, 2L);
            testContext.addOpenTransaction(6, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions())
                    .containsExactlyInAnyOrder(t02, t12, t52);
        }

        @Test
        void testUpscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 4);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            testContext.addOpenTransaction(1, 1L); // not owned
            String t02 = testContext.addOpenTransaction(0, 2L);
            testContext.addOpenTransaction(1, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t02);
        }

        @Test
        void testUpscaleWithIntermediateUpscale() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(0, 4);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(0, 2);

            testContext.addPrecommittedTransactionalIds(0, 1L);
            String t02 = testContext.addOpenTransaction(0, 2L);
            String t42 = testContext.addOpenTransaction(4, 2L);
            testContext.addOpenTransaction(5, 2L); // not owned

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t02, t42);
        }

        @Test
        void testUpscaleWithNoState() {
            TestContext testContext = new TestContext();
            testContext.setSubtaskIdAndParallelism(3, 4);
            testContext.setOwnedSubtaskIdsAndMaxParallelism(3, 4);

            String t31 = testContext.addOpenTransaction(3, 1L);
            String t32 = testContext.addOpenTransaction(3, 2L);

            LISTING.abortTransactions(testContext);
            assertThat(testContext.getAbortedTransactions()).containsExactlyInAnyOrder(t31, t32);
        }
    }

    static final class TestContext implements TransactionAbortStrategyImpl.Context {
        private final Set<Integer> ownedSubtaskIds = new HashSet<>();
        private final Set<String> precommittedTransactionalIds = new HashSet<>();
        private final Collection<String> abortedTransactions = new ArrayList<>();
        private final Collection<String> openTransactionalIds = new ArrayList<>();
        private int currentSubtaskId;
        private int currentParallelism;
        private int maxParallelism;
        private String prefix = PREFIX;
        private final Set<String> prefixesToAbort = new HashSet<>(Set.of(prefix));
        private long startCheckpointId = INITIAL_CHECKPOINT_ID;

        public void setPrefix(String prefix) {
            this.prefix = prefix;
            this.prefixesToAbort.add(prefix);
        }

        public Collection<String> getAbortedTransactions() {
            return abortedTransactions;
        }

        public String addPrecommittedTransactionalIds(int oldSubtaskId, long checkpointId) {
            String transactionalId = addOpenTransaction(oldSubtaskId, checkpointId);
            precommittedTransactionalIds.add(transactionalId);
            return transactionalId;
        }

        public String addOpenTransaction(int oldSubtaskId, long checkpointId) {
            String transactionalId = getTransactionalId(oldSubtaskId, checkpointId);
            openTransactionalIds.add(transactionalId);
            return transactionalId;
        }

        @Override
        public int getCurrentSubtaskId() {
            return this.currentSubtaskId;
        }

        @Override
        public int getCurrentParallelism() {
            return this.currentParallelism;
        }

        @Override
        public boolean ownsTransactionalId(String transactionalId) {
            return ownedSubtaskIds.contains(extractSubtaskId(transactionalId) % maxParallelism);
        }

        @Override
        public Set<String> getPrefixesToAbort() {
            return this.prefixesToAbort;
        }

        @Override
        public Set<String> getPrecommittedTransactionalIds() {
            return this.precommittedTransactionalIds;
        }

        @Override
        public long getStartCheckpointId() {
            return this.startCheckpointId;
        }

        public void setStartCheckpointId(long startCheckpointId) {
            this.startCheckpointId = startCheckpointId;
        }

        @Override
        public TransactionAbortStrategyImpl.TransactionAborter getTransactionAborter() {
            return transactionalId -> {
                abortedTransactions.add(transactionalId);
                return 0;
            };
        }

        @Override
        public Collection<String> getOpenTransactionsForTopics() {
            return this.openTransactionalIds;
        }

        public void setSubtaskIdAndParallelism(int newSubtaskId, int newParallelism) {
            this.currentSubtaskId = newSubtaskId;
            this.currentParallelism = newParallelism;
        }

        public void setOwnedSubtaskIdsAndMaxParallelism(int... subtaskIdsOrParallelism) {
            for (int index = 0; index < subtaskIdsOrParallelism.length; index++) {
                if (index < subtaskIdsOrParallelism.length - 1) {
                    this.ownedSubtaskIds.add(subtaskIdsOrParallelism[index]);
                } else {
                    this.maxParallelism = subtaskIdsOrParallelism[index];
                }
            }
        }

        private String getTransactionalId(int oldSubtaskId, long checkpointId) {
            return TransactionalIdFactory.buildTransactionalId(prefix, oldSubtaskId, checkpointId);
        }
    }
}
