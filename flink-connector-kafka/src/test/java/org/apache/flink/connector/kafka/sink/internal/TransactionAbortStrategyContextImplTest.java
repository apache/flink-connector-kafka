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

import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory.buildTransactionalId;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link TransactionAbortStrategyContextImpl#isPrecommittedTransactionSuperseded} against
 * broker descriptions without a broker.
 */
class TransactionAbortStrategyContextImplTest {

    private static final String ID = buildTransactionalId("prefix", 0, 1L);
    private static final long PRODUCER_ID = 42L;
    private static final short EPOCH = 3;

    private final Map<String, TransactionDescription> brokerDescriptions = new HashMap<>();
    private final List<Collection<String>> describeCalls = new ArrayList<>();

    @Test
    void stateWithoutEpochIsNotSuperseded() {
        brokerDescriptions.put(ID, description(PRODUCER_ID, EPOCH + 1));

        TransactionAbortStrategyContextImpl context = context(new CheckpointTransaction(ID, 1L));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isFalse();
        assertThat(describeCalls).isEmpty();
    }

    @Test
    void idUnknownToBrokerIsNotSuperseded() {
        TransactionAbortStrategyContextImpl context = context(precommitted(ID));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isFalse();
    }

    @Test
    void sameProducerAndEpochIsNotSuperseded() {
        brokerDescriptions.put(ID, description(PRODUCER_ID, EPOCH));

        TransactionAbortStrategyContextImpl context = context(precommitted(ID));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isFalse();
    }

    @Test
    void newerEpochIsSuperseded() {
        brokerDescriptions.put(ID, description(PRODUCER_ID, EPOCH + 1));

        TransactionAbortStrategyContextImpl context = context(precommitted(ID));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isTrue();
    }

    @Test
    void newerEpochThatCompletedSinceTheListingIsNotSuperseded() {
        brokerDescriptions.put(
                ID, description(PRODUCER_ID, EPOCH + 1, TransactionState.COMPLETE_COMMIT));

        TransactionAbortStrategyContextImpl context = context(precommitted(ID));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isFalse();
    }

    @Test
    void differentProducerIdIsSupersededRegardlessOfEpoch() {
        brokerDescriptions.put(ID, description(PRODUCER_ID + 1, 0));

        TransactionAbortStrategyContextImpl context = context(precommitted(ID));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isTrue();
    }

    @Test
    void idThatWasNotPrecommittedIsNotSuperseded() {
        String other = buildTransactionalId("prefix", 0, 2L);
        brokerDescriptions.put(other, description(PRODUCER_ID, EPOCH + 1));

        TransactionAbortStrategyContextImpl context = context(precommitted(ID));

        assertThat(context.isPrecommittedTransactionSuperseded(other)).isFalse();
    }

    @Test
    void describesAllPrecommittedIdsInOneRequest() {
        String second = buildTransactionalId("prefix", 0, 2L);
        brokerDescriptions.put(ID, description(PRODUCER_ID, EPOCH + 1));
        brokerDescriptions.put(second, description(PRODUCER_ID, EPOCH));

        TransactionAbortStrategyContextImpl context =
                context(precommitted(ID), precommitted(second));

        assertThat(context.isPrecommittedTransactionSuperseded(ID)).isTrue();
        assertThat(context.isPrecommittedTransactionSuperseded(second)).isFalse();
        assertThat(describeCalls).hasSize(1);
        assertThat(describeCalls.get(0)).containsExactlyInAnyOrder(ID, second);
    }

    private static CheckpointTransaction precommitted(String transactionalId) {
        return new CheckpointTransaction(transactionalId, 1L, PRODUCER_ID, EPOCH);
    }

    private static TransactionDescription description(long producerId, int epoch) {
        return description(producerId, epoch, TransactionState.ONGOING);
    }

    private static TransactionDescription description(
            long producerId, int epoch, TransactionState state) {
        return new TransactionDescription(
                0, state, producerId, epoch, 60_000L, OptionalLong.empty(), Collections.emptySet());
    }

    private TransactionAbortStrategyContextImpl context(CheckpointTransaction... precommitted) {
        return new TransactionAbortStrategyContextImpl(
                Collections::emptyList,
                0,
                1,
                new int[] {0},
                1,
                Collections.singletonList("prefix"),
                1L,
                transactionalId -> 0,
                () -> {
                    throw new AssertionError("the describer must not need an Admin");
                },
                List.of(precommitted),
                transactionalId -> {},
                transactionalIds -> {
                    describeCalls.add(new ArrayList<>(transactionalIds));
                    Map<String, TransactionDescription> result = new HashMap<>();
                    for (String transactionalId : transactionalIds) {
                        if (brokerDescriptions.containsKey(transactionalId)) {
                            result.put(transactionalId, brokerDescriptions.get(transactionalId));
                        }
                    }
                    return result;
                });
    }
}
