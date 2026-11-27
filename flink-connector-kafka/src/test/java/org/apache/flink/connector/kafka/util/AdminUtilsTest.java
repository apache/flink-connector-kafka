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

package org.apache.flink.connector.kafka.util;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AdminUtils#describeTransactions}. */
class AdminUtilsTest {

    private static final TransactionDescription DESCRIPTION =
            new TransactionDescription(
                    0,
                    TransactionState.ONGOING,
                    1L,
                    0,
                    60_000L,
                    OptionalLong.empty(),
                    Collections.emptySet());

    @Test
    void testDescribeTransactionsOmitsUnknownIds() throws Exception {
        KafkaFutureImpl<TransactionDescription> known = new KafkaFutureImpl<>();
        known.complete(DESCRIPTION);
        KafkaFutureImpl<TransactionDescription> unknown = new KafkaFutureImpl<>();
        unknown.completeExceptionally(new TransactionalIdNotFoundException("unknown"));
        Map<String, KafkaFuture<TransactionDescription>> futures = new HashMap<>();
        futures.put("known", known);
        futures.put("unknown", unknown);

        Map<String, TransactionDescription> descriptions =
                AdminUtils.describeTransactions(adminReturning(futures), futures.keySet());

        assertThat(descriptions).containsOnlyKeys("known").containsEntry("known", DESCRIPTION);
    }

    @Test
    void testDescribeTransactionsWrapsOtherFailures() throws Exception {
        KafkaFutureImpl<TransactionDescription> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new IllegalStateException("not supported"));
        Map<String, KafkaFuture<TransactionDescription>> futures =
                Collections.singletonMap("failed", failed);

        assertThatThrownBy(
                        () ->
                                AdminUtils.describeTransactions(
                                        adminReturning(futures), futures.keySet()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to describe transaction failed")
                .hasMessageContaining("at least version 3.0")
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testDescribeTransactionsRestoresInterruptFlag() throws Exception {
        Map<String, KafkaFuture<TransactionDescription>> futures =
                Collections.singletonMap("pending", new KafkaFutureImpl<>());

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    AdminUtils.describeTransactions(
                                            adminReturning(futures), futures.keySet()))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Interrupted while describing transaction pending")
                    .hasRootCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // clear the flag so that it does not leak into other tests
            Thread.interrupted();
        }
    }

    @Test
    void testDescribeTransactionsWithoutIdsDoesNotCallAdmin() {
        Admin admin =
                (Admin)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Admin.class},
                                (proxy, method, args) -> {
                                    throw new AssertionError("unexpected call to " + method);
                                });

        assertThat(AdminUtils.describeTransactions(admin, Collections.emptySet())).isEmpty();
    }

    /**
     * Returns an {@link Admin} whose {@code describeTransactions} returns the given futures. The
     * result constructor is package-private in Kafka, hence the reflection.
     */
    private static Admin adminReturning(Map<String, KafkaFuture<TransactionDescription>> futures)
            throws Exception {
        Map<CoordinatorKey, KafkaFuture<TransactionDescription>> byKey = new HashMap<>();
        futures.forEach((id, f) -> byKey.put(CoordinatorKey.byTransactionalId(id), f));
        Constructor<DescribeTransactionsResult> constructor =
                DescribeTransactionsResult.class.getDeclaredConstructor(Map.class);
        constructor.setAccessible(true);
        DescribeTransactionsResult result = constructor.newInstance(byKey);
        return (Admin)
                Proxy.newProxyInstance(
                        AdminUtilsTest.class.getClassLoader(),
                        new Class<?>[] {Admin.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("describeTransactions")) {
                                return result;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }
}
