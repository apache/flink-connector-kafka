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

package org.apache.flink.connector.kafka.tool;

import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.MapAssert;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/** Custom AssertJ assertions for Kafka Admin Client to verify transactions. */
public class KafkaAdminAssert extends AbstractAssert<KafkaAdminAssert, Admin>
        implements AutoCloseable {

    private final boolean selfManaged;

    private KafkaAdminAssert(Admin admin, boolean selfManaged) {
        super(admin, KafkaAdminAssert.class);
        this.selfManaged = selfManaged;
    }

    /** Entry point using a TestContainer. Creates and manages the Admin client lifecycle. */
    public static KafkaAdminAssert assertThat(TestKafkaContainer kafkaContainer) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return new KafkaAdminAssert(Admin.create(props), true);
    }

    /** Entry point using an existing Admin client. */
    public static KafkaAdminAssert assertThat(Admin admin) {
        return new KafkaAdminAssert(admin, false);
    }

    /** Fetches all transactions and switches context to Transaction assertions. */
    public KafkaTransactionsAssert transactions() {
        try {
            Collection<TransactionListing> listings = actual.listTransactions().all().get();
            return new KafkaTransactionsAssert(listings);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list transactions from Kafka", e);
        }
    }

    @Override
    public void close() {
        if (selfManaged && actual != null) {
            actual.close();
        }
    }

    /** Assertions for a collection of Kafka Transactions. */
    public static class KafkaTransactionsAssert
            extends AbstractAssert<KafkaTransactionsAssert, Collection<TransactionListing>> {

        public KafkaTransactionsAssert(Collection<TransactionListing> actual) {
            super(actual, KafkaTransactionsAssert.class);
        }

        /** Extracts IDs and returns a standard ListAssert. */
        public ListAssert<String> extractingIds() {
            List<String> ids =
                    actual.stream()
                            .map(TransactionListing::transactionalId)
                            .collect(Collectors.toList());
            return Assertions.assertThat(ids);
        }

        /** Extracts ID -> State map and returns a standard MapAssert. */
        public MapAssert<String, TransactionState> extractingStates() {
            Map<String, TransactionState> stateMap =
                    actual.stream()
                            .collect(
                                    Collectors.toMap(
                                            TransactionListing::transactionalId,
                                            TransactionListing::state));
            return Assertions.assertThat(stateMap);
        }
    }
}
