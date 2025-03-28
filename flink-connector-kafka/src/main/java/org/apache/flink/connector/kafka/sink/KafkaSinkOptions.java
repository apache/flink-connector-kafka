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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyImpl;

/** (Table) options for {@link KafkaSink}. */
@PublicEvolving
public class KafkaSinkOptions {

    /**
     * The strategy to name transactions. Naming strategy has implications on the resource
     * consumption on the broker because each unique transaction name requires the broker to keep
     * some metadata in memory for 7 days.
     *
     * <p>All naming strategies use the format {@code transactionalIdPrefix-subtask-offset} where
     * offset is calculated differently.
     */
    public static final ConfigOption<TransactionNamingStrategy> TRANSACTION_NAMING_STRATEGY =
            ConfigOptions.key("sink.transaction-naming-strategy")
                    .enumType(TransactionNamingStrategy.class)
                    .defaultValue(TransactionNamingStrategy.DEFAULT)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Advanced option to influence how transactions are named.")
                                    .linebreak()
                                    .text(
                                            "INCREMENTING is the strategy used in flink-kafka-connector 3.X (DEFAULT). It wastes memory of the Kafka broker but works with older Kafka broker versions (Kafka 2.X).")
                                    .linebreak()
                                    .text(
                                            "POOLING is a new strategy introduced in flink-kafka-connector 4.X. It is more resource-friendly than INCREMENTING but requires Kafka 3.0+. Switching to this strategy requires a checkpoint taken with flink-kafka-connector 4.X or a snapshot taken with earlier versions.")
                                    .build());

    /**
     * The strategy to name transactions. Naming strategy has implications on the resource
     * consumption on the broker because each unique transaction name requires the broker to keep
     * some metadata in memory for 7 days.
     *
     * <p>All naming strategies use the format {@code transactionalIdPrefix-subtask-offset} where
     * offset is calculated differently.
     */
    @PublicEvolving
    public enum TransactionNamingStrategy {
        /**
         * The offset of the transaction name is a monotonically increasing number that mostly
         * corresponds to the checkpoint id. This strategy is wasteful in terms of resource
         * consumption on the broker.
         *
         * <p>This is exactly the same behavior as in flink-connector-kafka 3.X.
         */
        INCREMENTING(
                TransactionNamingStrategyImpl.INCREMENTING, TransactionAbortStrategyImpl.PROBING),
        /**
         * This strategy reuses transaction names. It is more resource-friendly than {@link
         * #INCREMENTING} on the Kafka broker.
         *
         * <p>It's a new strategy introduced in flink-connector-kafka 4.X. It requires Kafka 3.0+
         * and additional read permissions on the target topics.
         */
        POOLING(TransactionNamingStrategyImpl.POOLING, TransactionAbortStrategyImpl.LISTING);

        /**
         * The default transaction naming strategy. Currently set to {@link #INCREMENTING}, which is
         * the same behavior of flink-connector-kafka 3.X.
         */
        public static final TransactionNamingStrategy DEFAULT = INCREMENTING;

        /**
         * The backing implementation of the transaction naming strategy. Separation allows to avoid
         * leaks of internal classes in signatures.
         */
        private final TransactionNamingStrategyImpl impl;
        /**
         * The set of supported abort strategies for this naming strategy. Some naming strategies
         * may not support all abort strategies.
         */
        private final TransactionAbortStrategyImpl abortImpl;

        TransactionNamingStrategy(
                TransactionNamingStrategyImpl impl, TransactionAbortStrategyImpl abortImpl) {
            this.impl = impl;
            this.abortImpl = abortImpl;
        }

        boolean requiresKnownTopics() {
            return impl.requiresKnownTopics();
        }

        @Internal
        TransactionAbortStrategyImpl getAbortImpl() {
            return abortImpl;
        }

        @Internal
        TransactionNamingStrategyImpl getImpl() {
            return impl;
        }
    }
}
