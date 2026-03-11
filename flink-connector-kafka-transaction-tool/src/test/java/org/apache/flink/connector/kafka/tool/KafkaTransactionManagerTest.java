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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link KafkaTransactionManager} input validation and error handling. */
class KafkaTransactionManagerTest {

    private final KafkaTransactionManager manager = new KafkaTransactionManager();

    @Test
    void testAbortWithNullBootstrapServers() {
        assertThatThrownBy(() -> manager.abortTransaction(null, "tx-1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Bootstrap servers cannot be null");
    }

    @Test
    void testAbortWithEmptyBootstrapServers() {
        assertThatThrownBy(() -> manager.abortTransaction("", "tx-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Bootstrap servers cannot be empty");
    }

    @Test
    void testAbortWithNullTransactionalId() {
        assertThatThrownBy(() -> manager.abortTransaction("localhost:9092", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Transactional ID cannot be null");
    }

    @Test
    void testAbortWithEmptyTransactionalId() {
        assertThatThrownBy(() -> manager.abortTransaction("localhost:9092", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Transactional ID cannot be empty");
    }

    @Test
    void testCommitWithNegativeProducerId() {
        assertThatThrownBy(() -> manager.commitTransaction("localhost:9092", "tx-1", -1, (short) 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Producer ID must be non-negative");
    }

    @Test
    void testCommitWithNegativeEpoch() {
        assertThatThrownBy(
                        () -> manager.commitTransaction("localhost:9092", "tx-1", 100, (short) -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Epoch must be non-negative");
    }
}
