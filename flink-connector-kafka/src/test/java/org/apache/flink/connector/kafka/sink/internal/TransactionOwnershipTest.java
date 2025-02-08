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

import org.apache.flink.connector.kafka.sink.KafkaWriterState;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.connector.kafka.sink.KafkaWriterState.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TransactionOwnershipTest {
    private static final String PREFIX = "prefix";

    @Nested
    class Implicit {
        TransactionOwnership ownership = TransactionOwnership.IMPLICIT_BY_SUBTASK_ID;

        @Test
        void testNoRecoveredStates() {
            assertThat(ownership.getOwnedSubtaskIds(2, 4, List.of())).containsExactlyInAnyOrder(2);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 4, List.of())).isEqualTo(4);
        }

        @Test
        void testOneRecoveredStateWithUnknownValues() {
            KafkaWriterState state =
                    new KafkaWriterState(PREFIX, UNKNOWN, UNKNOWN, ownership, List.of());

            assertThat(ownership.getOwnedSubtaskIds(2, 4, List.of(state)))
                    .containsExactlyInAnyOrder(2);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 4, List.of(state))).isEqualTo(4);
        }

        @Test
        void testOneRecoveredState() {
            KafkaWriterState state = new KafkaWriterState(PREFIX, 1, 2, ownership, List.of());

            assertThat(ownership.getOwnedSubtaskIds(2, 4, List.of(state)))
                    .containsExactlyInAnyOrder(2);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 4, List.of(state))).isEqualTo(4);
        }

        @Test
        void testOneRecoveredStateFromPoolingWithDownscaling() {
            KafkaWriterState state =
                    new KafkaWriterState(
                            PREFIX, 1, 2, TransactionOwnership.EXPLICIT_BY_WRITER_STATE, List.of());

            assertThatCode(() -> ownership.getOwnedSubtaskIds(2, 3, List.of(state)))
                    .hasMessageContaining(
                            "Attempted to switch the transaction naming strategy back to INCREMENTING");
        }

        @Test
        void testTwoRecoveredStates() {
            KafkaWriterState state1 = new KafkaWriterState(PREFIX, 1, 4, ownership, List.of());
            KafkaWriterState state2 = new KafkaWriterState(PREFIX, 3, 4, ownership, List.of());

            assertThat(ownership.getOwnedSubtaskIds(2, 3, List.of(state1, state2)))
                    .containsExactlyInAnyOrder(2);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 3, List.of(state1, state2)))
                    .isEqualTo(3);
        }

        @Test
        void testTwoRecoveredStatesFromPooling() {
            KafkaWriterState state1 =
                    new KafkaWriterState(
                            PREFIX, 1, 4, TransactionOwnership.EXPLICIT_BY_WRITER_STATE, List.of());
            KafkaWriterState state2 =
                    new KafkaWriterState(
                            PREFIX, 3, 4, TransactionOwnership.EXPLICIT_BY_WRITER_STATE, List.of());

            assertThatCode(() -> ownership.getOwnedSubtaskIds(2, 3, List.of(state1, state2)))
                    .hasMessageContaining(
                            "Attempted to switch the transaction naming strategy back to INCREMENTING");
        }
    }

    @Nested
    class Explicit {
        TransactionOwnership ownership = TransactionOwnership.EXPLICIT_BY_WRITER_STATE;

        @Test
        void testNoRecoveredStates() {
            assertThat(ownership.getOwnedSubtaskIds(2, 4, List.of())).containsExactlyInAnyOrder(2);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 4, List.of())).isEqualTo(4);
        }

        @Test
        void testOneRecoveredStateWithUnknownValues() {
            KafkaWriterState state =
                    new KafkaWriterState(PREFIX, UNKNOWN, UNKNOWN, ownership, List.of());

            assertThatCode(() -> ownership.getOwnedSubtaskIds(2, 4, List.of(state)))
                    .hasMessageContaining("migrate");
            assertThatCode(() -> ownership.getTotalNumberOfOwnedSubtasks(2, 4, List.of(state)))
                    .hasMessageContaining("migrate");
        }

        @Test
        void testOneRecoveredState() {
            KafkaWriterState state = new KafkaWriterState(PREFIX, 1, 4, ownership, List.of());

            assertThat(ownership.getOwnedSubtaskIds(2, 3, List.of(state)))
                    .containsExactlyInAnyOrder(1);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 3, List.of(state))).isEqualTo(4);
        }

        @Test
        void testNonConsecutive() {
            KafkaWriterState state = new KafkaWriterState(PREFIX, 1, 2, ownership, List.of());

            assertThatCode(() -> ownership.getOwnedSubtaskIds(3, 4, List.of(state)))
                    .hasMessageContaining("State not consecutively assigned");
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(3, 4, List.of(state))).isEqualTo(4);
        }

        @Test
        void testNonUniform() {
            KafkaWriterState state1 = new KafkaWriterState(PREFIX, 1, 4, ownership, List.of());
            KafkaWriterState state2 = new KafkaWriterState(PREFIX, 3, 4, ownership, List.of());

            assertThat(ownership.getOwnedSubtaskIds(3, 4, List.of(state1, state2)))
                    .containsExactlyInAnyOrder(1, 3);
            assertThatCode(
                            () ->
                                    ownership.getTotalNumberOfOwnedSubtasks(
                                            3, 4, List.of(state1, state2)))
                    .hasMessageContaining("Not uniformly assigned");
        }

        @Test
        void testTwoRecoveredStates() {
            KafkaWriterState state1 = new KafkaWriterState(PREFIX, 1, 4, ownership, List.of());
            KafkaWriterState state2 = new KafkaWriterState(PREFIX, 3, 4, ownership, List.of());

            assertThat(ownership.getOwnedSubtaskIds(2, 3, List.of(state1, state2)))
                    .containsExactlyInAnyOrder(1, 3);
            assertThat(ownership.getTotalNumberOfOwnedSubtasks(2, 3, List.of(state1, state2)))
                    .isEqualTo(4);
        }
    }
}
