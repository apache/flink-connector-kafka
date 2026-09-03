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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaTransactionTool} CLI argument parsing. */
class KafkaTransactionToolTest {

    @Test
    void testHelpReturnsZero() {
        assertThat(KafkaTransactionTool.run(new String[] {"--help"})).isEqualTo(0);
    }

    @Test
    void testEmptyArgsReturnsZero() {
        assertThat(KafkaTransactionTool.run(new String[] {})).isEqualTo(0);
    }

    @Test
    void testMissingRequiredArgsReturnsOne() {
        assertThat(KafkaTransactionTool.run(new String[] {"--action", "abort"})).isEqualTo(1);
    }

    @Test
    void testUnknownActionReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "unknown",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testCommitWithoutProducerIdReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--epoch", "5"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testCommitWithoutEpochReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testNonNumericProducerIdReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "abc",
                                    "--epoch", "5"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testEpochOverflowReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100",
                                    "--epoch", "50000"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testNonNumericEpochReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100",
                                    "--epoch", "xyz"
                                }))
                .isEqualTo(2);
    }
}
