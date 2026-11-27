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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransactionalIdFactory}. */
class TransactionIdFactoryTest {

    @Test
    void testBuildTransactionalId() {
        final String expected = "prefix-1-2";
        assertThat(TransactionalIdFactory.buildTransactionalId("prefix", 1, 2L))
                .isEqualTo(expected);
    }

    @Test
    void testExtractSubtaskId() {
        final String transactionalId = "prefix-1-2";
        assertThat(TransactionalIdFactory.extractSubtaskId(transactionalId)).isEqualTo(1);
    }

    @Test
    void testExtractPrefix() {
        final String transactionalId = "prefix-1-2";
        assertThat(TransactionalIdFactory.extractPrefix(transactionalId)).isEqualTo("prefix");
    }

    @ParameterizedTest
    @CsvSource({
        "-0-1, ''",
        "prefix-1-2, prefix",
        "my-prefix-1-2, my-prefix",
        "prefix--1-2, prefix-",
        "prefix-1-0-0, prefix-1",
        "prefix-2147483647-9223372036854775807, prefix"
    })
    void testHasPrefix(String transactionalId, String prefix) {
        assertThat(TransactionalIdFactory.hasPrefix(transactionalId, prefix)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
        // ids of sinks with another prefix
        "other-0-1, ''",
        "prefix-10-0-0, prefix-1",
        "prefix-2-0-0, prefix",
        "prefix-1-2, pre",
        "prefix-1-2, my-prefix",
        "prefix-1-2, prefiX",
        // ids without subtask id and offset
        "prefix-a-b, prefix",
        "prefix-1-b, prefix",
        "prefix-a-2, prefix",
        "prefix-1, prefix",
        "prefix, prefix",
        "prefix-1-, prefix",
        "prefix--2, prefix",
        // numbers that a sink does not create
        "prefix--1-2, prefix",
        "prefix-01-2, prefix",
        "prefix-1-+2, prefix",
        "prefix-2147483648-2, prefix",
        "prefix-1-9223372036854775808, prefix"
    })
    void testHasPrefixWithIdsOfOtherProducers(String transactionalId, String prefix) {
        assertThat(TransactionalIdFactory.hasPrefix(transactionalId, prefix)).isFalse();
    }
}
