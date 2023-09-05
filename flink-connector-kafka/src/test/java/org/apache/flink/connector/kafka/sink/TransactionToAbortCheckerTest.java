/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransactionsToAbortChecker}. */
public class TransactionToAbortCheckerTest extends TestLogger {

    public static final String ABORT = "abort";
    public static final String KEEP = "keep";

    @Test
    public void testMustAbortTransactionsWithSameSubtaskIdAndHigherCheckpointOffset() {
        Map<Integer, Long> offsetMapping = new HashMap<>(2);
        offsetMapping.put(0, 1L);
        offsetMapping.put(2, 3L);
        final TransactionsToAbortChecker checker =
                new TransactionsToAbortChecker(2, offsetMapping, 0);

        // abort recovered subtasksId with equal or higher checkpoint offset
        final Map<Integer, Map<Long, String>> openTransactions = new HashMap<>(3);
        final Map<Long, String> subtask0 = new HashMap<>();
        subtask0.put(1L, ABORT);
        subtask0.put(2L, ABORT);
        openTransactions.put(0, subtask0);
        final Map<Long, String> subtask2 = new HashMap<>();
        subtask2.put(3L, ABORT);
        subtask2.put(4L, ABORT);
        openTransactions.put(2, subtask2);
        final Map<Long, String> subtask3 = new HashMap<>();
        subtask3.put(3L, KEEP);
        subtask3.put(4L, KEEP);
        openTransactions.put(3, subtask3);

        final List<String> transactionsToAbort = checker.getTransactionsToAbort(openTransactions);
        assertThat(transactionsToAbort).hasSize(4);
        assertThatAbortCorrectTransaction(transactionsToAbort);
    }

    @Test
    public void testMustAbortTransactionsIfLowestCheckpointOffsetIsMinimumOffset() {
        final TransactionsToAbortChecker checker =
                new TransactionsToAbortChecker(2, Collections.singletonMap(0, 1L), 0);

        // abort recovered subtasksId with equal or higher checkpoint offset
        final Map<Integer, Map<Long, String>> openTransactions = new HashMap<>(5);
        final Map<Long, String> subtask0 = new HashMap<>();
        subtask0.put(1L, ABORT);
        subtask0.put(2L, ABORT);
        openTransactions.put(0, subtask0);
        openTransactions.put(2, Collections.singletonMap(1L, ABORT));
        openTransactions.put(3, Collections.singletonMap(1L, KEEP));
        openTransactions.put(4, Collections.singletonMap(1L, ABORT));
        openTransactions.put(5, Collections.singletonMap(1L, KEEP));

        final List<String> transactionsToAbort = checker.getTransactionsToAbort(openTransactions);
        assertThat(transactionsToAbort).hasSize(4);
        assertThatAbortCorrectTransaction(transactionsToAbort);
    }

    private static void assertThatAbortCorrectTransaction(List<String> abortedTransactions) {
        assertThat(abortedTransactions.stream().allMatch(t -> t.equals(ABORT))).isTrue();
    }
}
