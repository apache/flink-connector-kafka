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

package org.apache.flink.connector.kafka.share;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkKafkaOutputTransactionCommitterTest {

    @Test
    void testCommitsOutputTransaction() throws Exception {
        TestingKafkaCommitter delegate = new TestingKafkaCommitter(Outcome.SUCCESS);
        FlinkKafkaOutputTransactionCommitter committer =
                new FlinkKafkaOutputTransactionCommitter(delegate);

        assertThat(committer.commit(committable("output-txn-1")))
                .isEqualTo(TransactionCommitResult.COMMITTED);

        assertThat(delegate.committedTransactionalId).isEqualTo("output-txn-1");
    }

    @Test
    void testAlreadyCommittedOutputTransaction() throws Exception {
        FlinkKafkaOutputTransactionCommitter committer =
                new FlinkKafkaOutputTransactionCommitter(
                        new TestingKafkaCommitter(Outcome.ALREADY_COMMITTED));

        assertThat(committer.commit(committable("output-txn-1")))
                .isEqualTo(TransactionCommitResult.ALREADY_COMMITTED);
    }

    @Test
    void testRetriableOutputTransactionFailure() {
        FlinkKafkaOutputTransactionCommitter committer =
                new FlinkKafkaOutputTransactionCommitter(new TestingKafkaCommitter(Outcome.RETRY));

        assertThatThrownBy(() -> committer.commit(committable("output-txn-1")))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("should be retried");
    }

    @Test
    void testKnownOutputTransactionFailure() {
        RuntimeException failure = new IllegalStateException("known failure");
        FlinkKafkaOutputTransactionCommitter committer =
                new FlinkKafkaOutputTransactionCommitter(
                        new TestingKafkaCommitter(Outcome.knownFailure(failure)));

        assertThatThrownBy(() -> committer.commit(committable("output-txn-1")))
                .isInstanceOf(IOException.class)
                .hasCause(failure)
                .hasMessageContaining("known reason");
    }

    @Test
    void testUnknownOutputTransactionFailure() {
        RuntimeException failure = new IllegalStateException("unknown failure");
        FlinkKafkaOutputTransactionCommitter committer =
                new FlinkKafkaOutputTransactionCommitter(
                        new TestingKafkaCommitter(Outcome.unknownFailure(failure)));

        assertThatThrownBy(() -> committer.commit(committable("output-txn-1")))
                .isInstanceOf(IOException.class)
                .hasCause(failure)
                .hasMessageContaining("unknown reason");
    }

    @Test
    void testCloseClosesDelegate() throws Exception {
        TestingKafkaCommitter delegate = new TestingKafkaCommitter(Outcome.SUCCESS);
        FlinkKafkaOutputTransactionCommitter committer =
                new FlinkKafkaOutputTransactionCommitter(delegate);

        committer.close();

        assertThat(delegate.closed).isTrue();
    }

    private static KafkaCommittable committable(String transactionalId) {
        return new KafkaCommittable(1L, (short) 0, transactionalId, null);
    }

    private static final class TestingKafkaCommitter implements Committer<KafkaCommittable> {
        private final Outcome outcome;
        private String committedTransactionalId;
        private boolean closed;

        private TestingKafkaCommitter(Outcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public void commit(Collection<CommitRequest<KafkaCommittable>> requests) {
            assertThat(requests).hasSize(1);
            CommitRequest<KafkaCommittable> request = requests.iterator().next();
            committedTransactionalId = request.getCommittable().getTransactionalId();
            outcome.apply(request);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class Outcome {
        private static final Outcome SUCCESS = new Outcome(null, null, false, false);
        private static final Outcome ALREADY_COMMITTED = new Outcome(null, null, false, true);
        private static final Outcome RETRY = new Outcome(null, null, true, false);

        private final Throwable knownFailure;
        private final Throwable unknownFailure;
        private final boolean retry;
        private final boolean alreadyCommitted;

        private Outcome(
                Throwable knownFailure,
                Throwable unknownFailure,
                boolean retry,
                boolean alreadyCommitted) {
            this.knownFailure = knownFailure;
            this.unknownFailure = unknownFailure;
            this.retry = retry;
            this.alreadyCommitted = alreadyCommitted;
        }

        private static Outcome knownFailure(Throwable throwable) {
            return new Outcome(throwable, null, false, false);
        }

        private static Outcome unknownFailure(Throwable throwable) {
            return new Outcome(null, throwable, false, false);
        }

        private void apply(Committer.CommitRequest<KafkaCommittable> request) {
            if (knownFailure != null) {
                request.signalFailedWithKnownReason(knownFailure);
            } else if (unknownFailure != null) {
                request.signalFailedWithUnknownReason(unknownFailure);
            } else if (retry) {
                request.retryLater();
            } else if (alreadyCommitted) {
                request.signalAlreadyCommitted();
            }
        }
    }
}
