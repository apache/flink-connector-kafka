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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;

import org.apache.kafka.common.errors.TimeoutException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

@Internal
public final class FlinkKafkaOutputTransactionCommitter implements KafkaOutputTransactionCommitter {

    private final Committer<KafkaCommittable> kafkaCommitter;

    public FlinkKafkaOutputTransactionCommitter(
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            int subtaskId,
            int attemptNumber,
            boolean reusesTransactionalIds) {
        this(
                new KafkaCommitter(
                        kafkaProducerConfig,
                        transactionalIdPrefix,
                        subtaskId,
                        attemptNumber,
                        reusesTransactionalIds,
                        FlinkKafkaInternalProducer::new));
    }

    @VisibleForTesting
    FlinkKafkaOutputTransactionCommitter(Committer<KafkaCommittable> kafkaCommitter) {
        this.kafkaCommitter = Objects.requireNonNull(kafkaCommitter, "kafkaCommitter");
    }

    @Override
    public TransactionCommitResult commit(KafkaCommittable committable)
            throws IOException, InterruptedException {
        TrackingCommitRequest request = new TrackingCommitRequest(committable);
        kafkaCommitter.commit(Collections.singletonList(request));
        return request.result();
    }

    @Override
    public void close() throws Exception {
        kafkaCommitter.close();
    }

    private static final class TrackingCommitRequest
            implements Committer.CommitRequest<KafkaCommittable> {

        private KafkaCommittable committable;
        private boolean retryLater;
        private boolean alreadyCommitted;
        @Nullable private Throwable knownFailure;
        @Nullable private Throwable unknownFailure;

        private TrackingCommitRequest(KafkaCommittable committable) {
            this.committable = Objects.requireNonNull(committable, "committable");
        }

        @Override
        public KafkaCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            knownFailure = Objects.requireNonNull(t, "t");
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            unknownFailure = Objects.requireNonNull(t, "t");
        }

        @Override
        public void retryLater() {
            retryLater = true;
        }

        @Override
        public void updateAndRetryLater(KafkaCommittable committable) {
            this.committable = Objects.requireNonNull(committable, "committable");
            retryLater = true;
        }

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted = true;
        }

        private TransactionCommitResult result() throws IOException {
            if (retryLater) {
                throw new TimeoutException("Kafka output transaction commit should be retried.");
            }
            if (knownFailure != null) {
                throw new IOException("Kafka output transaction commit failed with known reason.", knownFailure);
            }
            if (unknownFailure != null) {
                throw new IOException("Kafka output transaction commit failed with unknown reason.", unknownFailure);
            }
            return alreadyCommitted
                    ? TransactionCommitResult.ALREADY_COMMITTED
                    : TransactionCommitResult.COMMITTED;
        }
    }
}
