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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;

/**
 * Committer implementation for {@link KafkaSink}
 *
 * <p>The committer is responsible to finalize the Kafka transactions by committing them.
 */
@Internal
public class KafkaCommitter implements Committer<KafkaCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitter.class);
    public static final String UNKNOWN_PRODUCER_ID_ERROR_MESSAGE =
            "because of a bug in the Kafka broker (KAFKA-9310). Please upgrade to Kafka 2.5+. If you are running with concurrent checkpoints, you also may want to try without them.\n"
                    + "To avoid data loss, the application will restart.";

    private final Properties kafkaProducerConfig;
    private final boolean reusesTransactionalIds;
    private final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory;
    private final WritableBackchannel<TransactionFinished> backchannel;
    @Nullable private FlinkKafkaInternalProducer<?, ?> committingProducer;

    public KafkaCommitter(
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            int subtaskId,
            int attemptNumber,
            boolean reusesTransactionalIds,
            BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory) {
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.reusesTransactionalIds = reusesTransactionalIds;
        this.producerFactory = producerFactory;
        backchannel =
                BackchannelFactory.getInstance()
                        .getWritableBackchannel(subtaskId, attemptNumber, transactionalIdPrefix);
    }

    @VisibleForTesting
    public WritableBackchannel<TransactionFinished> getBackchannel() {
        return backchannel;
    }

    @Nullable
    @VisibleForTesting
    FlinkKafkaInternalProducer<?, ?> getCommittingProducer() {
        return committingProducer;
    }

    @Override
    public void commit(Collection<CommitRequest<KafkaCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<KafkaCommittable> request : requests) {
            final KafkaCommittable committable = request.getCommittable();
            final String transactionalId = committable.getTransactionalId();
            LOG.debug("Committing Kafka transaction {}", transactionalId);
            Optional<FlinkKafkaInternalProducer<?, ?>> writerProducer = committable.getProducer();
            FlinkKafkaInternalProducer<?, ?> producer = null;
            try {
                producer = writerProducer.orElseGet(() -> getProducer(committable));
                producer.commitTransaction();
                backchannel.send(TransactionFinished.successful(committable.getTransactionalId()));
            } catch (RetriableException e) {
                LOG.warn(
                        "Encountered retriable exception while committing {}.", transactionalId, e);
                request.retryLater();
            } catch (ProducerFencedException e) {
                logFencedRequest(request, e);
                handleFailedTransaction(producer);
                request.signalFailedWithKnownReason(e);
            } catch (InvalidTxnStateException e) {
                // This exception only occurs when aborting after a commit or vice versa.
                // It does not appear on double commits or double aborts.
                LOG.error(
                        "Unable to commit transaction ({}) because it's in an invalid state. "
                                + "Most likely the transaction has been aborted for some reason. Please check the Kafka logs for more details.",
                        request,
                        e);
                handleFailedTransaction(producer);
                request.signalFailedWithKnownReason(e);
            } catch (UnknownProducerIdException e) {
                LOG.error(
                        "Unable to commit transaction ({}) " + UNKNOWN_PRODUCER_ID_ERROR_MESSAGE,
                        request,
                        e);
                handleFailedTransaction(producer);
                request.signalFailedWithKnownReason(e);
            } catch (InvalidPidMappingException e) {
                LOG.error(
                        "Unable to commit transaction ({}) because the producer id mapping is invalid. "
                                + "This typically happens when the transaction has expired on the broker. "
                                + "The transaction cannot be committed and data may have been lost.",
                        request,
                        e);
                handleFailedTransaction(producer);
                request.signalFailedWithKnownReason(e);
            } catch (InterruptException e) {
                // note that we do not attempt to recover from this exception; producer is likely
                // left in an inconsistent state
                LOG.info(
                        "Committing transaction ({}) was interrupted. This most likely happens because the task is being cancelled.",
                        request,
                        e);
                // reset the interrupt flag that is set when InterruptException is created
                Thread.interrupted();
                // propagate interruption through java.lang.InterruptedException instead
                throw new InterruptedException(e.getMessage());
            } catch (Exception e) {
                LOG.error(
                        "Transaction ({}) encountered error and data has been potentially lost.",
                        request,
                        e);
                closeCommitterProducer(producer);
                // cause failover
                request.signalFailedWithUnknownReason(e);
            }
        }
    }

    private void logFencedRequest(
            CommitRequest<KafkaCommittable> request, ProducerFencedException e) {
        if (reusesTransactionalIds) {
            // If checkpoint 1 succeeds, checkpoint 2 is aborted, and checkpoint 3 may reuse the id
            // of checkpoint 1. A recovery of checkpoint 1 would show that the transaction has been
            // fenced.
            LOG.warn(
                    "Unable to commit transaction ({}) because its producer is already fenced."
                            + " If this warning appears as part of the recovery of a checkpoint, it is expected in some cases (e.g., aborted checkpoints in previous attempt)."
                            + " If it's outside of recovery, this means that you either have a different sink with the same '{}'"
                            + " or recovery took longer than '{}' ({}ms). In both cases this most likely signals data loss,"
                            + " please consult the Flink documentation for more details.",
                    request,
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                    kafkaProducerConfig.getProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                    e);
        } else {
            // initTransaction has been called on this transaction before
            LOG.error(
                    "Unable to commit transaction ({}) because its producer is already fenced."
                            + " This means that you either have a different producer with the same '{}' (this is"
                            + " unlikely with the '{}' as all generated ids are unique and shouldn't be reused)"
                            + " or recovery took longer than '{}' ({}ms). In both cases this most likely signals data loss,"
                            + " please consult the Flink documentation for more details.",
                    request,
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    KafkaSink.class.getSimpleName(),
                    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                    kafkaProducerConfig.getProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                    e);
        }
    }

    private void handleFailedTransaction(FlinkKafkaInternalProducer<?, ?> producer) {
        if (producer == null) {
            return;
        }
        backchannel.send(TransactionFinished.erroneously(producer.getTransactionalId()));
        closeCommitterProducer(producer);
    }

    private void closeCommitterProducer(FlinkKafkaInternalProducer<?, ?> producer) {
        if (producer == this.committingProducer) {
            this.committingProducer.close();
            this.committingProducer = null;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(backchannel, committingProducer);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * Creates a producer that can commit into the same transaction as the upstream producer that
     * was serialized into {@link KafkaCommittable}.
     */
    private FlinkKafkaInternalProducer<?, ?> getProducer(KafkaCommittable committable) {
        if (committingProducer == null) {
            committingProducer =
                    producerFactory.apply(kafkaProducerConfig, committable.getTransactionalId());
        } else {
            committingProducer.setTransactionId(committable.getTransactionalId());
        }
        committingProducer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
        return committingProducer;
    }
}
