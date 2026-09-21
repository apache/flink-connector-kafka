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
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;

@Internal
public final class FlinkKafkaShareAckTransactionCommitter implements ShareAckTransactionCommitter {

    private final Properties producerProperties;
    private final BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory;
    @Nullable private FlinkKafkaInternalProducer<?, ?> committingProducer;

    public FlinkKafkaShareAckTransactionCommitter(Properties producerProperties) {
        this(producerProperties, FlinkKafkaInternalProducer::new);
    }

    @VisibleForTesting
    FlinkKafkaShareAckTransactionCommitter(
            Properties producerProperties,
            BiFunction<Properties, String, FlinkKafkaInternalProducer<?, ?>> producerFactory) {
        this.producerProperties = copyOf(producerProperties);
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
    }

    @Override
    public TransactionCommitResult commit(ShareAckCommittable committable)
            throws IOException, InterruptedException {
        FlinkKafkaInternalProducer<?, ?> producer = getProducer(committable.getTransactionalId());
        try {
            Optional<String> preparedTransactionState = committable.getPreparedTransactionState();
            if (preparedTransactionState.isPresent()) {
                producer.completePreparedTransaction(preparedTransactionState.get());
            } else {
                producer.resumeTransaction(committable.getProducerId(), committable.getProducerEpoch());
                producer.commitTransaction();
            }
            return TransactionCommitResult.COMMITTED;
        } catch (InterruptException e) {
            Thread.interrupted();
            throw new InterruptedException(e.getMessage());
        } catch (ProducerFencedException | InvalidTxnStateException | UnknownProducerIdException e) {
            closeCommitterProducer(producer);
            throw e;
        } catch (RetriableException e) {
            throw e;
        } catch (RuntimeException e) {
            closeCommitterProducer(producer);
            throw e;
        }
    }

    private FlinkKafkaInternalProducer<?, ?> getProducer(String transactionalId) {
        if (committingProducer == null) {
            committingProducer = producerFactory.apply(producerProperties, transactionalId);
        } else {
            committingProducer.setTransactionId(transactionalId);
        }
        return committingProducer;
    }

    private void closeCommitterProducer(FlinkKafkaInternalProducer<?, ?> producer) {
        if (producer == committingProducer) {
            producer.close();
            committingProducer = null;
        }
    }

    @Override
    public void close() {
        if (committingProducer != null) {
            committingProducer.close();
            committingProducer = null;
        }
    }

    private static Properties copyOf(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(Objects.requireNonNull(properties, "properties"));
        return copy;
    }
}
