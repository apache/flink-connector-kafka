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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * This class holds the necessary information to construct a new {@link FlinkKafkaInternalProducer}
 * to commit transactions in {@link KafkaCommitter}.
 */
@Internal
public class KafkaCommittable {

    private final long producerId;
    private final short epoch;
    private final String transactionalId;
    // Legacy committables do not record the protocol used by the original transaction.
    @Nullable private final Boolean transactionV2Enabled;
    @Nullable private FlinkKafkaInternalProducer<?, ?> producer;

    public KafkaCommittable(
            long producerId,
            short epoch,
            String transactionalId,
            @Nullable FlinkKafkaInternalProducer<?, ?> producer) {
        this(producerId, epoch, transactionalId, null, producer);
    }

    public KafkaCommittable(
            long producerId,
            short epoch,
            String transactionalId,
            @Nullable Boolean transactionV2Enabled,
            @Nullable FlinkKafkaInternalProducer<?, ?> producer) {
        this.producerId = producerId;
        this.epoch = epoch;
        this.transactionalId = transactionalId;
        this.transactionV2Enabled = transactionV2Enabled;
        this.producer = producer;
    }

    public static <K, V> KafkaCommittable of(FlinkKafkaInternalProducer<K, V> producer) {
        return new KafkaCommittable(
                producer.getProducerId(),
                producer.getEpoch(),
                producer.getTransactionalId(),
                producer.isTransactionV2Enabled(),
                producer);
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    /** Returns the original transaction protocol, or null when it was not checkpointed. */
    @Nullable
    public Boolean getTransactionV2Enabled() {
        return transactionV2Enabled;
    }

    public Optional<FlinkKafkaInternalProducer<?, ?>> getProducer() {
        return Optional.ofNullable(producer);
    }

    @Override
    public String toString() {
        return "KafkaCommittable{"
                + "producerId="
                + producerId
                + ", epoch="
                + epoch
                + ", transactionalId='"
                + transactionalId
                + '\''
                + ", transactionV2Enabled="
                + transactionV2Enabled
                + ", producer="
                + producer
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaCommittable that = (KafkaCommittable) o;
        return producerId == that.producerId
                && epoch == that.epoch
                && Objects.equals(transactionV2Enabled, that.transactionV2Enabled)
                && transactionalId.equals(that.transactionalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, epoch, transactionalId, transactionV2Enabled);
    }
}
