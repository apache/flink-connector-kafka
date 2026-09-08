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

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

@Internal
public final class FlinkKafkaShareAckTransactionalProducer implements ShareAckTransactionalProducer {

    private final FlinkKafkaInternalProducer<byte[], byte[]> producer;
    private final PayloadStager payloadStager;

    public FlinkKafkaShareAckTransactionalProducer(
            FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        this(producer, ShareAckPayloadStager::stage);
    }

    @VisibleForTesting
    FlinkKafkaShareAckTransactionalProducer(
            FlinkKafkaInternalProducer<byte[], byte[]> producer, PayloadStager payloadStager) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.payloadStager = Objects.requireNonNull(payloadStager, "payloadStager");
    }

    @Override
    public void beginTransaction() {
        producer.beginTransaction();
    }

    @Override
    public void stage(ShareAckPayload payload) throws IOException {
        payloadStager.stage(producer, Objects.requireNonNull(payload, "payload"));
        producer.markShareAcksStaged();
    }

    @Override
    public Optional<String> prepareTransaction() {
        return producer.precommitTransaction();
    }

    @Override
    public String getTransactionalId() {
        String transactionalId = producer.getTransactionalId();
        if (transactionalId == null) {
            throw new IllegalStateException(
                    "Share acknowledgements require a transactional Kafka producer.");
        }
        return transactionalId;
    }

    @Override
    public long getProducerId() {
        return producer.getProducerId();
    }

    @Override
    public short getProducerEpoch() {
        return producer.getEpoch();
    }

    @Override
    public void close() {
        producer.close();
    }

    @FunctionalInterface
    interface PayloadStager {
        void stage(FlinkKafkaInternalProducer<byte[], byte[]> producer, ShareAckPayload payload)
                throws IOException;
    }
}
