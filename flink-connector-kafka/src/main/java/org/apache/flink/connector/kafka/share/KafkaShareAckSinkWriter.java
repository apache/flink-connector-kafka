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
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.TransactionalIdFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

@Internal
final class KafkaShareAckSinkWriter
        implements CommittingSinkWriter<ShareAckRecord, ShareAckCommittable> {

    private static final String SHARE_ACK_TRANSACTION_SUFFIX = "share-ack";

    private final String shareExactlyOnceId;
    private final int subtaskId;
    private final ProducerFactory producerFactory;
    private long currentCheckpointId;
    @Nullable private ShareAckTransactionWriter currentWriter;

    KafkaShareAckSinkWriter(
            String shareExactlyOnceId, Properties producerProperties, WriterInitContext context) {
        this(
                shareExactlyOnceId,
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getRestoredCheckpointId().orElse(InitContext.INITIAL_CHECKPOINT_ID - 1) + 1,
                producerFactory(producerProperties));
    }

    @VisibleForTesting
    KafkaShareAckSinkWriter(
            String shareExactlyOnceId,
            int subtaskId,
            long firstCheckpointId,
            ProducerFactory producerFactory) {
        this.shareExactlyOnceId = Objects.requireNonNull(shareExactlyOnceId, "shareExactlyOnceId");
        this.subtaskId = subtaskId;
        this.currentCheckpointId = firstCheckpointId;
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
    }

    @Override
    public void write(ShareAckRecord element, SinkWriter.Context context) throws IOException {
        currentWriter().write(element);
    }

    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public Collection<ShareAckCommittable> prepareCommit() throws IOException {
        if (currentWriter == null) {
            currentCheckpointId++;
            return Collections.emptyList();
        }

        Optional<ShareAckCommittable> committable =
                currentWriter.prepareCommit(currentCheckpointId);
        closeCurrentWriter();
        currentWriter = null;
        currentCheckpointId++;
        return committable.map(Collections::singletonList).orElseGet(Collections::emptyList);
    }

    @Override
    public void close() throws Exception {
        if (currentWriter != null) {
            closeCurrentWriter();
            currentWriter = null;
        }
    }

    private ShareAckTransactionWriter currentWriter() throws IOException {
        if (currentWriter == null) {
            String transactionalId =
                    TransactionalIdFactory.buildTransactionalId(
                            shareExactlyOnceId + "-" + SHARE_ACK_TRANSACTION_SUFFIX,
                            subtaskId,
                            currentCheckpointId);
            currentWriter =
                    new ShareAckTransactionWriter(
                            shareExactlyOnceId, producerFactory.create(transactionalId));
        }
        return currentWriter;
    }

    private void closeCurrentWriter() throws IOException {
        try {
            currentWriter.close();
        } catch (Exception e) {
            throw new IOException("Failed to close share acknowledgement transaction writer.", e);
        }
    }

    private static ProducerFactory producerFactory(Properties producerProperties) {
        Properties properties = new Properties();
        properties.putAll(producerProperties);
        return transactionalId -> {
            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    new FlinkKafkaInternalProducer<>(properties, transactionalId);
            producer.initTransactions();
            return new FlinkKafkaShareAckTransactionalProducer(producer);
        };
    }

    @FunctionalInterface
    interface ProducerFactory {
        ShareAckTransactionalProducer create(String transactionalId) throws IOException;
    }
}
