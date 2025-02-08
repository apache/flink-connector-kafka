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

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link TransactionNamingStrategyImpl.Context}. */
@Internal
public class TransactionNamingStrategyContextImpl implements TransactionNamingStrategyImpl.Context {
    private final String transactionalIdPrefix;
    private final int subtaskId;
    private final ProducerPool producerPool;
    private long lastCheckpointId;
    private long nextCheckpointId;

    /** Creates a new {@link TransactionNamingStrategyContextImpl}. */
    public TransactionNamingStrategyContextImpl(
            String transactionalIdPrefix,
            int subtaskId,
            long lastCheckpointId,
            ProducerPool producerPool) {
        this.transactionalIdPrefix =
                checkNotNull(transactionalIdPrefix, "transactionalIdPrefix must not be null");
        this.subtaskId = subtaskId;
        this.producerPool = checkNotNull(producerPool, "producerPool must not be null");
        this.lastCheckpointId = lastCheckpointId;
    }

    @Override
    public String buildTransactionalId(long offset) {
        return TransactionalIdFactory.buildTransactionalId(
                transactionalIdPrefix, subtaskId, offset);
    }

    @Override
    public long getNextCheckpointId() {
        return nextCheckpointId;
    }

    public void setNextCheckpointId(long nextCheckpointId) {
        this.nextCheckpointId = nextCheckpointId;
    }

    public void setLastCheckpointId(long lastCheckpointId) {
        this.lastCheckpointId = lastCheckpointId;
    }

    @Override
    public long getLastCheckpointId() {
        return lastCheckpointId;
    }

    @Override
    public FlinkKafkaInternalProducer<byte[], byte[]> getProducer(String transactionalId) {
        return producerPool.getTransactionalProducer(transactionalId, nextCheckpointId);
    }

    @Override
    public void recycle(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        producerPool.recycle(producer);
    }
}
