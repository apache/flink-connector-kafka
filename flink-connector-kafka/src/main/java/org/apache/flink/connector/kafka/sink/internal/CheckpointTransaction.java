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

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An immutable class that represents a transactional id and a checkpoint id. It's used inside the
 * {@link ProducerPoolImpl} to keep track of the transactions that are currently in-flight. The
 * checkpoint id is used to subsume committed transactions wrt to recycling producers.
 *
 * <p>Since writer state v3 it also records the producer id and epoch under which the transaction
 * was opened, so that a recovery can tell whether the broker still holds this very transaction or a
 * later one that reused the transactional id.
 */
@Internal
public class CheckpointTransaction {
    /** Marker for a producer id or epoch that was not recorded, e.g. state written before v3. */
    public static final long UNKNOWN_PRODUCER_ID = -1L;

    public static final short UNKNOWN_EPOCH = -1;

    private final String transactionalId;
    private final long checkpointId;
    private final long producerId;
    private final short epoch;

    public CheckpointTransaction(String transactionalId, long checkpointId) {
        this(transactionalId, checkpointId, UNKNOWN_PRODUCER_ID, UNKNOWN_EPOCH);
    }

    public CheckpointTransaction(
            String transactionalId, long checkpointId, long producerId, short epoch) {
        this.transactionalId = checkNotNull(transactionalId, "transactionalId must not be null");
        this.checkpointId = checkpointId;
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    /** Whether producer id and epoch were recorded when this transaction was snapshotted. */
    public boolean hasKnownEpoch() {
        return epoch != UNKNOWN_EPOCH && producerId != UNKNOWN_PRODUCER_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointTransaction that = (CheckpointTransaction) o;
        return checkpointId == that.checkpointId
                && producerId == that.producerId
                && epoch == that.epoch
                && Objects.equals(transactionalId, that.transactionalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionalId, checkpointId, producerId, epoch);
    }

    @Override
    public String toString() {
        return "CheckpointTransaction{"
                + "transactionalId='"
                + transactionalId
                + '\''
                + ", checkpointId="
                + checkpointId
                + ", producerId="
                + producerId
                + ", epoch="
                + epoch
                + '}';
    }
}
