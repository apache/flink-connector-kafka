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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Internal
public final class ShareAckCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String ackScopeId;
    private final long checkpointId;
    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    @Nullable private final String preparedTransactionState;
    private final List<ShareAckId> shareAckIds;

    public ShareAckCommittable(
            String ackScopeId,
            long checkpointId,
            String transactionalId,
            long producerId,
            short producerEpoch,
            @Nullable String preparedTransactionState,
            Collection<ShareAckId> shareAckIds) {
        this.ackScopeId = Objects.requireNonNull(ackScopeId, "ackScopeId");
        this.checkpointId = checkpointId;
        this.transactionalId = Objects.requireNonNull(transactionalId, "transactionalId");
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.preparedTransactionState = preparedTransactionState;
        this.shareAckIds = List.copyOf(Objects.requireNonNull(shareAckIds, "shareAckIds"));
        if (this.shareAckIds.isEmpty()) {
            throw new IllegalArgumentException("shareAckIds must not be empty");
        }
    }

    public String getAckScopeId() {
        return ackScopeId;
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

    public short getProducerEpoch() {
        return producerEpoch;
    }

    public Optional<String> getPreparedTransactionState() {
        return Optional.ofNullable(preparedTransactionState);
    }

    public List<ShareAckId> getShareAckIds() {
        return shareAckIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShareAckCommittable that = (ShareAckCommittable) o;
        return checkpointId == that.checkpointId
                && producerId == that.producerId
                && producerEpoch == that.producerEpoch
                && ackScopeId.equals(that.ackScopeId)
                && transactionalId.equals(that.transactionalId)
                && Objects.equals(preparedTransactionState, that.preparedTransactionState)
                && shareAckIds.equals(that.shareAckIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                ackScopeId,
                checkpointId,
                transactionalId,
                producerId,
                producerEpoch,
                preparedTransactionState,
                shareAckIds);
    }

    @Override
    public String toString() {
        return "ShareAckCommittable{"
                + "ackScopeId='"
                + ackScopeId
                + '\''
                + ", checkpointId="
                + checkpointId
                + ", transactionalId='"
                + transactionalId
                + '\''
                + ", producerId="
                + producerId
                + ", producerEpoch="
                + producerEpoch
                + ", preparedTransactionState="
                + preparedTransactionState
                + ", shareAckIds="
                + shareAckIds
                + '}';
    }
}
