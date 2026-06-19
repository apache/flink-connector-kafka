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

import java.util.Objects;

@Internal
public class ShareAckCommittable {

    private final long checkpointId;
    private final String transactionalId;
    private final long transactionOwnerId;
    private final short transactionOwnerEpoch;
    private final String groupId;
    private final int sourceSubtaskId;

    public ShareAckCommittable(
            long checkpointId,
            String transactionalId,
            long transactionOwnerId,
            short transactionOwnerEpoch,
            String groupId,
            int sourceSubtaskId) {
        this.checkpointId = checkpointId;
        this.transactionalId = transactionalId;
        this.transactionOwnerId = transactionOwnerId;
        this.transactionOwnerEpoch = transactionOwnerEpoch;
        this.groupId = groupId;
        this.sourceSubtaskId = sourceSubtaskId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public long getTransactionOwnerId() {
        return transactionOwnerId;
    }

    public short getTransactionOwnerEpoch() {
        return transactionOwnerEpoch;
    }

    public String getGroupId() {
        return groupId;
    }

    public int getSourceSubtaskId() {
        return sourceSubtaskId;
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
                && transactionOwnerId == that.transactionOwnerId
                && transactionOwnerEpoch == that.transactionOwnerEpoch
                && sourceSubtaskId == that.sourceSubtaskId
                && transactionalId.equals(that.transactionalId)
                && groupId.equals(that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                checkpointId,
                transactionalId,
                transactionOwnerId,
                transactionOwnerEpoch,
                groupId,
                sourceSubtaskId);
    }

    @Override
    public String toString() {
        return "ShareAckCommittable{"
                + "checkpointId="
                + checkpointId
                + ", transactionalId='"
                + transactionalId
                + '\''
                + ", transactionOwnerId="
                + transactionOwnerId
                + ", transactionOwnerEpoch="
                + transactionOwnerEpoch
                + ", groupId='"
                + groupId
                + '\''
                + ", sourceSubtaskId="
                + sourceSubtaskId
                + '}';
    }
}
