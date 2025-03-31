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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.kafka.sink.internal.CheckpointTransaction;
import org.apache.flink.connector.kafka.sink.internal.TransactionOwnership;

import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The state of the Kafka writer. Used to capture information regarding transactions. */
@Internal
public class KafkaWriterState {
    public static final int UNKNOWN = -1;

    private final String transactionalIdPrefix;
    private final int ownedSubtaskId;
    private final int totalNumberOfOwnedSubtasks;
    private final TransactionOwnership transactionOwnership;
    private final Collection<CheckpointTransaction> precommittedTransactionalIds;

    @VisibleForTesting
    public KafkaWriterState(
            String transactionalIdPrefix,
            int ownedSubtaskId,
            int totalNumberOfOwnedSubtasks,
            TransactionOwnership transactionOwnership,
            Collection<CheckpointTransaction> precommittedTransactionalIds) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        this.ownedSubtaskId = ownedSubtaskId;
        this.totalNumberOfOwnedSubtasks = totalNumberOfOwnedSubtasks;
        this.transactionOwnership = transactionOwnership;
        this.precommittedTransactionalIds = precommittedTransactionalIds;
    }

    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    public int getOwnedSubtaskId() {
        return ownedSubtaskId;
    }

    public int getTotalNumberOfOwnedSubtasks() {
        return totalNumberOfOwnedSubtasks;
    }

    public Collection<CheckpointTransaction> getPrecommittedTransactionalIds() {
        return precommittedTransactionalIds;
    }

    public TransactionOwnership getTransactionOwnership() {
        return transactionOwnership;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        KafkaWriterState that = (KafkaWriterState) object;
        return ownedSubtaskId == that.ownedSubtaskId
                && totalNumberOfOwnedSubtasks == that.totalNumberOfOwnedSubtasks
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && transactionOwnership == that.transactionOwnership
                && Objects.equals(precommittedTransactionalIds, that.precommittedTransactionalIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionalIdPrefix,
                ownedSubtaskId,
                totalNumberOfOwnedSubtasks,
                transactionOwnership,
                precommittedTransactionalIds);
    }

    @Override
    public String toString() {
        return "KafkaWriterState{"
                + "transactionalIdPrefix='"
                + transactionalIdPrefix
                + '\''
                + ", ownedSubtaskId="
                + ownedSubtaskId
                + ", totalNumberOfOwnedSubtasks="
                + totalNumberOfOwnedSubtasks
                + ", transactionOwnership="
                + transactionOwnership
                + ", precommittedTransactionalIds="
                + precommittedTransactionalIds
                + '}';
    }
}
