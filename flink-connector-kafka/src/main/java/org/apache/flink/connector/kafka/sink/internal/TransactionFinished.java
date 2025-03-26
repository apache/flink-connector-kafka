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

/**
 * Represents the end of a transaction. This event is sent from the committer to the writer to
 * signal that the transaction with the given id has finished. The writer then recycles the producer
 * with the given id if the transaction was successfully committed.
 */
@Internal
public class TransactionFinished {
    private final String transactionId;
    private final boolean success;

    public TransactionFinished(String transactionId, boolean success) {
        this.transactionId = transactionId;
        this.success = success;
    }

    public static TransactionFinished successful(String transactionalId) {
        return new TransactionFinished(transactionalId, true);
    }

    public static TransactionFinished erroneously(String transactionalId) {
        return new TransactionFinished(transactionalId, false);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TransactionFinished that = (TransactionFinished) object;
        return success == that.success && Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, success);
    }

    @Override
    public String toString() {
        return "TransactionFinished{"
                + "transactionId='"
                + transactionId
                + '\''
                + ", success="
                + success
                + '}';
    }
}
