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

package org.apache.flink.connector.kafka.source.reader.transaction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Internal
public class KafkaShareAckTransactionManager implements AutoCloseable {

    private final ShareAckTransactionClient client;
    private final String groupId;
    private final int sourceSubtaskId;
    private final List<ShareAckCommittable> pendingCommittables;

    private ShareAckTransactionHandle activeTransaction;
    private boolean activeTransactionHasAcknowledgements;

    public KafkaShareAckTransactionManager(
            ShareAckTransactionClient client,
            String groupId,
            int sourceSubtaskId,
            Collection<ShareAckCommittable> restoredPendingCommittables) {
        this.client = client;
        this.groupId = groupId;
        this.sourceSubtaskId = sourceSubtaskId;
        this.pendingCommittables = new ArrayList<>(restoredPendingCommittables);
    }

    public void stageAcknowledgements() throws IOException, InterruptedException {
        stageAcknowledgementsForTransaction();
    }

    public ShareAckTransactionHandle stageAcknowledgementsForTransaction()
            throws IOException, InterruptedException {
        ShareAckTransactionHandle transaction = activeTransaction();
        client.stageAcknowledgements(transaction);
        activeTransactionHasAcknowledgements = true;
        return transaction;
    }

    public List<ShareAckCommittable> snapshotState(long checkpointId)
            throws IOException, InterruptedException {
        if (activeTransactionHasAcknowledgements) {
            String preparedTransactionState = client.preCommit(activeTransaction);
            pendingCommittables.add(
                    toCommittable(checkpointId, activeTransaction, preparedTransactionState));
            activeTransaction = null;
            activeTransactionHasAcknowledgements = false;
        }
        return List.copyOf(pendingCommittables);
    }

    public void markCommittedUpTo(long checkpointId) {
        pendingCommittables.removeIf(committable -> committable.getCheckpointId() <= checkpointId);
    }

    private ShareAckTransactionHandle activeTransaction()
            throws IOException, InterruptedException {
        if (activeTransaction == null) {
            activeTransaction = client.beginTransaction();
        }
        return activeTransaction;
    }

    private ShareAckCommittable toCommittable(
            long checkpointId,
            ShareAckTransactionHandle transaction,
            String preparedTransactionState) {
        return new ShareAckCommittable(
                checkpointId,
                transaction.getTransactionalId(),
                transaction.getTransactionOwnerId(),
                transaction.getTransactionOwnerEpoch(),
                preparedTransactionState,
                groupId,
                sourceSubtaskId);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
