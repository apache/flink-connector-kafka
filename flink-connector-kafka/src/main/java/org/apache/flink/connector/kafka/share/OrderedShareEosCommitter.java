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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import org.apache.kafka.common.errors.RetriableException;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

@Internal
public final class OrderedShareEosCommitter implements Committer<ShareEosCheckpointLedger> {

    private final KafkaOutputTransactionCommitter outputCommitter;
    private final ShareAckTransactionCommitter shareAckCommitter;

    public OrderedShareEosCommitter(
            KafkaOutputTransactionCommitter outputCommitter,
            ShareAckTransactionCommitter shareAckCommitter) {
        this.outputCommitter = Objects.requireNonNull(outputCommitter, "outputCommitter");
        this.shareAckCommitter = Objects.requireNonNull(shareAckCommitter, "shareAckCommitter");
    }

    @Override
    public void commit(Collection<CommitRequest<ShareEosCheckpointLedger>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<ShareEosCheckpointLedger> request : requests) {
            commitOne(request);
        }
    }

    private void commitOne(CommitRequest<ShareEosCheckpointLedger> request)
            throws IOException, InterruptedException {
        ShareEosCheckpointLedger ledger = request.getCommittable();
        if (ledger.getPhase() == ShareAckCommitPhase.DONE) {
            request.signalAlreadyCommitted();
            return;
        }

        ShareEosCheckpointLedger current = ledger;
        try {
            if (!current.outputsCommitted()) {
                current = current.withPhase(ShareAckCommitPhase.COMMITTING_OUTPUTS);
                for (KafkaCommittable outputCommittable : current.getOutputCommittables()) {
                    outputCommitter.commit(outputCommittable);
                }
                current = current.withPhase(ShareAckCommitPhase.OUTPUTS_COMMITTED);
            }

            current = current.withPhase(ShareAckCommitPhase.COMMITTING_SHARE_ACKS);
            for (ShareAckCommittable shareAckCommittable : current.getShareAckCommittables()) {
                shareAckCommitter.commit(shareAckCommittable);
            }
        } catch (RetriableException e) {
            request.updateAndRetryLater(current);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            request.signalFailedWithUnknownReason(e);
        }
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        try {
            outputCommitter.close();
        } catch (Exception e) {
            first = e;
        }
        try {
            shareAckCommitter.close();
        } catch (Exception e) {
            if (first == null) {
                first = e;
            } else {
                first.addSuppressed(e);
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
