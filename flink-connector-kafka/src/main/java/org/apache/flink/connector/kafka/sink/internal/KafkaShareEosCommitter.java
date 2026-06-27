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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;
import org.apache.flink.connector.kafka.sink.KafkaShareEosCommittable;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Internal
public class KafkaShareEosCommitter implements Committer<KafkaShareEosCommittable> {

    private final TransactionCommitter<KafkaCommittable> kafkaCommitter;
    private final TransactionCommitter<ShareAckCommittable> shareAckCommitter;

    public KafkaShareEosCommitter(
            TransactionCommitter<KafkaCommittable> kafkaCommitter,
            TransactionCommitter<ShareAckCommittable> shareAckCommitter) {
        this.kafkaCommitter = kafkaCommitter;
        this.shareAckCommitter = shareAckCommitter;
    }

    @Override
    public void commit(Collection<CommitRequest<KafkaShareEosCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<KafkaShareEosCommittable> request : requests) {
            commit(request);
        }
    }

    private void commit(CommitRequest<KafkaShareEosCommittable> request)
            throws IOException, InterruptedException {
        KafkaShareEosCommittable committable = request.getCommittable();
        if (committable.getCommitPhase() != KafkaShareEosCommittable.CommitPhase.SINK_COMMITTED) {
            committable = commitSinkTransactions(request, committable);
            if (committable == null) {
                return;
            }
        }

        commitShareAckTransactions(request, committable);
    }

    private KafkaShareEosCommittable commitSinkTransactions(
            CommitRequest<KafkaShareEosCommittable> request,
            KafkaShareEosCommittable committable)
            throws InterruptedException {
        List<KafkaCommittable> remainingCommittables =
                new ArrayList<>(committable.getKafkaCommittables());
        boolean committedAny = false;
        while (!remainingCommittables.isEmpty()) {
            KafkaCommittable nextCommittable = remainingCommittables.get(0);
            try {
                kafkaCommitter.commit(List.of(nextCommittable));
                remainingCommittables.remove(0);
                committedAny = true;
            } catch (IOException e) {
                if (committedAny) {
                    request.updateAndRetryLater(
                            committable.withKafkaCommittables(remainingCommittables));
                } else {
                    request.retryLater();
                }
                return null;
            }
        }
        return committable.withKafkaCommittables(List.of()).withSinkCommitted();
    }

    private void commitShareAckTransactions(
            CommitRequest<KafkaShareEosCommittable> request,
            KafkaShareEosCommittable committable)
            throws InterruptedException {
        List<ShareAckCommittable> remainingCommittables =
                new ArrayList<>(committable.getShareAckCommittables());
        while (!remainingCommittables.isEmpty()) {
            ShareAckCommittable nextCommittable = remainingCommittables.get(0);
            try {
                shareAckCommitter.commit(List.of(nextCommittable));
                remainingCommittables.remove(0);
            } catch (IOException e) {
                request.updateAndRetryLater(
                        committable.withShareAckCommittables(remainingCommittables));
                return;
            }
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(kafkaCommitter, shareAckCommitter);
    }
}
