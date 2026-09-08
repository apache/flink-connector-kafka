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

import org.apache.kafka.common.errors.RetriableException;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

@Internal
public final class AtLeastOnceShareAckCommitter implements Committer<ShareAckCommittable> {

    private final ShareAckTransactionCommitter shareAckCommitter;

    public AtLeastOnceShareAckCommitter(ShareAckTransactionCommitter shareAckCommitter) {
        this.shareAckCommitter = Objects.requireNonNull(shareAckCommitter, "shareAckCommitter");
    }

    @Override
    public void commit(Collection<CommitRequest<ShareAckCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<ShareAckCommittable> request : requests) {
            try {
                TransactionCommitResult result =
                        shareAckCommitter.commit(request.getCommittable());
                if (result == TransactionCommitResult.ALREADY_COMMITTED) {
                    request.signalAlreadyCommitted();
                }
            } catch (RetriableException e) {
                request.retryLater();
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                request.signalFailedWithUnknownReason(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        shareAckCommitter.close();
    }
}
