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
import org.apache.flink.connector.kafka.sink.ShareAckCommittable;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.util.Collection;

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
        boolean sinkCommitted =
                committable.getCommitPhase()
                        == KafkaShareEosCommittable.CommitPhase.SINK_COMMITTED;
        if (!sinkCommitted) {
            try {
                kafkaCommitter.commit(committable.getKafkaCommittables());
                sinkCommitted = true;
                committable = committable.withSinkCommitted();
            } catch (IOException e) {
                request.retryLater();
                return;
            }
        }

        try {
            shareAckCommitter.commit(committable.getShareAckCommittables());
        } catch (IOException e) {
            if (sinkCommitted) {
                request.updateAndRetryLater(committable);
            } else {
                request.retryLater();
            }
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(kafkaCommitter, shareAckCommitter);
    }
}
