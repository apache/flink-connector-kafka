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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Internal
public final class ShareAckTransactionWriter implements AutoCloseable {

    private final String ackScopeId;
    private final ShareAckTransactionalProducer producer;
    private final Map<String, ShareAckPayload> stagedByPayloadId = new LinkedHashMap<>();
    private final Set<ShareAckId> shareAckIds = new LinkedHashSet<>();
    private boolean transactionStarted;

    public ShareAckTransactionWriter(String ackScopeId, ShareAckTransactionalProducer producer) {
        this.ackScopeId = Objects.requireNonNull(ackScopeId, "ackScopeId");
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    public void write(ShareAckRecord record) throws IOException {
        ShareAckPayload payload = Objects.requireNonNull(record, "record").toPayload();
        ShareAckPayload previous = stagedByPayloadId.putIfAbsent(payload.getId(), payload);
        if (previous == null) {
            try {
                ensureTransactionStarted();
                producer.stage(payload);
                shareAckIds.add(record.getId());
            } catch (IOException | RuntimeException e) {
                stagedByPayloadId.remove(payload.getId());
                throw e;
            }
            return;
        }
        if (!previous.equals(payload)) {
            throw new IOException(
                    "Conflicting share acknowledgement payload for id " + payload.getId());
        }
    }

    public Optional<ShareAckCommittable> prepareCommit(long checkpointId) throws IOException {
        if (stagedByPayloadId.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> preparedTransactionState = producer.prepareTransaction();
        ShareAckCommittable committable =
                new ShareAckCommittable(
                        ackScopeId,
                        checkpointId,
                        producer.getTransactionalId(),
                        producer.getProducerId(),
                        producer.getProducerEpoch(),
                        preparedTransactionState.orElse(null),
                        new ArrayList<>(shareAckIds));
        stagedByPayloadId.clear();
        shareAckIds.clear();
        transactionStarted = false;
        return Optional.of(committable);
    }

    public boolean hasStagedAcks() {
        return !stagedByPayloadId.isEmpty();
    }

    private void ensureTransactionStarted() throws IOException {
        if (!transactionStarted) {
            producer.beginTransaction();
            transactionStarted = true;
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
