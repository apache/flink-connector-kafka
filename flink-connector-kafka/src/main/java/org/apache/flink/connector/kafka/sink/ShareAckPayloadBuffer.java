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
import org.apache.flink.connector.kafka.share.ShareAckPayload;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@Internal
class ShareAckPayloadBuffer {

    private final Map<String, ShareAckPayload> payloadsById = new LinkedHashMap<>();

    void addAll(Collection<ShareAckPayload> payloads) throws IOException {
        for (ShareAckPayload payload : payloads) {
            add(payload);
        }
    }

    void add(ShareAckPayload payload) throws IOException {
        ShareAckPayload previous = payloadsById.putIfAbsent(payload.getId(), payload);
        if (previous != null && !previous.equals(payload)) {
            throw new IOException(
                    "Conflicting share acknowledgement payload for id " + payload.getId());
        }
    }

    boolean isEmpty() {
        return payloadsById.isEmpty();
    }

    void stage(Object producer, boolean transactionHasRecords, ShareAckPayloadStageFunction stager)
            throws IOException {
        if (payloadsById.isEmpty()) {
            return;
        }
        if (!transactionHasRecords) {
            throw new IOException(
                    "Cannot commit share acknowledgements without sink records in the same Kafka transaction.");
        }
        for (ShareAckPayload payload : payloadsById.values()) {
            stager.stage(producer, payload);
        }
    }

    void clear() {
        payloadsById.clear();
    }

    @FunctionalInterface
    interface ShareAckPayloadStageFunction {
        void stage(Object producer, ShareAckPayload payload) throws IOException;
    }
}
