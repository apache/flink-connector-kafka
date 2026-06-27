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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tracks the share acknowledgement payloads already staged into the current Kafka transaction so
 * that each payload is staged at most once per transaction window, and conflicting payloads sharing
 * an id are rejected.
 *
 * <p>Payloads are staged incrementally as records reach the sink (see {@link
 * SameTransactionShareAckKafkaWriter}); this buffer only deduplicates, it does not hold payloads for
 * a later bulk stage. It is reset (via {@link #clear()}) when a transaction is committed and a new
 * window begins.
 */
@Internal
class ShareAckPayloadBuffer {

    private final Map<String, ShareAckPayload> stagedById = new LinkedHashMap<>();

    /**
     * Registers a payload for the current transaction window.
     *
     * @return {@code true} if the payload is new and the caller should stage it; {@code false} if a
     *     payload with the same id and content was already registered (already staged).
     * @throws IOException if a different payload with the same id was already registered.
     */
    boolean register(ShareAckPayload payload) throws IOException {
        ShareAckPayload previous = stagedById.putIfAbsent(payload.getId(), payload);
        if (previous == null) {
            return true;
        }
        if (!previous.equals(payload)) {
            throw new IOException(
                    "Conflicting share acknowledgement payload for id " + payload.getId());
        }
        return false;
    }

    boolean isEmpty() {
        return stagedById.isEmpty();
    }

    void clear() {
        stagedById.clear();
    }
}
