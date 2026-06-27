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

import org.apache.flink.connector.kafka.share.ShareAckPayload;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShareAckPayloadBufferTest {

    @Test
    void testRegisterReturnsTrueOnlyForNewPayload() throws Exception {
        ShareAckPayloadBuffer buffer = new ShareAckPayloadBuffer();
        ShareAckPayload payload = payload("ack-0", "group", 0);

        assertThat(buffer.register(payload)).isTrue();
        assertThat(buffer.register(payload)).isFalse();
        assertThat(buffer.isEmpty()).isFalse();
    }

    @Test
    void testRejectsConflictingPayloadWithSameId() throws Exception {
        ShareAckPayloadBuffer buffer = new ShareAckPayloadBuffer();

        assertThat(buffer.register(payload("ack-0", "group", 0))).isTrue();

        assertThatThrownBy(() -> buffer.register(payload("ack-0", "group", 1)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Conflicting share acknowledgement payload");
    }

    @Test
    void testClearResetsDeduplication() throws Exception {
        ShareAckPayloadBuffer buffer = new ShareAckPayloadBuffer();
        ShareAckPayload payload = payload("ack-0", "group", 0);

        assertThat(buffer.register(payload)).isTrue();
        buffer.clear();

        assertThat(buffer.isEmpty()).isTrue();
        // After a new window begins the same id may be staged again.
        assertThat(buffer.register(payload)).isTrue();
    }

    private static ShareAckPayload payload(String id, String groupId, int memberEpoch) {
        return new ShareAckPayload(
                id,
                groupId,
                "member",
                memberEpoch,
                List.of(
                        new ShareAckPayload.TopicPartitionAcknowledgements(
                                "AAAAAAAAAAAAAAAAAAAAAA",
                                "input",
                                0,
                                List.of(
                                        new ShareAckPayload.AcknowledgementBatch(
                                                0L, 0L, List.of((byte) 1))))));
    }
}
