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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShareAckPayloadBufferTest {

    @Test
    void testStagesEachPayloadOnce() throws Exception {
        ShareAckPayloadBuffer buffer = new ShareAckPayloadBuffer();
        ShareAckPayload payload = payload("ack-0", "group", 0);
        List<String> stagedPayloads = new ArrayList<>();

        buffer.add(payload);
        buffer.add(payload);
        buffer.stage(
                new Object(),
                true,
                (producer, shareAckPayload) -> stagedPayloads.add(shareAckPayload.getId()));

        assertThat(stagedPayloads).containsExactly("ack-0");

        buffer.clear();

        assertThat(buffer.isEmpty()).isTrue();
    }

    @Test
    void testRejectsConflictingPayloadWithSameId() throws Exception {
        ShareAckPayloadBuffer buffer = new ShareAckPayloadBuffer();

        buffer.add(payload("ack-0", "group", 0));

        assertThatThrownBy(() -> buffer.add(payload("ack-0", "group", 1)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Conflicting share acknowledgement payload");
    }

    @Test
    void testRejectsShareAckOnlyTransaction() throws Exception {
        ShareAckPayloadBuffer buffer = new ShareAckPayloadBuffer();
        buffer.add(payload("ack-0", "group", 0));
        List<String> stagedPayloads = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                buffer.stage(
                                        new Object(),
                                        false,
                                        (producer, shareAckPayload) ->
                                                stagedPayloads.add(shareAckPayload.getId())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without sink records");
        assertThat(stagedPayloads).isEmpty();
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
