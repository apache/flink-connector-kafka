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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@Internal
public final class ShareAckRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ShareAckId id;
    private final String memberId;
    private final int memberEpoch;
    private final ShareAckDecision decision;

    public ShareAckRecord(
            ShareAckId id, String memberId, int memberEpoch, ShareAckDecision decision) {
        this.id = Objects.requireNonNull(id, "id");
        this.memberId = Objects.requireNonNull(memberId, "memberId");
        this.memberEpoch = memberEpoch;
        this.decision = Objects.requireNonNull(decision, "decision");
    }

    public ShareAckId getId() {
        return id;
    }

    public String getMemberId() {
        return memberId;
    }

    public int getMemberEpoch() {
        return memberEpoch;
    }

    public ShareAckDecision getDecision() {
        return decision;
    }

    public ShareAckPayload toPayload() {
        return new ShareAckPayload(
                id.asString(),
                id.getShareGroupId(),
                memberId,
                memberEpoch,
                List.of(
                        new ShareAckPayload.TopicPartitionAcknowledgements(
                                id.getTopicId(),
                                id.getTopic(),
                                id.getPartition(),
                                List.of(
                                        new ShareAckPayload.AcknowledgementBatch(
                                                id.getOffset(),
                                                id.getOffset(),
                                                List.of(decision.kafkaTypeId()))))));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShareAckRecord that = (ShareAckRecord) o;
        return memberEpoch == that.memberEpoch
                && id.equals(that.id)
                && memberId.equals(that.memberId)
                && decision == that.decision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, memberId, memberEpoch, decision);
    }

    @Override
    public String toString() {
        return "ShareAckRecord{"
                + "id="
                + id
                + ", memberId='"
                + memberId
                + '\''
                + ", memberEpoch="
                + memberEpoch
                + ", decision="
                + decision
                + '}';
    }
}
