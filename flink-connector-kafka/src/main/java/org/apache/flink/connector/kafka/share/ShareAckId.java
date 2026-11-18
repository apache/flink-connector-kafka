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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

/** Stable identity of one Kafka share-group record acknowledgement. */
@PublicEvolving
public final class ShareAckId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String shareGroupId;
    private final String topicId;
    private final String topic;
    private final int partition;
    private final long offset;

    public ShareAckId(
            String shareGroupId, String topicId, String topic, int partition, long offset) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        this.shareGroupId = Objects.requireNonNull(shareGroupId, "shareGroupId");
        this.topicId = Objects.requireNonNull(topicId, "topicId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.partition = partition;
        this.offset = offset;
    }

    public String getShareGroupId() {
        return shareGroupId;
    }

    public String getTopicId() {
        return topicId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String asString() {
        return shareGroupId + "|" + topicId + "|" + topic + "|" + partition + "|" + offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShareAckId that = (ShareAckId) o;
        return partition == that.partition
                && offset == that.offset
                && shareGroupId.equals(that.shareGroupId)
                && topicId.equals(that.topicId)
                && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shareGroupId, topicId, topic, partition, offset);
    }

    @Override
    public String toString() {
        return "ShareAckId{"
                + "shareGroupId='"
                + shareGroupId
                + '\''
                + ", topicId='"
                + topicId
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", partition="
                + partition
                + ", offset="
                + offset
                + '}';
    }
}
