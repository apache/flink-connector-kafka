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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Internal
public class ShareAckPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final String groupId;
    private final String memberId;
    private final int memberEpoch;
    private final List<TopicPartitionAcknowledgements> acknowledgements;

    public ShareAckPayload(
            String id,
            String groupId,
            String memberId,
            int memberEpoch,
            Collection<TopicPartitionAcknowledgements> acknowledgements) {
        this.id = Objects.requireNonNull(id, "id");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.memberId = Objects.requireNonNull(memberId, "memberId");
        this.memberEpoch = memberEpoch;
        this.acknowledgements = List.copyOf(Objects.requireNonNull(acknowledgements));
        if (this.acknowledgements.isEmpty()) {
            throw new IllegalArgumentException("acknowledgements must not be empty");
        }
    }

    @SuppressWarnings("unchecked")
    public static ShareAckPayload fromKafkaObjects(
            String id, Object acknowledgements, Object groupMetadata) {
        Objects.requireNonNull(acknowledgements, "acknowledgements");
        Objects.requireNonNull(groupMetadata, "groupMetadata");
        if ((Boolean) invoke(acknowledgements, "isEmpty")) {
            throw new IllegalArgumentException("acknowledgements must not be empty");
        }

        List<TopicPartitionAcknowledgements> partitions = new ArrayList<>();
        Map<?, ?> acknowledgementsByPartition =
                (Map<?, ?>) invoke(acknowledgements, "acknowledgements");
        for (Map.Entry<?, ?> entry : acknowledgementsByPartition.entrySet()) {
            Object topicIdPartition = entry.getKey();
            List<?> batches = (List<?>) entry.getValue();
            List<AcknowledgementBatch> copiedBatches = new ArrayList<>();
            for (Object batch : batches) {
                copiedBatches.add(
                        new AcknowledgementBatch(
                                (Long) invoke(batch, "firstOffset"),
                                (Long) invoke(batch, "lastOffset"),
                                (List<Byte>) invoke(batch, "acknowledgeTypes")));
            }
            partitions.add(
                    new TopicPartitionAcknowledgements(
                            invoke(topicIdPartition, "topicId").toString(),
                            (String) invoke(topicIdPartition, "topic"),
                            (Integer) invoke(topicIdPartition, "partition"),
                            copiedBatches));
        }

        return new ShareAckPayload(
                id,
                (String) invoke(groupMetadata, "groupId"),
                (String) invoke(groupMetadata, "memberId"),
                (Integer) invoke(groupMetadata, "memberEpoch"),
                partitions);
    }

    public String getId() {
        return id;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getMemberId() {
        return memberId;
    }

    public int getMemberEpoch() {
        return memberEpoch;
    }

    public List<TopicPartitionAcknowledgements> getAcknowledgements() {
        return acknowledgements;
    }

    private static Object invoke(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Kafka object does not expose " + methodName + "()", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalArgumentException("Failed to invoke " + methodName + "()", cause);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShareAckPayload that = (ShareAckPayload) o;
        return memberEpoch == that.memberEpoch
                && id.equals(that.id)
                && groupId.equals(that.groupId)
                && memberId.equals(that.memberId)
                && acknowledgements.equals(that.acknowledgements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, groupId, memberId, memberEpoch, acknowledgements);
    }

    @Override
    public String toString() {
        return "ShareAckPayload{"
                + "id='"
                + id
                + '\''
                + ", groupId='"
                + groupId
                + '\''
                + ", memberId='"
                + memberId
                + '\''
                + ", memberEpoch="
                + memberEpoch
                + ", acknowledgements="
                + acknowledgements
                + '}';
    }

    public static class TopicPartitionAcknowledgements implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String topicId;
        private final String topic;
        private final int partition;
        private final List<AcknowledgementBatch> batches;

        public TopicPartitionAcknowledgements(
                String topicId,
                String topic,
                int partition,
                Collection<AcknowledgementBatch> batches) {
            this.topicId = Objects.requireNonNull(topicId, "topicId");
            this.topic = Objects.requireNonNull(topic, "topic");
            this.partition = partition;
            this.batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
            if (this.batches.isEmpty()) {
                throw new IllegalArgumentException("batches must not be empty");
            }
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

        public List<AcknowledgementBatch> getBatches() {
            return batches;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicPartitionAcknowledgements that = (TopicPartitionAcknowledgements) o;
            return partition == that.partition
                    && topicId.equals(that.topicId)
                    && topic.equals(that.topic)
                    && batches.equals(that.batches);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, topic, partition, batches);
        }

        @Override
        public String toString() {
            return "TopicPartitionAcknowledgements{"
                    + "topicId='"
                    + topicId
                    + '\''
                    + ", topic='"
                    + topic
                    + '\''
                    + ", partition="
                    + partition
                    + ", batches="
                    + batches
                    + '}';
        }
    }

    public static class AcknowledgementBatch implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long firstOffset;
        private final long lastOffset;
        private final List<Byte> acknowledgeTypes;

        public AcknowledgementBatch(
                long firstOffset, long lastOffset, Collection<Byte> acknowledgeTypes) {
            if (lastOffset < firstOffset) {
                throw new IllegalArgumentException("lastOffset must not be smaller than firstOffset");
            }
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.acknowledgeTypes =
                    List.copyOf(Objects.requireNonNull(acknowledgeTypes, "acknowledgeTypes"));
            if (this.acknowledgeTypes.isEmpty()) {
                throw new IllegalArgumentException("acknowledgeTypes must not be empty");
            }
        }

        public long getFirstOffset() {
            return firstOffset;
        }

        public long getLastOffset() {
            return lastOffset;
        }

        public List<Byte> getAcknowledgeTypes() {
            return acknowledgeTypes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AcknowledgementBatch that = (AcknowledgementBatch) o;
            return firstOffset == that.firstOffset
                    && lastOffset == that.lastOffset
                    && acknowledgeTypes.equals(that.acknowledgeTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstOffset, lastOffset, acknowledgeTypes);
        }

        @Override
        public String toString() {
            return "AcknowledgementBatch{"
                    + "firstOffset="
                    + firstOffset
                    + ", lastOffset="
                    + lastOffset
                    + ", acknowledgeTypes="
                    + acknowledgeTypes
                    + '}';
        }
    }
}
