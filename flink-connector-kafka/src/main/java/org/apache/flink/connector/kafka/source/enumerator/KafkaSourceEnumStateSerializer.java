/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.utils.SerdeUtils;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit.MIGRATED;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer Serializer} for the enumerator
 * state of Kafka source.
 */
@Internal
public class KafkaSourceEnumStateSerializer
        implements SimpleVersionedSerializer<KafkaSourceEnumState> {

    /**
     * state of VERSION_0 contains splitAssignments, which is a mapping from subtask ids to lists of
     * assigned splits.
     */
    private static final int VERSION_0 = 0;
    /** state of VERSION_1 only contains assignedPartitions, which is a list of assigned splits. */
    private static final int VERSION_1 = 1;
    /**
     * state of VERSION_2 contains initialDiscoveryFinished and partitions with different assignment
     * status.
     */
    private static final int VERSION_2 = 2;

    private static final int VERSION_3 = 3;

    private static final int CURRENT_VERSION = VERSION_3;

    private static final KafkaPartitionSplitSerializer SPLIT_SERIALIZER =
            new KafkaPartitionSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KafkaSourceEnumState enumState) throws IOException {
        return serializeV3(enumState);
    }

    @VisibleForTesting
    static byte[] serializeV3(KafkaSourceEnumState enumState) throws IOException {
        Set<SplitAndAssignmentStatus> splits = enumState.splits();
        boolean initialDiscoveryFinished = enumState.initialDiscoveryFinished();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(splits.size());
            out.writeInt(SPLIT_SERIALIZER.getVersion());
            for (SplitAndAssignmentStatus split : splits) {
                final byte[] splitBytes = SPLIT_SERIALIZER.serialize(split.split());
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
                out.writeInt(split.assignmentStatus().getStatusCode());
            }
            out.writeBoolean(initialDiscoveryFinished);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_3:
                return deserializeVersion3(serialized);
            case VERSION_2:
                return deserializeVersion2(serialized);
            case VERSION_1:
                return deserializeVersion1(serialized);
            case VERSION_0:
                return deserializeVersion0(serialized);
            default:
                throw new IOException(
                        String.format(
                                "The bytes are serialized with version %d, "
                                        + "while this deserializer only supports version up to %d",
                                version, CURRENT_VERSION));
        }
    }

    private static KafkaSourceEnumState deserializeVersion3(byte[] serialized) throws IOException {

        final KafkaPartitionSplitSerializer splitSerializer = new KafkaPartitionSplitSerializer();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            final int numPartitions = in.readInt();
            final int splitVersion = in.readInt();
            Set<SplitAndAssignmentStatus> partitions = new HashSet<>(numPartitions);

            for (int i = 0; i < numPartitions; i++) {
                final KafkaPartitionSplit split =
                        splitSerializer.deserialize(splitVersion, in.readNBytes(in.readInt()));
                final int statusCode = in.readInt();
                partitions.add(
                        new SplitAndAssignmentStatus(
                                split, AssignmentStatus.ofStatusCode(statusCode)));
            }
            final boolean initialDiscoveryFinished = in.readBoolean();
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }

            return new KafkaSourceEnumState(partitions, initialDiscoveryFinished);
        }
    }

    private static KafkaSourceEnumState deserializeVersion0(byte[] serialized) throws IOException {
        Map<Integer, Set<KafkaPartitionSplit>> currentPartitionAssignment =
                SerdeUtils.deserializeSplitAssignments(
                        serialized, new KafkaPartitionSplitSerializer(), HashSet::new);
        Set<KafkaPartitionSplit> currentAssignedSplits = new HashSet<>();
        for (Map.Entry<Integer, Set<KafkaPartitionSplit>> entry :
                currentPartitionAssignment.entrySet()) {
            currentAssignedSplits.addAll(entry.getValue());
        }
        return new KafkaSourceEnumState(currentAssignedSplits, new HashSet<>(), true);
    }

    @VisibleForTesting
    static byte[] serializeV1(Collection<KafkaPartitionSplit> splits) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(splits.size());
            for (KafkaPartitionSplit split : splits) {
                final TopicPartition tp = split.getTopicPartition();
                out.writeUTF(tp.topic());
                out.writeInt(tp.partition());
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    private static KafkaSourceEnumState deserializeVersion1(byte[] serializedTopicPartitions)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedTopicPartitions);
                DataInputStream in = new DataInputStream(bais)) {

            final int numPartitions = in.readInt();
            Set<SplitAndAssignmentStatus> partitions = new HashSet<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                final String topic = in.readUTF();
                final int partition = in.readInt();
                partitions.add(
                        new SplitAndAssignmentStatus(
                                new KafkaPartitionSplit(
                                        new TopicPartition(topic, partition), MIGRATED),
                                AssignmentStatus.ASSIGNED));
            }
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }
            return new KafkaSourceEnumState(partitions, true);
        }
    }

    @VisibleForTesting
    static byte[] serializeV2(
            Collection<SplitAndAssignmentStatus> splits, boolean initialDiscoveryFinished)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(splits.size());
            for (SplitAndAssignmentStatus splitAndAssignmentStatus : splits) {
                final TopicPartition topicPartition =
                        splitAndAssignmentStatus.split().getTopicPartition();
                out.writeUTF(topicPartition.topic());
                out.writeInt(topicPartition.partition());
                out.writeInt(splitAndAssignmentStatus.assignmentStatus().getStatusCode());
            }
            out.writeBoolean(initialDiscoveryFinished);
            out.flush();
            return baos.toByteArray();
        }
    }

    private static KafkaSourceEnumState deserializeVersion2(byte[] serialized) throws IOException {

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            final int numPartitions = in.readInt();
            Set<SplitAndAssignmentStatus> partitions = new HashSet<>(numPartitions);

            for (int i = 0; i < numPartitions; i++) {
                final String topic = in.readUTF();
                final int partition = in.readInt();
                final int statusCode = in.readInt();
                final AssignmentStatus assignStatus = AssignmentStatus.ofStatusCode(statusCode);
                partitions.add(
                        new SplitAndAssignmentStatus(
                                new KafkaPartitionSplit(
                                        new TopicPartition(topic, partition), MIGRATED),
                                assignStatus));
            }
            final boolean initialDiscoveryFinished = in.readBoolean();
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }

            return new KafkaSourceEnumState(partitions, initialDiscoveryFinished);
        }
    }
}
