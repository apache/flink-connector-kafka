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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Internal
public class KafkaShareEosCommittable {

    public enum CommitPhase {
        READY,
        SINK_COMMITTED
    }

    private final long checkpointId;
    private final List<KafkaCommittable> kafkaCommittables;
    private final List<ShareAckCommittable> shareAckCommittables;
    private final CommitPhase commitPhase;

    public KafkaShareEosCommittable(
            long checkpointId,
            Collection<KafkaCommittable> kafkaCommittables,
            Collection<ShareAckCommittable> shareAckCommittables,
            CommitPhase commitPhase) {
        this.checkpointId = checkpointId;
        this.kafkaCommittables = copy(kafkaCommittables);
        this.shareAckCommittables = copy(shareAckCommittables);
        this.commitPhase = commitPhase;
    }

    public static KafkaShareEosCommittable ready(
            long checkpointId,
            Collection<KafkaCommittable> kafkaCommittables,
            Collection<ShareAckCommittable> shareAckCommittables) {
        return new KafkaShareEosCommittable(
                checkpointId, kafkaCommittables, shareAckCommittables, CommitPhase.READY);
    }

    private static <T> List<T> copy(Collection<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public List<KafkaCommittable> getKafkaCommittables() {
        return kafkaCommittables;
    }

    public List<ShareAckCommittable> getShareAckCommittables() {
        return shareAckCommittables;
    }

    public CommitPhase getCommitPhase() {
        return commitPhase;
    }

    public KafkaShareEosCommittable withSinkCommitted() {
        if (commitPhase == CommitPhase.SINK_COMMITTED) {
            return this;
        }
        return new KafkaShareEosCommittable(
                checkpointId,
                kafkaCommittables,
                shareAckCommittables,
                CommitPhase.SINK_COMMITTED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaShareEosCommittable that = (KafkaShareEosCommittable) o;
        return checkpointId == that.checkpointId
                && kafkaCommittables.equals(that.kafkaCommittables)
                && shareAckCommittables.equals(that.shareAckCommittables)
                && commitPhase == that.commitPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId, kafkaCommittables, shareAckCommittables, commitPhase);
    }

    @Override
    public String toString() {
        return "KafkaShareEosCommittable{"
                + "checkpointId="
                + checkpointId
                + ", kafkaCommittables="
                + kafkaCommittables
                + ", shareAckCommittables="
                + shareAckCommittables
                + ", commitPhase="
                + commitPhase
                + '}';
    }
}
