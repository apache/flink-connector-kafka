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
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@Internal
public final class ShareEosCheckpointLedger implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String eosId;
    private final long checkpointId;
    private final ShareAckCommitPhase phase;
    private final List<KafkaCommittable> outputCommittables;
    private final List<ShareAckCommittable> shareAckCommittables;

    public ShareEosCheckpointLedger(
            String eosId,
            long checkpointId,
            ShareAckCommitPhase phase,
            Collection<KafkaCommittable> outputCommittables,
            Collection<ShareAckCommittable> shareAckCommittables) {
        this.eosId = Objects.requireNonNull(eosId, "eosId");
        this.checkpointId = checkpointId;
        this.phase = Objects.requireNonNull(phase, "phase");
        this.outputCommittables =
                List.copyOf(Objects.requireNonNull(outputCommittables, "outputCommittables"));
        this.shareAckCommittables =
                List.copyOf(Objects.requireNonNull(shareAckCommittables, "shareAckCommittables"));
    }

    public String getEosId() {
        return eosId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public ShareAckCommitPhase getPhase() {
        return phase;
    }

    public List<KafkaCommittable> getOutputCommittables() {
        return outputCommittables;
    }

    public List<ShareAckCommittable> getShareAckCommittables() {
        return shareAckCommittables;
    }

    public boolean outputsCommitted() {
        return phase == ShareAckCommitPhase.OUTPUTS_COMMITTED
                || phase == ShareAckCommitPhase.COMMITTING_SHARE_ACKS
                || phase == ShareAckCommitPhase.DONE;
    }

    public ShareEosCheckpointLedger withPhase(ShareAckCommitPhase phase) {
        return new ShareEosCheckpointLedger(
                eosId, checkpointId, phase, outputCommittables, shareAckCommittables);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShareEosCheckpointLedger that = (ShareEosCheckpointLedger) o;
        return checkpointId == that.checkpointId
                && eosId.equals(that.eosId)
                && phase == that.phase
                && outputCommittables.equals(that.outputCommittables)
                && shareAckCommittables.equals(that.shareAckCommittables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                eosId, checkpointId, phase, outputCommittables, shareAckCommittables);
    }

    @Override
    public String toString() {
        return "ShareEosCheckpointLedger{"
                + "eosId='"
                + eosId
                + '\''
                + ", checkpointId="
                + checkpointId
                + ", phase="
                + phase
                + ", outputCommittables="
                + outputCommittables
                + ", shareAckCommittables="
                + shareAckCommittables
                + '}';
    }
}
