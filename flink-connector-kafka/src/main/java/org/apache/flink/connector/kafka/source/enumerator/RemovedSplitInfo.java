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
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata about a removed split that is retained as a tombstone for potential resurrection.
 *
 * <p>When a split is removed from the source (e.g., due to Kafka metadata service instability), it
 * can be retained in checkpoints for a configurable duration. If the split reappears within the
 * retention window, its progress can be restored to prevent duplicate processing or data loss.
 */
@Internal
public class RemovedSplitInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final KafkaPartitionSplit split;
    private final long removalTimestamp;

    /**
     * Creates a new RemovedSplitInfo.
     *
     * @param split The split that was removed
     * @param removalTimestamp The timestamp (in milliseconds) when the split was removed
     */
    public RemovedSplitInfo(KafkaPartitionSplit split, long removalTimestamp) {
        this.split = split;
        this.removalTimestamp = removalTimestamp;
    }

    public KafkaPartitionSplit getSplit() {
        return split;
    }

    public long getRemovalTimestamp() {
        return removalTimestamp;
    }

    /**
     * Checks if this tombstone has expired based on the given retention duration.
     *
     * @param currentTimestamp The current timestamp in milliseconds
     * @param retentionMs The retention duration in milliseconds
     * @return true if the tombstone has expired and should be removed
     */
    public boolean isExpired(long currentTimestamp, long retentionMs) {
        return currentTimestamp - removalTimestamp > retentionMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemovedSplitInfo that = (RemovedSplitInfo) o;
        return removalTimestamp == that.removalTimestamp && Objects.equals(split, that.split);
    }

    @Override
    public int hashCode() {
        return Objects.hash(split, removalTimestamp);
    }

    @Override
    public String toString() {
        return "RemovedSplitInfo{"
                + "split="
                + split
                + ", removalTimestamp="
                + removalTimestamp
                + '}';
    }
}
