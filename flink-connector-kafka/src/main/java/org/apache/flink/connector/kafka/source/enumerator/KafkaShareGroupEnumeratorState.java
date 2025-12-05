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

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * State class for KafkaShareGroupEnumerator that stores minimal information needed for
 * checkpointing and recovery.
 *
 * <p>Unlike regular Kafka partition enumerator states that track complex partition metadata, share
 * group enumerator state is minimal since:
 *
 * <ul>
 *   <li>No offset tracking (handled by share group protocol)
 *   <li>No partition discovery (topics are configured upfront)
 *   <li>No split lifecycle management (coordinator handles distribution)
 * </ul>
 */
public class KafkaShareGroupEnumeratorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<String> topics;
    private final String shareGroupId;

    /**
     * Creates enumerator state for share group source.
     *
     * @param topics the set of topics being consumed
     * @param shareGroupId the share group identifier
     */
    public KafkaShareGroupEnumeratorState(Set<String> topics, String shareGroupId) {
        this.topics = Objects.requireNonNull(topics, "Topics cannot be null");
        this.shareGroupId = Objects.requireNonNull(shareGroupId, "Share group ID cannot be null");
    }

    /** Gets the topics being consumed. */
    public Set<String> getTopics() {
        return topics;
    }

    /** Gets the share group ID. */
    public String getShareGroupId() {
        return shareGroupId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaShareGroupEnumeratorState that = (KafkaShareGroupEnumeratorState) obj;
        return Objects.equals(topics, that.topics)
                && Objects.equals(shareGroupId, that.shareGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topics, shareGroupId);
    }

    @Override
    public String toString() {
        return String.format(
                "KafkaShareGroupEnumeratorState{topics=%s, shareGroup='%s'}", topics, shareGroupId);
    }
}
