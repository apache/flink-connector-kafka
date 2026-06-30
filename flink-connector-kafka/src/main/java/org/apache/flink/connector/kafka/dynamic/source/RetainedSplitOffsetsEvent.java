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

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Reports retained removed-cluster split offsets from a reader to the enumerator.
 *
 * <p>The handoff id is copied from {@link RequestRetainedSplitOffsetsEvent}. Readers with no
 * retained splits for the requested cluster still send an empty response so the enumerator can wait
 * for every reader before assigning the cluster again.
 */
@Internal
public class RetainedSplitOffsetsEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final long handoffId;
    private final String kafkaClusterId;
    private final Map<String, Long> retainedSplitOffsets;

    public RetainedSplitOffsetsEvent(
            long handoffId, String kafkaClusterId, Map<String, Long> retainedSplitOffsets) {
        this.handoffId = handoffId;
        this.kafkaClusterId = kafkaClusterId;
        this.retainedSplitOffsets =
                Collections.unmodifiableMap(new HashMap<>(retainedSplitOffsets));
    }

    public long getHandoffId() {
        return handoffId;
    }

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    public Map<String, Long> getRetainedSplitOffsets() {
        return retainedSplitOffsets;
    }

    @Override
    public String toString() {
        return "RetainedSplitOffsetsEvent{"
                + "handoffId="
                + handoffId
                + ", kafkaClusterId='"
                + kafkaClusterId
                + '\''
                + ", retainedSplitOffsets="
                + retainedSplitOffsets
                + '}';
    }
}
