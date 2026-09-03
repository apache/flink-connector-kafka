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

/**
 * Requests a reader's retained offsets for one re-added cluster.
 *
 * <p>The handoff id correlates the response with this request so a delayed response from an older
 * remove/re-add cycle cannot satisfy the current all-reader barrier.
 */
@Internal
public class RequestRetainedSplitOffsetsEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final long handoffId;
    private final String kafkaClusterId;

    public RequestRetainedSplitOffsetsEvent(long handoffId, String kafkaClusterId) {
        this.handoffId = handoffId;
        this.kafkaClusterId = kafkaClusterId;
    }

    public long getHandoffId() {
        return handoffId;
    }

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    @Override
    public String toString() {
        return "RequestRetainedSplitOffsetsEvent{"
                + "handoffId="
                + handoffId
                + ", kafkaClusterId='"
                + kafkaClusterId
                + '\''
                + '}';
    }
}
