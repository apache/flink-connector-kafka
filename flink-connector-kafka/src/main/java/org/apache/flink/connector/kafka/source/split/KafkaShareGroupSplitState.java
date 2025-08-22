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

package org.apache.flink.connector.kafka.source.split;

import java.util.Objects;

/**
 * State wrapper for KafkaShareGroupSplit.
 * 
 * <p>Unlike regular Kafka partition split states that track offsets and other metadata,
 * share group split states are minimal since the Kafka share group coordinator handles
 * message delivery state automatically.
 * 
 * <p>This state primarily exists for:
 * <ul>
 *   <li>Flink's split lifecycle management</li>
 *   <li>Checkpoint integration</li>
 *   <li>Split recovery after failures</li>
 * </ul>
 */
public class KafkaShareGroupSplitState {
    
    private final KafkaShareGroupSplit split;
    private boolean subscribed;
    
    /**
     * Creates a state wrapper for the share group split.
     */
    public KafkaShareGroupSplitState(KafkaShareGroupSplit split) {
        this.split = Objects.requireNonNull(split, "Split cannot be null");
        this.subscribed = false;
    }
    
    /**
     * Gets the underlying share group split.
     */
    public KafkaShareGroupSplit toKafkaShareGroupSplit() {
        return split;
    }
    
    /**
     * Gets the split ID.
     */
    public String getSplitId() {
        return split.splitId();
    }
    
    /**
     * Gets the topic name.
     */
    public String getTopicName() {
        return split.getTopicName();
    }
    
    /**
     * Gets the share group ID.
     */
    public String getShareGroupId() {
        return split.getShareGroupId();
    }
    
    /**
     * Gets the reader ID.
     */
    public int getReaderId() {
        return split.getReaderId();
    }
    
    /**
     * Marks this split as subscribed.
     */
    public void setSubscribed(boolean subscribed) {
        this.subscribed = subscribed;
    }
    
    /**
     * Returns whether this split is subscribed.
     */
    public boolean isSubscribed() {
        return subscribed;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        KafkaShareGroupSplitState that = (KafkaShareGroupSplitState) obj;
        return Objects.equals(split, that.split) && subscribed == that.subscribed;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(split, subscribed);
    }
    
    @Override
    public String toString() {
        return String.format("KafkaShareGroupSplitState{split=%s, subscribed=%s}", split, subscribed);
    }
}