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

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Share Group Split for Kafka topics using share group semantics.
 * 
 * <p>Unlike regular Kafka partition splits, share group splits represent entire topics.
 * Multiple readers can be assigned the same topic, and Kafka's share group coordinator
 * will distribute messages at the record level across all consumers in the share group.
 * 
 * <p>Key differences from KafkaPartitionSplit:
 * <ul>
 *   <li>Topic-based, not partition-based</li>
 *   <li>No offset tracking (handled by share group protocol)</li>
 *   <li>Multiple readers can have the same topic</li>
 *   <li>Message-level distribution by Kafka coordinator</li>
 * </ul>
 */
public class KafkaShareGroupSplit implements SourceSplit {
    
    private static final long serialVersionUID = 1L;
    
    private final String topicName;
    private final String shareGroupId;
    private final int readerId;
    private final String splitId;
    
    /**
     * Creates a share group split for a topic.
     * 
     * @param topicName the Kafka topic name
     * @param shareGroupId the share group identifier
     * @param readerId unique identifier for the reader (usually subtask ID)
     */
    public KafkaShareGroupSplit(String topicName, String shareGroupId, int readerId) {
        this.topicName = Objects.requireNonNull(topicName, "Topic name cannot be null");
        this.shareGroupId = Objects.requireNonNull(shareGroupId, "Share group ID cannot be null");
        this.readerId = readerId;
        this.splitId = createSplitId(shareGroupId, topicName, readerId);
    }
    
    @Override
    public String splitId() {
        return splitId;
    }
    
    /**
     * Gets the topic name for this split.
     */
    public String getTopicName() {
        return topicName;
    }
    
    /**
     * Gets the share group ID.
     */
    public String getShareGroupId() {
        return shareGroupId;
    }
    
    /**
     * Gets the reader ID (typically subtask ID).
     */
    public int getReaderId() {
        return readerId;
    }
    
    /**
     * Creates a unique split ID for the share group split.
     */
    private static String createSplitId(String shareGroupId, String topicName, int readerId) {
        return String.format("share-group-%s-topic-%s-reader-%d", shareGroupId, topicName, readerId);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        KafkaShareGroupSplit that = (KafkaShareGroupSplit) obj;
        return readerId == that.readerId &&
               Objects.equals(topicName, that.topicName) &&
               Objects.equals(shareGroupId, that.shareGroupId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(topicName, shareGroupId, readerId);
    }
    
    @Override
    public String toString() {
        return String.format("KafkaShareGroupSplit{topic='%s', shareGroup='%s', reader=%d}", 
                topicName, shareGroupId, readerId);
    }
}