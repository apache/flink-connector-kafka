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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Enumerator for Kafka Share Group sources that assigns topic-based splits.
 * 
 * <p>This enumerator implements the key architectural principle for share groups:
 * it assigns topic-based splits to readers, where multiple readers can get the same
 * topic but with different reader IDs. Each reader creates its own KafkaShareConsumer
 * instance, and Kafka's share group coordinator distributes different messages
 * to each consumer.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Assigns same topic to multiple readers (enables > partitions parallelism)</li>
 *   <li>Each reader gets unique reader ID for distinct consumer instances</li>
 *   <li>No partition-level split management (share group handles distribution)</li>
 *   <li>Supports both cases: subtasks > partitions and subtasks <= partitions</li>
 * </ul>
 */
@Internal
public class KafkaShareGroupEnumerator implements SplitEnumerator<KafkaShareGroupSplit, KafkaShareGroupEnumeratorState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupEnumerator.class);
    
    private final SplitEnumeratorContext<KafkaShareGroupSplit> context;
    private final Set<String> topics;
    private final String shareGroupId;
    private final KafkaShareGroupEnumeratorState state;
    
    /**
     * Creates a share group enumerator.
     *
     * @param topics the set of topics to subscribe to
     * @param shareGroupId the share group identifier
     * @param state the enumerator state (for checkpointing)
     * @param context the enumerator context
     */
    public KafkaShareGroupEnumerator(
            Set<String> topics,
            String shareGroupId,
            @Nullable KafkaShareGroupEnumeratorState state,
            SplitEnumeratorContext<KafkaShareGroupSplit> context) {
        
        this.topics = topics;
        this.shareGroupId = shareGroupId;
        this.context = context;
        this.state = state != null ? state : new KafkaShareGroupEnumeratorState(topics, shareGroupId);
        
        LOG.info("*** ENUMERATOR: Created KafkaShareGroupEnumerator for topics {} with share group '{}' - {} readers expected", 
                topics, shareGroupId, context.currentParallelism());
                
        if (topics.isEmpty()) {
            LOG.warn("*** ENUMERATOR: No topics provided to enumerator! This will prevent split assignment.");
        }
    }
    
    @Override
    public void start() {
        LOG.info("*** ENUMERATOR: Starting KafkaShareGroupEnumerator for share group '{}' with {} topics", 
                shareGroupId, topics.size());
        
        if (topics.isEmpty()) {
            LOG.error("*** ENUMERATOR ERROR: Cannot start enumerator with empty topics! No splits will be assigned.");
            return;
        }
        
        // Assign splits to all available readers immediately
        assignSplitsToAllReaders();
    }
    
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("*** ENUMERATOR: Received split request from subtask {} for share group '{}' from host {}", 
                subtaskId, shareGroupId, requesterHostname);
        
        // For share groups, we assign splits immediately on reader registration
        // This is different from partition-based sources that wait for requests
        assignSplitsToReader(subtaskId);
    }
    
    @Override
    public void addSplitsBack(List<KafkaShareGroupSplit> splits, int subtaskId) {
        LOG.debug("Adding back {} splits from subtask {} to share group '{}'", 
                splits.size(), subtaskId, shareGroupId);
        
        // For share groups, splits don't need to be redistributed in the traditional sense
        // The share group coordinator will handle message redistribution automatically
        // We just log this for monitoring purposes
        for (KafkaShareGroupSplit split : splits) {
            LOG.debug("Split returned: {} from subtask {}", split.splitId(), subtaskId);
        }
    }
    
    @Override
    public void addReader(int subtaskId) {
        LOG.info("*** ENUMERATOR: Adding reader {} to share group '{}' - assigning topic splits. Current readers: {}", 
                subtaskId, shareGroupId, context.registeredReaders().keySet());
        assignSplitsToReader(subtaskId);
    }
    
    @Override
    public KafkaShareGroupEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting state for share group '{}' at checkpoint {}", shareGroupId, checkpointId);
        return state;
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing KafkaShareGroupEnumerator for share group '{}'", shareGroupId);
    }
    
    /**
     * Assigns splits to all currently registered readers.
     */
    private void assignSplitsToAllReaders() {
        LOG.info("*** ENUMERATOR: Assigning splits to all readers. Current registered readers: {}", 
                context.registeredReaders().keySet());
        for (int readerId : context.registeredReaders().keySet()) {
            assignSplitsToReader(readerId);
        }
    }
    
    /**
     * Assigns topic-based splits to a specific reader.
     * 
     * <p>The key insight: Each reader gets the same topics but with a unique reader ID.
     * This allows multiple readers to consume from the same topics while Kafka's
     * share group coordinator distributes different messages to each consumer.
     */
    private void assignSplitsToReader(int readerId) {
        if (topics.isEmpty()) {
            LOG.warn("*** ENUMERATOR: Cannot assign splits to reader {} - no topics available", readerId);
            return;
        }
        
        List<KafkaShareGroupSplit> splitsToAssign = new ArrayList<>();
        
        // Create a split for each topic - same topics for all readers but unique reader IDs
        for (String topic : topics) {
            KafkaShareGroupSplit split = new KafkaShareGroupSplit(topic, shareGroupId, readerId);
            splitsToAssign.add(split);
            
            LOG.info("*** ENUMERATOR: Assigning split to reader {}: {} (topic: {}, shareGroup: {})", 
                    readerId, split.splitId(), topic, shareGroupId);
        }
        
        if (!splitsToAssign.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(readerId, splitsToAssign)));
            LOG.info("*** ENUMERATOR: Successfully assigned {} splits to reader {} for share group '{}'", 
                    splitsToAssign.size(), readerId, shareGroupId);
        }
    }
}