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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ShareGroupBatchManager.
 */
public class ShareGroupBatchManagerTest {

    @Test
    public void testBatchAdditionAndRetrieval() {
        ShareGroupBatchManager<String, String> manager = new ShareGroupBatchManager<>("test-split");
        
        // Create test records
        List<ConsumerRecord<String, String>> records = Arrays.asList(
            new ConsumerRecord<>("topic1", 0, 100L, "key1", "value1"),
            new ConsumerRecord<>("topic1", 0, 101L, "key2", "value2")
        );
        
        // Add batch
        manager.addBatch(records);
        
        // Verify batch was added
        assertThat(manager.getPendingBatchCount()).isEqualTo(1);
        assertThat(manager.getPendingRecordCount()).isEqualTo(2);
        assertThat(manager.hasUnprocessedBatches()).isTrue();
        
        // Get unprocessed records
        var unprocessedRecords = manager.getNextUnprocessedRecords();
        assertThat(unprocessedRecords.nextSplit()).isEqualTo("test-split");
        
        // Count records returned
        int recordCount = 0;
        while (unprocessedRecords.nextRecordFromSplit() != null) {
            recordCount++;
        }
        assertThat(recordCount).isEqualTo(2);
    }

    @Test
    public void testCheckpointLifecycle() {
        ShareGroupBatchManager<String, String> manager = new ShareGroupBatchManager<>("test-split");
        
        // Add records
        List<ConsumerRecord<String, String>> records = Arrays.asList(
            new ConsumerRecord<>("topic1", 0, 100L, "key1", "value1")
        );
        manager.addBatch(records);
        
        // Process records
        manager.getNextUnprocessedRecords();
        
        // Snapshot state
        long checkpointId = 1L;
        var state = manager.snapshotState(checkpointId, System.currentTimeMillis());
        assertThat(state).hasSize(1);
        assertThat(state.get(0).getCheckpointId()).isEqualTo(checkpointId);
        
        // Complete checkpoint
        manager.notifyCheckpointComplete(checkpointId);
        assertThat(manager.getPendingBatchCount()).isEqualTo(0);
        assertThat(manager.hasUnprocessedBatches()).isFalse();
    }

    @Test
    public void testStateRestoration() {
        ShareGroupBatchManager<String, String> manager = new ShareGroupBatchManager<>("test-split");
        
        // Create test batch
        List<ConsumerRecord<String, String>> records = Arrays.asList(
            new ConsumerRecord<>("topic1", 0, 100L, "key1", "value1")
        );
        ShareGroupBatchForCheckpoint<String, String> batch = new ShareGroupBatchForCheckpoint<>(1L, records);
        batch.setCheckpointId(1L);
        
        // Mark as emitted but not reached sink
        var state = batch.getRecordState(records.get(0));
        state.setEmittedDownstream(true);
        state.setReachedSink(false);
        
        // Restore state
        manager.restoreState(Arrays.asList(batch));
        
        // Verify restoration
        assertThat(manager.getPendingBatchCount()).isEqualTo(1);
        assertThat(manager.hasUnprocessedBatches()).isTrue();
        
        // Should re-emit the record
        var unprocessedRecords = manager.getNextUnprocessedRecords();
        assertThat(unprocessedRecords.nextSplit()).isEqualTo("test-split");
        assertThat(unprocessedRecords.nextRecordFromSplit()).isNotNull();
    }
}