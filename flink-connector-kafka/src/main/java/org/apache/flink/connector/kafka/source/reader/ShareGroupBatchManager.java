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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages batches of records from Kafka share consumer for checkpoint persistence.
 * Controls when new batches can be fetched to work within share consumer's auto-commit constraints.
 */
public class ShareGroupBatchManager<K, V> 
        implements ListCheckpointed<ShareGroupBatchForCheckpoint<K, V>>, CheckpointListener {
    
    private static final Logger LOG = LoggerFactory.getLogger(ShareGroupBatchManager.class);
    
    private final List<ShareGroupBatchForCheckpoint<K, V>> pendingBatches;
    private final String splitId;
    private final AtomicInteger batchIdGenerator;

    public ShareGroupBatchManager(String splitId) {
        this.splitId = splitId;
        this.pendingBatches = new ArrayList<>();
        this.batchIdGenerator = new AtomicInteger(0);
    }

    /**
     * Adds a new batch of records to be managed.
     * 
     * @param records List of consumer records from poll()
     */
    public void addBatch(List<ConsumerRecord<K, V>> records) {
        if (records.isEmpty()) {
            return;
        }
        
        long batchId = batchIdGenerator.incrementAndGet();
        ShareGroupBatchForCheckpoint<K, V> batch = new ShareGroupBatchForCheckpoint<>(batchId, records);
        pendingBatches.add(batch);
        
        LOG.info("ShareGroup [{}]: Added batch {} with {} records, total pending batches: {}", 
                 splitId, batchId, records.size(), pendingBatches.size());
    }

    /**
     * Returns unprocessed records from all batches for downstream emission.
     * Marks returned records as emitted to track processing state.
     * 
     * @return Records ready for downstream processing
     */
    public RecordsWithSplitIds<ConsumerRecord<K, V>> getNextUnprocessedRecords() {
        List<ConsumerRecord<K, V>> unprocessed = new ArrayList<>();
        
        for (ShareGroupBatchForCheckpoint<K, V> batch : pendingBatches) {
            for (ConsumerRecord<K, V> record : batch.getRecords()) {
                ShareGroupBatchForCheckpoint.RecordProcessingState state = batch.getRecordState(record);
                if (!state.isEmittedDownstream()) {
                    unprocessed.add(record);
                    state.setEmittedDownstream(true);
                }
            }
        }
        
        if (!unprocessed.isEmpty()) {
            LOG.info("ShareGroup [{}]: Emitting {} records downstream for processing", splitId, unprocessed.size());
        }
        
        return RecordsWithSplitIds.forRecords(splitId, unprocessed);
    }

    /**
     * Checks if there are any batches with unprocessed records.
     * Used to control when new polling should occur.
     * 
     * @return true if batches exist that haven't completed sink processing
     */
    public boolean hasUnprocessedBatches() {
        return pendingBatches.stream().anyMatch(batch -> !batch.allRecordsReachedSink());
    }

    /**
     * Returns total count of pending batches.
     */
    public int getPendingBatchCount() {
        return pendingBatches.size();
    }

    /**
     * Returns total count of pending records across all batches.
     */
    public int getPendingRecordCount() {
        return pendingBatches.stream().mapToInt(ShareGroupBatchForCheckpoint::size).sum();
    }

    @Override
    public List<ShareGroupBatchForCheckpoint<K, V>> snapshotState(long checkpointId, long timestamp) {
        // Associate current checkpoint ID with all pending batches
        pendingBatches.forEach(batch -> batch.setCheckpointId(checkpointId));
        
        int totalRecords = getPendingRecordCount();
        LOG.info("ShareGroup [{}]: Checkpoint {} - Snapshotting {} batches ({} records) for at-least-once recovery", 
                 splitId, checkpointId, pendingBatches.size(), totalRecords);
        
        return new ArrayList<>(pendingBatches);
    }

    @Override
    public void restoreState(List<ShareGroupBatchForCheckpoint<K, V>> restoredBatches) {
        this.pendingBatches.clear();
        this.pendingBatches.addAll(restoredBatches);
        
        // Reset emission state for records that were emitted but didn't reach sink
        int replayCount = 0;
        for (ShareGroupBatchForCheckpoint<K, V> batch : pendingBatches) {
            for (ConsumerRecord<K, V> record : batch.getRecords()) {
                ShareGroupBatchForCheckpoint.RecordProcessingState state = batch.getRecordState(record);
                if (state.isEmittedDownstream() && !state.isReachedSink()) {
                    state.setEmittedDownstream(false);
                    replayCount++;
                }
            }
        }
        
        int totalRecords = getPendingRecordCount();
        if (replayCount > 0) {
            LOG.info("ShareGroup [{}]: RECOVERY - Restored {} batches ({} total records), {} records marked for replay due to incomplete processing", 
                     splitId, pendingBatches.size(), totalRecords, replayCount);
        } else {
            LOG.info("ShareGroup [{}]: RECOVERY - Restored {} batches ({} records), all previously processed successfully", 
                     splitId, pendingBatches.size(), totalRecords);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Iterator<ShareGroupBatchForCheckpoint<K, V>> iterator = pendingBatches.iterator();
        int completedBatches = 0;
        
        while (iterator.hasNext()) {
            ShareGroupBatchForCheckpoint<K, V> batch = iterator.next();
            
            if (batch.getCheckpointId() <= checkpointId) {
                // Mark all records as successfully processed through pipeline
                batch.markAllRecordsReachedSink();
                iterator.remove();
                completedBatches++;
            }
        }
        
        if (completedBatches > 0) {
            LOG.info("ShareGroup [{}]: Checkpoint {} SUCCESS - Completed {} batches, {} batches remaining", 
                     splitId, checkpointId, completedBatches, pendingBatches.size());
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        int totalRecords = getPendingRecordCount();
        LOG.info("ShareGroup [{}]: Checkpoint {} ABORTED - Retaining {} batches ({} records) for recovery", 
                 splitId, checkpointId, pendingBatches.size(), totalRecords);
    }
}