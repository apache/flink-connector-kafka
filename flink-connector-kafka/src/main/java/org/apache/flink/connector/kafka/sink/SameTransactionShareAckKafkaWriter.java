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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.connector.kafka.share.ShareAckPayloadStager;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * A sink writer that commits Kafka share-group acknowledgements (KIP-1289) in the <em>same</em> Kafka
 * transaction as the sink output records, so that source acknowledgement and sink output are made
 * durable atomically by a single transaction-commit marker.
 *
 * <p>Acknowledgements are staged into the current producer transaction <em>as soon as each record
 * reaches the sink</em> (in {@link #write}), not batched until {@code prepareCommit}. Staging moves
 * the broker-side records from {@code ACQUIRED} to {@code TX_PENDING}, which cancels the
 * acquisition-lock timer immediately and shrinks the window in which a record's lock could expire (or
 * its member epoch could be fenced) while held inside the Flink pipeline.
 *
 * <p>v1 scope is forwarding-only: acks ride on the data record, so every ack-bearing record must
 * reach the sink. Pipelines that drop ack-bearing records before the sink (filter/aggregate/join) are
 * not yet supported.
 */
@Internal
class SameTransactionShareAckKafkaWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, KafkaWriterState, KafkaCommittable> {

    private final SameTransactionWriterDelegate<IN> delegate;
    private final Function<IN, Collection<ShareAckPayload>> shareAckPayloadExtractor;
    private final ShareAckPayloadBuffer payloadBuffer;
    private final ShareAckStager stager;

    SameTransactionShareAckKafkaWriter(
            ExactlyOnceKafkaWriter<IN> delegate,
            Function<IN, Collection<ShareAckPayload>> shareAckPayloadExtractor) {
        this(
                new ExactlyOnceWriterDelegate<>(delegate),
                shareAckPayloadExtractor,
                new ShareAckPayloadBuffer(),
                ShareAckPayloadStager::stage);
    }

    SameTransactionShareAckKafkaWriter(
            SameTransactionWriterDelegate<IN> delegate,
            Function<IN, Collection<ShareAckPayload>> shareAckPayloadExtractor,
            ShareAckPayloadBuffer payloadBuffer,
            ShareAckStager stager) {
        this.delegate = delegate;
        this.shareAckPayloadExtractor = shareAckPayloadExtractor;
        this.payloadBuffer = payloadBuffer;
        this.stager = stager;
    }

    void initialize() {
        delegate.initialize();
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        delegate.write(element, context);
        if (element == null) {
            return;
        }
        // Stage each record's share acknowledgements into the same (open) transaction immediately.
        // If staging fails, the exception propagates and the checkpoint fails, so the transaction is
        // aborted wholesale on recovery (all-or-nothing): sink output for this window is never made
        // visible and the records are redelivered. We never commit output while dropping an ack.
        for (ShareAckPayload payload : shareAckPayloadExtractor.apply(element)) {
            if (payloadBuffer.register(payload)) {
                stager.stage(delegate.currentProducer(), payload);
                delegate.markShareAcksStaged();
            }
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        delegate.flush(endOfInput);
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() throws IOException, InterruptedException {
        Collection<KafkaCommittable> committables = delegate.prepareCommit();
        if (!committables.isEmpty()) {
            // The transaction (with its staged acks) has been precommitted; start tracking the next
            // window's payloads afresh.
            payloadBuffer.clear();
        }
        return committables;
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    interface SameTransactionWriterDelegate<IN>
            extends TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                    IN, KafkaWriterState, KafkaCommittable> {

        void initialize();

        Object currentProducer();

        void markShareAcksStaged();
    }

    /** Stages a single share acknowledgement payload into the given producer's transaction. */
    @FunctionalInterface
    interface ShareAckStager {
        void stage(Object producer, ShareAckPayload payload) throws IOException;
    }

    private static final class ExactlyOnceWriterDelegate<IN>
            implements SameTransactionWriterDelegate<IN> {

        private final ExactlyOnceKafkaWriter<IN> writer;

        private ExactlyOnceWriterDelegate(ExactlyOnceKafkaWriter<IN> writer) {
            this.writer = writer;
        }

        @Override
        public void initialize() {
            writer.initialize();
        }

        @Override
        public Object currentProducer() {
            return writer.getCurrentProducer();
        }

        @Override
        public void markShareAcksStaged() {
            writer.getCurrentProducer().markShareAcksStaged();
        }

        @Override
        public void write(IN element, Context context) throws IOException, InterruptedException {
            writer.write(element, context);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            writer.flush(endOfInput);
        }

        @Override
        public Collection<KafkaCommittable> prepareCommit()
                throws IOException, InterruptedException {
            return writer.prepareCommit();
        }

        @Override
        public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
            return writer.snapshotState(checkpointId);
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }
}
