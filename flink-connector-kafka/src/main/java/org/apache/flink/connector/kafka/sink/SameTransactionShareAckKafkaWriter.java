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

@Internal
class SameTransactionShareAckKafkaWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, KafkaWriterState, KafkaCommittable> {

    private final SameTransactionWriterDelegate<IN> delegate;
    private final Function<IN, Collection<ShareAckPayload>> shareAckPayloadExtractor;
    private final ShareAckPayloadBuffer payloadBuffer;

    SameTransactionShareAckKafkaWriter(
            ExactlyOnceKafkaWriter<IN> delegate,
            Function<IN, Collection<ShareAckPayload>> shareAckPayloadExtractor) {
        this(
                new ExactlyOnceWriterDelegate<>(delegate),
                shareAckPayloadExtractor,
                new ShareAckPayloadBuffer());
    }

    SameTransactionShareAckKafkaWriter(
            SameTransactionWriterDelegate<IN> delegate,
            Function<IN, Collection<ShareAckPayload>> shareAckPayloadExtractor,
            ShareAckPayloadBuffer payloadBuffer) {
        this.delegate = delegate;
        this.shareAckPayloadExtractor = shareAckPayloadExtractor;
        this.payloadBuffer = payloadBuffer;
    }

    void initialize() {
        delegate.initialize();
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        delegate.write(element, context);
        if (element != null) {
            payloadBuffer.addAll(shareAckPayloadExtractor.apply(element));
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        delegate.flush(endOfInput);
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() throws IOException, InterruptedException {
        boolean transactionHasRecords = delegate.currentTransactionHasRecords();
        payloadBuffer.stage(
                delegate.currentProducer(), transactionHasRecords, ShareAckPayloadStager::stage);
        Collection<KafkaCommittable> committables = delegate.prepareCommit();
        if (!committables.isEmpty()) {
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

        boolean currentTransactionHasRecords();
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
        public boolean currentTransactionHasRecords() {
            return writer.getCurrentProducer().hasRecordsInTransaction();
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
