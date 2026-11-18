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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.TransactionAbortStrategyImpl;
import org.apache.flink.connector.kafka.sink.internal.TransactionNamingStrategyImpl;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for {@link ExactlyOnceKafkaWriter}. */
@ExtendWith(TestLoggerExtension.class)
class ExactlyOnceKafkaWriterTest {

    @Test
    void testShareStagingUsesTheOutputProducerBeforePrepare() throws Exception {
        final ExactlyOnceKafkaWriter<Integer> writer = createWriter(createSinkWriterMetricGroup());
        final MockProducer producer = new MockProducer(writer.deliveryCallback, null);
        writer.currentProducer = producer;
        writer.setShareAckPayloadExtractor(
                ignored -> List.of(sharePayload()), new RecordingPayloadBuffer());
        try {
            writer.write(1, new KafkaWriterTestBase.DummySinkWriterContext());
            writer.write(1, new KafkaWriterTestBase.DummySinkWriterContext());

            assertThat(producer.events).containsExactly("produce", "stage", "produce");
            assertThat(writer.prepareCommit()).hasSize(1);
            assertThat(producer.events).containsExactly("produce", "stage", "produce", "prepare");
        } finally {
            writer.close();
        }
    }

    @Test
    void testShareStagingFailureIsPropagatedBeforePrepare() throws Exception {
        final ExactlyOnceKafkaWriter<Integer> writer = createWriter(createSinkWriterMetricGroup());
        final MockProducer producer = new MockProducer(writer.deliveryCallback, null);
        producer.stageException = new TimeoutException("stage failed");
        writer.currentProducer = producer;
        writer.setShareAckPayloadExtractor(
                ignored -> List.of(sharePayload()), new RecordingPayloadBuffer());
        try {
            assertThatThrownBy(
                            () -> writer.write(1, new KafkaWriterTestBase.DummySinkWriterContext()))
                    .isInstanceOf(TimeoutException.class)
                    .hasMessage("stage failed");
            assertThat(producer.events).containsExactly("produce", "stage");
            assertThat(producer.shareAcksStaged).isFalse();
        } finally {
            writer.close();
        }
    }

    private static ShareAckPayload sharePayload() {
        return new ShareAckPayload(
                "ack",
                "group",
                "member",
                1,
                List.of(
                        new ShareAckPayload.TopicPartitionAcknowledgements(
                                "AAAAAAAAAAAAAAAAAAAAAA",
                                "input",
                                0,
                                List.of(
                                        new ShareAckPayload.AcknowledgementBatch(
                                                0, 0, List.of((byte) 1))))));
    }

    @Test
    void testPrepareAcknowledgementOnlyTransaction() throws Exception {
        final ExactlyOnceKafkaWriter<Integer> writer = createWriter(createSinkWriterMetricGroup());
        final MockProducer producer = new MockProducer(writer.deliveryCallback, null, true);
        writer.currentProducer = producer;

        assertThat(producer.hasRecordsInTransaction()).isFalse();
        assertThat(writer.prepareCommit()).hasSize(1);
        writer.close();
        assertThat(producer.aborted).isFalse();
    }

    @Test
    void testCloseAbortsUnpreparedAcknowledgementOnlyTransaction() throws Exception {
        final ExactlyOnceKafkaWriter<Integer> writer = createWriter(createSinkWriterMetricGroup());
        final MockProducer producer = new MockProducer(writer.deliveryCallback, null, true);
        writer.currentProducer = producer;

        writer.close();

        assertThat(producer.aborted).isTrue();
    }

    @Test
    void testCloseIgnoresAbortTriggeredAsyncError() {
        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        final ExactlyOnceKafkaWriter<Integer> writer = createWriter(metricGroup);
        writer.currentProducer =
                new MockProducer(
                        writer.deliveryCallback,
                        new TransactionAbortedException("Transaction aborted during close"));

        assertThatCode(writer::close).doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);
    }

    @Test
    void testClosePropagatesAsyncErrorReportedBeforeClose() {
        final ExactlyOnceKafkaWriter<Integer> writer = createWriter(createSinkWriterMetricGroup());
        writer.currentProducer = new MockProducer(writer.deliveryCallback, null);
        writer.deliveryCallback.onCompletion(
                null, new ProducerFencedException("Producer fenced before close"));

        assertThatCode(writer::close).hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
    }

    private static ExactlyOnceKafkaWriter<Integer> createWriter(SinkWriterMetricGroup metricGroup) {
        return new ExactlyOnceKafkaWriter<>(
                DeliveryGuarantee.EXACTLY_ONCE,
                getKafkaClientConfiguration(),
                "test-prefix",
                new KafkaWriterTestBase.SinkInitContext(
                        metricGroup, new KafkaWriterTestBase.TriggerTimeService(), null),
                (element, context, timestamp) -> new ProducerRecord<>("topic", new byte[0]),
                null,
                TransactionAbortStrategyImpl.PROBING,
                TransactionNamingStrategyImpl.INCREMENTING,
                List.of());
    }

    private static SinkWriterMetricGroup createSinkWriterMetricGroup() {
        return InternalSinkWriterMetricGroup.wrap(
                new KafkaWriterTestBase.DummyOperatorMetricGroup(
                        new MetricListener().getMetricGroup()));
    }

    private static Properties getKafkaClientConfiguration() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return properties;
    }

    private static class RecordingPayloadBuffer extends ShareAckPayloadBuffer {
        @Override
        void stageForRecord(
                Object producer,
                boolean transactionHasRecords,
                Collection<ShareAckPayload> payloads)
                throws IOException {
            addAll(payloads);
            stage(
                    producer,
                    transactionHasRecords,
                    (p, payload) -> {
                        final MockProducer mock = (MockProducer) p;
                        mock.events.add("stage");
                        if (mock.stageException != null) {
                            throw mock.stageException;
                        }
                        mock.markShareAcksStaged();
                    });
        }
    }

    private static class MockProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {

        private final Callback callback;
        @Nullable private final RuntimeException abortException;
        private boolean shareAcksStaged;
        private boolean aborted;
        private boolean recordsSent;
        private final List<String> events = new ArrayList<>();
        @Nullable private RuntimeException stageException;

        private MockProducer(Callback callback, @Nullable RuntimeException abortException) {
            this(callback, abortException, false);
        }

        private MockProducer(
                Callback callback,
                @Nullable RuntimeException abortException,
                boolean shareAcksStaged) {
            super(getKafkaClientConfiguration());
            this.callback = callback;
            this.abortException = abortException;
            this.shareAcksStaged = shareAcksStaged;
        }

        @Override
        public boolean hasWorkInTransaction() {
            return abortException != null || shareAcksStaged || recordsSent;
        }

        @Override
        public boolean hasRecordsInTransaction() {
            return recordsSent;
        }

        @Override
        public Future<RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record, Callback callback) {
            events.add("produce");
            recordsSent = true;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void markShareAcksStaged() {
            shareAcksStaged = true;
        }

        @Override
        public Optional<String> precommitTransaction() {
            events.add("prepare");
            recordsSent = false;
            shareAcksStaged = false;
            return Optional.empty();
        }

        @Override
        public long getProducerId() {
            return 42L;
        }

        @Override
        public short getEpoch() {
            return 0;
        }

        @Override
        public void abortTransaction() {
            aborted = true;
            callback.onCompletion(null, abortException);
        }
    }
}
