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
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for {@link ExactlyOnceKafkaWriter}. */
@ExtendWith(TestLoggerExtension.class)
class ExactlyOnceKafkaWriterTest {

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

    private static class MockProducer extends FlinkKafkaInternalProducer<byte[], byte[]> {

        private final Callback callback;
        @Nullable private final RuntimeException abortException;
        private boolean shareAcksStaged;
        private boolean aborted;

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
            return abortException != null || shareAcksStaged;
        }

        @Override
        public Optional<String> precommitTransaction() {
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
