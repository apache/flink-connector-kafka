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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.TestLoggerExtension;

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for the standalone KafkaWriter. */
@ExtendWith(TestLoggerExtension.class)
public class ExactlyOnceKafkaWriterITCase extends KafkaWriterTestBase {

    @Test
    void testFlushAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);

        // test flush
        assertThatCode(() -> writer.flush(false))
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .as("the exception is not thrown again")
                .doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        // async exception is checked and thrown on close
        assertThatCode(writer::close).hasRootCauseInstanceOf(ProducerFencedException.class);
    }

    @Test
    void testWriteAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);
        // to ensure that the exceptional send request has completed
        writer.getCurrentProducer().flush();

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .as("the exception is not thrown again")
                .doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        // async exception is checked and thrown on close
        assertThatCode(writer::close).hasRootCauseInstanceOf(ProducerFencedException.class);
    }

    @Test
    void testMailboxAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        SinkInitContext sinkInitContext =
                new SinkInitContext(createSinkWriterMetricGroup(), timeService, null);

        final KafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE, sinkInitContext);
        final Counter numRecordsOutErrors =
                sinkInitContext.metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);
        // to ensure that the exceptional send request has completed
        writer.getCurrentProducer().flush();

        assertThatCode(
                        () -> {
                            while (sinkInitContext.getMailboxExecutor().tryYield()) {
                                // execute all mails
                            }
                        })
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);

        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                .as("the exception is not thrown again")
                .doesNotThrowAnyException();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);

        // async exception is checked and thrown on close
        assertThatCode(writer::close).hasRootCauseInstanceOf(ProducerFencedException.class);
    }

    @Test
    void testCloseAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.EXACTLY_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));
        final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(0L);

        triggerProducerException(writer, properties);
        // to ensure that the exceptional send request has completed
        writer.getCurrentProducer().flush();

        // test flush
        assertThatCode(writer::close)
                .as("flush should throw the exception from the WriterCallback")
                .hasRootCauseExactlyInstanceOf(ProducerFencedException.class);
        assertThat(numRecordsOutErrors.getCount()).isEqualTo(1L);
    }

    private void triggerProducerException(KafkaWriter<Integer> writer, Properties properties)
            throws IOException {
        final String transactionalId = writer.getCurrentProducer().getTransactionalId();

        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "1".getBytes()));
            producer.commitTransaction();
        }

        writer.write(1, SINK_WRITER_CONTEXT);
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @Test
    void testLingeringTransaction() throws Exception {
        final KafkaWriter<Integer> failedWriter = createWriter(DeliveryGuarantee.EXACTLY_ONCE);

        // create two lingering transactions
        failedWriter.flush(false);
        failedWriter.prepareCommit();
        failedWriter.snapshotState(1);
        failedWriter.flush(false);
        failedWriter.prepareCommit();
        failedWriter.snapshotState(2);

        try (final KafkaWriter<Integer> recoveredWriter =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            recoveredWriter.write(1, SINK_WRITER_CONTEXT);

            recoveredWriter.flush(false);
            Collection<KafkaCommittable> committables = recoveredWriter.prepareCommit();
            recoveredWriter.snapshotState(1);
            assertThat(committables).hasSize(1);
            final KafkaCommittable committable = committables.stream().findFirst().get();
            assertThat(committable.getProducer().isPresent()).isTrue();

            committable.getProducer().get().getObject().commitTransaction();

            List<ConsumerRecord<byte[], byte[]>> records =
                    drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true);
            assertThat(records).hasSize(1);
        }

        failedWriter.close();
    }

    /** Test that producers are reused when committed. */
    @Test
    void usePooledProducerForTransactional() throws Exception {
        try (final ExactlyOnceKafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(writer.getProducerPool()).hasSize(0);

            writer.write(1, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<KafkaCommittable> committables0 = writer.prepareCommit();
            writer.snapshotState(1);
            assertThat(committables0).hasSize(1);
            final KafkaCommittable committable = committables0.stream().findFirst().get();
            assertThat(committable.getProducer().isPresent()).isTrue();

            FlinkKafkaInternalProducer<?, ?> firstProducer =
                    committable.getProducer().get().getObject();
            assertThat(firstProducer != writer.getCurrentProducer())
                    .as("Expected different producer")
                    .isTrue();

            // recycle first producer, KafkaCommitter would commit it and then return it
            assertThat(writer.getProducerPool()).hasSize(0);
            firstProducer.commitTransaction();
            committable.getProducer().get().close();
            assertThat(writer.getProducerPool()).hasSize(1);

            writer.write(1, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<KafkaCommittable> committables1 = writer.prepareCommit();
            writer.snapshotState(2);
            assertThat(committables1).hasSize(1);
            final KafkaCommittable committable1 = committables1.stream().findFirst().get();
            assertThat(committable1.getProducer().isPresent()).isTrue();

            assertThat(firstProducer == writer.getCurrentProducer())
                    .as("Expected recycled producer")
                    .isTrue();
        }
    }

    /**
     * Tests that if a pre-commit attempt occurs on an empty transaction, the writer should not emit
     * a KafkaCommittable, and instead immediately commit the empty transaction and recycle the
     * producer.
     */
    @Test
    void prepareCommitForEmptyTransaction() throws Exception {
        try (final ExactlyOnceKafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(writer.getProducerPool()).hasSize(0);

            // no data written to current transaction
            writer.flush(false);
            Collection<KafkaCommittable> emptyCommittables = writer.prepareCommit();

            assertThat(emptyCommittables).hasSize(0);
            assertThat(writer.getProducerPool()).hasSize(1);
            final FlinkKafkaInternalProducer<?, ?> recycledProducer =
                    Iterables.getFirst(writer.getProducerPool(), null);
            assertThat(recycledProducer.isInTransaction()).isFalse();
        }
    }

    /**
     * Tests that open transactions are automatically aborted on close such that successive writes
     * succeed.
     */
    @Test
    void testAbortOnClose() throws Exception {
        Properties properties = getKafkaClientConfiguration();
        try (final KafkaWriter<Integer> writer = createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(drainAllRecordsFromTopic(topic, properties, true)).hasSize(0);
        }

        try (final KafkaWriter<Integer> writer = createWriter(DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(2, SINK_WRITER_CONTEXT);
            writer.flush(false);
            Collection<KafkaCommittable> committables = writer.prepareCommit();
            writer.snapshotState(1L);

            // manually commit here, which would only succeed if the first transaction was aborted
            assertThat(committables).hasSize(1);
            final KafkaCommittable committable = committables.stream().findFirst().get();
            String transactionalId = committable.getTransactionalId();
            try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
                producer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
                producer.commitTransaction();
            }

            assertThat(drainAllRecordsFromTopic(topic, properties, true)).hasSize(1);
        }
    }
}
