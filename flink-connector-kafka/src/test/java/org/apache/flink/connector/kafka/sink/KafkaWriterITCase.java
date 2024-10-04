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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for the standalone KafkaWriter. */
@ExtendWith(TestLoggerExtension.class)
public class KafkaWriterITCase extends KafkaWriterTestBase {

    @BeforeAll
    public static void beforeAll() {
        KAFKA_CONTAINER.start();
    }

    @AfterAll
    public static void afterAll() {
        KAFKA_CONTAINER.stop();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    public void testRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        try (final KafkaWriter<Integer> ignored =
                createWriterWithConfiguration(getKafkaClientConfiguration(), guarantee)) {
            assertThat(metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME).isPresent()).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    public void testNotRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        assertKafkaMetricNotPresent(guarantee, "flink.disable-metrics", "true");
        assertKafkaMetricNotPresent(guarantee, "register.producer.metrics", "false");
    }

    @Test
    public void testIncreasingRecordBasedCounters() throws Exception {
        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        metricGroup)) {
            final Counter numBytesOut = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
            final Counter numRecordsOut = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
            final Counter numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
            final Counter numRecordsSendErrors = metricGroup.getNumRecordsSendErrorsCounter();

            // ApiVersionsRequest etc. is sent on initialization, so establish some baseline
            writer.write(0, SINK_WRITER_CONTEXT);
            writer.flush(false); // ensure data is actually written
            timeService.trigger(); // sync byte count
            long baselineCount = numBytesOut.getCount();
            assertThat(numRecordsOut.getCount()).isEqualTo(1);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);
            assertThat(numRecordsSendErrors.getCount()).isEqualTo(0);

            // elements for which the serializer returns null should be silently skipped
            writer.write(null, SINK_WRITER_CONTEXT);
            writer.flush(false);
            timeService.trigger(); // sync byte count
            assertThat(numBytesOut.getCount()).isEqualTo(baselineCount);
            assertThat(numRecordsOut.getCount()).isEqualTo(1);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);
            assertThat(numRecordsSendErrors.getCount()).isEqualTo(0);

            // but elements for which a non-null producer record is returned should count
            writer.write(1, SINK_WRITER_CONTEXT);
            writer.flush(false);
            timeService.trigger(); // sync byte count
            assertThat(numBytesOut.getCount()).isGreaterThan(baselineCount);
            assertThat(numRecordsOut.getCount()).isEqualTo(2);
            assertThat(numRecordsOutErrors.getCount()).isEqualTo(0);
            assertThat(numRecordsSendErrors.getCount()).isEqualTo(0);
        }
    }

    @Test
    public void testCurrentSendTimeMetric() throws Exception {
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.AT_LEAST_ONCE)) {
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");
            assertThat(currentSendTime.isPresent()).isTrue();
            assertThat(currentSendTime.get().getValue()).isEqualTo(0L);
            IntStream.range(0, 100)
                    .forEach(
                            (run) -> {
                                try {
                                    writer.write(1, SINK_WRITER_CONTEXT);
                                    // Manually flush the records to generate a sendTime
                                    if (run % 10 == 0) {
                                        writer.flush(false);
                                    }
                                } catch (IOException | InterruptedException e) {
                                    throw new RuntimeException("Failed writing Kafka record.");
                                }
                            });
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }
    }

    @Test
    void testFlushAsyncErrorPropagationAndErrorCounter() throws Exception {
        Properties properties = getKafkaClientConfiguration();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup);
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
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup);
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
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, sinkInitContext);
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
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup);
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
            producer.send(new ProducerRecord<byte[], byte[]>(topic, "1".getBytes()));
            producer.commitTransaction();
        }

        writer.write(1, SINK_WRITER_CONTEXT);
    }

    @Test
    public void testMetadataPublisher() throws Exception {
        List<String> metadataList = new ArrayList<>();
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        createSinkWriterMetricGroup(),
                        meta -> metadataList.add(meta.toString()))) {
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                writer.write(1, SINK_WRITER_CONTEXT);
                expected.add("testMetadataPublisher-0@" + i);
            }
            writer.flush(false);
            assertThat(metadataList).usingRecursiveComparison().isEqualTo(expected);
        }
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @Test
    void testLingeringTransaction() throws Exception {
        final KafkaWriter<Integer> failedWriter =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE);

        // create two lingering transactions
        failedWriter.flush(false);
        failedWriter.prepareCommit();
        failedWriter.snapshotState(1);
        failedWriter.flush(false);
        failedWriter.prepareCommit();
        failedWriter.snapshotState(2);

        try (final KafkaWriter<Integer> recoveredWriter =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
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

    /** Test that producer is not accidentally recreated or pool is used. */
    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = "EXACTLY_ONCE",
            mode = EnumSource.Mode.EXCLUDE)
    void useSameProducerForNonTransactional(DeliveryGuarantee guarantee) throws Exception {
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(getKafkaClientConfiguration(), guarantee)) {
            assertThat(writer.getProducerPool()).hasSize(0);

            FlinkKafkaInternalProducer<byte[], byte[]> firstProducer = writer.getCurrentProducer();
            writer.flush(false);
            Collection<KafkaCommittable> committables = writer.prepareCommit();
            writer.snapshotState(0);
            assertThat(committables).hasSize(0);

            assertThat(writer.getCurrentProducer() == firstProducer)
                    .as("Expected same producer")
                    .isTrue();
            assertThat(writer.getProducerPool()).hasSize(0);
        }
    }

    /** Test that producers are reused when committed. */
    @Test
    void usePoolForTransactional() throws Exception {
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
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
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(writer.getProducerPool()).hasSize(0);

            // no data written to current transaction
            writer.flush(false);
            Collection<KafkaCommittable> emptyCommittables = writer.prepareCommit();

            assertThat(emptyCommittables).hasSize(0);
            assertThat(writer.getProducerPool()).hasSize(1);
            final FlinkKafkaInternalProducer<?, ?> recycledProducer =
                    writer.getProducerPool().pop();
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
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(properties, DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(drainAllRecordsFromTopic(topic, properties, true)).hasSize(0);
        }

        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(properties, DeliveryGuarantee.EXACTLY_ONCE)) {
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

    private void assertKafkaMetricNotPresent(
            DeliveryGuarantee guarantee, String configKey, String configValue) throws Exception {
        final Properties config = getKafkaClientConfiguration();
        config.put(configKey, configValue);
        try (final KafkaWriter<Integer> ignored =
                createWriterWithConfiguration(config, guarantee)) {
            assertThat(metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME)).isNotPresent();
        }
    }
}
