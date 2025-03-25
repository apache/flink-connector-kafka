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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the standalone KafkaWriter. */
@ExtendWith(TestLoggerExtension.class)
public class KafkaWriterITCase extends KafkaWriterTestBase {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    public void testRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        try (final KafkaWriter<Integer> ignored = createWriter(guarantee)) {
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
                createWriter(
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        new SinkInitContext(metricGroup, timeService, null))) {
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
        try (final KafkaWriter<Integer> writer = createWriter(DeliveryGuarantee.AT_LEAST_ONCE)) {
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
            writer.flush(true);
        }
    }

    @Test
    public void testMetadataPublisher() throws Exception {
        List<String> metadataList = new ArrayList<>();
        SinkWriterMetricGroup sinkWriterMetricGroup = createSinkWriterMetricGroup();
        try (final KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        new SinkInitContext(
                                sinkWriterMetricGroup,
                                timeService,
                                meta -> metadataList.add(meta.topic() + "@" + meta.offset())))) {
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                writer.write(1, SINK_WRITER_CONTEXT);
                expected.add("testMetadataPublisher@" + i);
            }
            writer.flush(false);
            assertThat(metadataList).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = "EXACTLY_ONCE",
            mode = EnumSource.Mode.EXCLUDE)
    void useSameProducerForNonTransactional(DeliveryGuarantee guarantee) throws Exception {
        try (final KafkaWriter<Integer> writer = createWriter(guarantee)) {
            FlinkKafkaInternalProducer<byte[], byte[]> firstProducer = writer.getCurrentProducer();
            writer.flush(false);
            Collection<KafkaCommittable> committables = writer.prepareCommit();
            writer.snapshotState(0);
            assertThat(committables).hasSize(0);

            assertThat(writer.getCurrentProducer() == firstProducer)
                    .as("Expected same producer")
                    .isTrue();
        }
    }

    private void assertKafkaMetricNotPresent(
            DeliveryGuarantee guarantee, String configKey, String configValue) throws Exception {
        final Properties config = getKafkaClientConfiguration();
        config.put(configKey, configValue);
        try (final KafkaWriter<Integer> ignored =
                createWriter(
                        builder ->
                                builder.setKafkaProducerConfig(config)
                                        .setDeliveryGuarantee(guarantee),
                        createInitContext())) {
            assertThat(metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME)).isNotPresent();
        }
    }
}
