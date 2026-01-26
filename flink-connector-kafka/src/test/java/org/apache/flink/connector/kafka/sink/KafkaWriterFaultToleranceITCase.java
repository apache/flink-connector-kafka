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
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for the standalone KafkaWriter in case of fault tolerance. */
public class KafkaWriterFaultToleranceITCase extends KafkaWriterTestBase {
    private static final String INIT_KAFKA_RETRIES = "0";
    private static final String INIT_KAFKA_REQUEST_TIMEOUT_MS = "1000";
    private static final String INIT_KAFKA_MAX_BLOCK_MS = "1000";
    /**
     * The delivery timeout has to be greater than the request timeout as the latter is part of the
     * former and this is enforced by a compile time check.
     */
    private static final String INIT_KAFKA_DELIVERY_TIMEOUT_MS = "1500";

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(8)
                            .setConfiguration(new Configuration())
                            .build());

    @Test
    void testWriteExceptionWhenKafkaUnavailable() throws Exception {
        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        try (KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        new SinkInitContext(metricGroup, timeService, null))) {

            writer.write(1, SINK_WRITER_CONTEXT);

            KAFKA_CONTAINER.stop();

            try {
                writer.getCurrentProducer().flush();
                assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                        .rootCause()
                        .isInstanceOfAny(NetworkException.class, TimeoutException.class);
            } finally {
                KAFKA_CONTAINER.start();
            }
        }
    }

    @Test
    void testFlushExceptionWhenKafkaUnavailable() throws Exception {
        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        try (KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        new SinkInitContext(metricGroup, timeService, null))) {
            writer.write(1, SINK_WRITER_CONTEXT);

            KAFKA_CONTAINER.stop();
            try {
                assertThatCode(() -> writer.flush(false))
                        .rootCause()
                        .isInstanceOfAny(NetworkException.class, TimeoutException.class);
            } finally {
                KAFKA_CONTAINER.start();
            }
        }
    }

    @Test
    void testCloseExceptionWhenKafkaUnavailable() throws Exception {
        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        KafkaWriter<Integer> writer =
                createWriter(
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        new SinkInitContext(metricGroup, timeService, null));

        writer.write(1, SINK_WRITER_CONTEXT);

        KAFKA_CONTAINER.stop();

        try {
            writer.getCurrentProducer().flush();
            // closing producer resource throws exception first
            assertThatCode(() -> writer.close())
                    .rootCause()
                    .isInstanceOfAny(NetworkException.class, TimeoutException.class);
        } catch (Exception e) {
            writer.close();
            throw e;
        } finally {
            KAFKA_CONTAINER.start();
        }
    }

    @Test
    void testMailboxExceptionWhenKafkaUnavailable() throws Exception {
        SinkInitContext sinkInitContext =
                new SinkInitContext(createSinkWriterMetricGroup(), timeService, null);

        try (KafkaWriter<Integer> writer =
                createWriter(DeliveryGuarantee.AT_LEAST_ONCE, sinkInitContext)) {

            KAFKA_CONTAINER.stop();

            writer.write(1, SINK_WRITER_CONTEXT);

            try {
                writer.getCurrentProducer().flush();

                assertThatCode(
                                () -> {
                                    while (sinkInitContext.getMailboxExecutor().tryYield()) {
                                        // execute all mails
                                    }
                                })
                        .hasRootCauseExactlyInstanceOf(TimeoutException.class);
            } finally {
                KAFKA_CONTAINER.start();
            }
        }
    }

    @Override
    protected Properties getKafkaClientConfiguration() {
        Properties properties = super.getKafkaClientConfiguration();

        // reduce the default vault for test case
        properties.setProperty("retries", INIT_KAFKA_RETRIES);
        properties.setProperty("request.timeout.ms", INIT_KAFKA_REQUEST_TIMEOUT_MS);
        properties.setProperty("max.block.ms", INIT_KAFKA_MAX_BLOCK_MS);
        properties.setProperty("delivery.timeout.ms", INIT_KAFKA_DELIVERY_TIMEOUT_MS);

        return properties;
    }
}
