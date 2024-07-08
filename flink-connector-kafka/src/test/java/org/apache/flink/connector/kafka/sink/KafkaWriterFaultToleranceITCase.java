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
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

/** Tests for the standalone KafkaWriter in case of fault tolerance. */
@ExtendWith(TestLoggerExtension.class)
public class KafkaWriterFaultToleranceITCase extends KafkaWriterTestBase {
    private static final String INIT_KAFKA_RETRIES = "1";
    private static final String INIT_KAFKA_REQUEST_TIMEOUT_MS = "2000";
    private static final String INIT_KAFKA_MAX_BLOCK_MS = "3000";
    private static final String INIT_KAFKA_DELIVERY_TIMEOUT_MS = "4000";

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

    @Test
    void testWriteExceptionWhenKafkaUnavailable() throws Exception {
        Properties properties = getPropertiesForSendingFaultTolerance();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup);

        writer.write(1, SINK_WRITER_CONTEXT);

        KAFKA_CONTAINER.stop();

        try {
            writer.getCurrentProducer().flush();
            assertThatCode(() -> writer.write(1, SINK_WRITER_CONTEXT))
                    .hasRootCauseExactlyInstanceOf(TimeoutException.class);
        } finally {
            KAFKA_CONTAINER.start();
        }
    }

    @Test
    void testFlushExceptionWhenKafkaUnavailable() throws Exception {
        Properties properties = getPropertiesForSendingFaultTolerance();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup);
        writer.write(1, SINK_WRITER_CONTEXT);

        KAFKA_CONTAINER.stop();
        try {
            assertThatCode(() -> writer.flush(false))
                    .hasRootCauseExactlyInstanceOf(TimeoutException.class);
        } finally {
            KAFKA_CONTAINER.start();
        }
    }

    @Test
    void testCloseExceptionWhenKafkaUnavailable() throws Exception {
        Properties properties = getPropertiesForSendingFaultTolerance();

        final SinkWriterMetricGroup metricGroup = createSinkWriterMetricGroup();

        final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, metricGroup);

        writer.write(1, SINK_WRITER_CONTEXT);

        KAFKA_CONTAINER.stop();

        try {
            writer.getCurrentProducer().flush();
            // closing producer resource throws exception first
            assertThatCode(() -> writer.close())
                    .hasNoCause()
                    .hasMessage(
                            String.format(
                                    "Timeout expired after %sms while awaiting " + "InitProducerId",
                                    INIT_KAFKA_MAX_BLOCK_MS));
        } finally {
            KAFKA_CONTAINER.start();
        }
    }

    @Test
    void testMailboxExceptionWhenKafkaUnavailable() throws Exception {
        Properties properties = getPropertiesForSendingFaultTolerance();
        SinkInitContext sinkInitContext =
                new SinkInitContext(createSinkWriterMetricGroup(), timeService, null);

        final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        properties, DeliveryGuarantee.EXACTLY_ONCE, sinkInitContext);

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

    private Properties getPropertiesForSendingFaultTolerance() {
        Properties properties = getKafkaClientConfiguration();

        // reduce the default vault for test case
        properties.setProperty("retries", INIT_KAFKA_RETRIES);
        properties.setProperty("request.timeout.ms", INIT_KAFKA_REQUEST_TIMEOUT_MS);
        properties.setProperty("max.block.ms", INIT_KAFKA_MAX_BLOCK_MS);
        properties.setProperty("delivery.timeout.ms", INIT_KAFKA_DELIVERY_TIMEOUT_MS);

        return properties;
    }
}
