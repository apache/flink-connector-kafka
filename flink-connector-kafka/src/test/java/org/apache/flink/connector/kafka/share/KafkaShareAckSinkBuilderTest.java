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

package org.apache.flink.connector.kafka.share;

import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaShareAckSinkBuilderTest {

    @Test
    void testBuildsSinkFromProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(
                KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG, "orders-pipeline-v1");

        KafkaShareAckSink sink =
                KafkaShareAckSink.builder().setKafkaProducerConfig(properties).build();

        assertThat(sink.getShareExactlyOnceId()).isEqualTo("orders-pipeline-v1");
        assertThat(sink.getDeliveryGuarantee()).isEqualTo(DeliveryGuarantee.AT_LEAST_ONCE);
        assertThat(sink.getKafkaProducerConfig())
                .containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                .containsEntry(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName())
                .containsEntry(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName())
                .doesNotContainKey(KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG);
    }

    @Test
    void testBuildsSinkFromDedicatedSetters() {
        KafkaShareAckSink sink =
                KafkaShareAckSink.builder()
                        .setBootstrapServers("localhost:9092")
                        .setShareExactlyOnceId("orders-pipeline-v1")
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build();

        assertThat(sink.getShareExactlyOnceId()).isEqualTo("orders-pipeline-v1");
        assertThat(sink.getDeliveryGuarantee()).isEqualTo(DeliveryGuarantee.EXACTLY_ONCE);
    }

    @Test
    void testSetPropertyHandlesShareExactlyOnceIdAsConnectorOption() {
        KafkaShareAckSink sink =
                KafkaShareAckSink.builder()
                        .setBootstrapServers("localhost:9092")
                        .setProperty(
                                KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG,
                                "orders-pipeline-v1")
                        .build();

        assertThat(sink.getShareExactlyOnceId()).isEqualTo("orders-pipeline-v1");
        assertThat(sink.getKafkaProducerConfig())
                .doesNotContainKey(KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG);
    }

    @Test
    void testRejectsNoneDeliveryGuarantee() {
        assertThatThrownBy(
                        () ->
                                KafkaShareAckSink.builder()
                                        .setBootstrapServers("localhost:9092")
                                        .setShareExactlyOnceId("orders-pipeline-v1")
                                        .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must wait for checkpoint completion");
    }

    @Test
    void testRejectsMissingShareExactlyOnceId() {
        assertThatThrownBy(
                        () ->
                                KafkaShareAckSink.builder()
                                        .setBootstrapServers("localhost:9092")
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining(KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG);
    }

    @Test
    void testRemovesDefaultTransactionTimeoutWhenTwoPhaseCommitIsEnabled() {
        KafkaShareAckSink sink =
                KafkaShareAckSink.builder()
                        .setBootstrapServers("localhost:9092")
                        .setShareExactlyOnceId("orders-pipeline-v1")
                        .setProperty(
                                ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, "true")
                        .build();

        assertThat(sink.getKafkaProducerConfig())
                .doesNotContainKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
    }

    @Test
    void testRejectsExplicitTransactionTimeoutWhenTwoPhaseCommitIsEnabled() {
        assertThatThrownBy(
                        () ->
                                KafkaShareAckSink.builder()
                                        .setBootstrapServers("localhost:9092")
                                        .setShareExactlyOnceId("orders-pipeline-v1")
                                        .setProperty(
                                                ProducerConfig
                                                        .TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG,
                                                "true")
                                        .setProperty(
                                                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
    }
}
