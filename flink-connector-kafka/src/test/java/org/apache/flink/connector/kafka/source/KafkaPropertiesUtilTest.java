/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaPropertiesUtil}. */
class KafkaPropertiesUtilTest {

    @Test
    void testUsesInitializerStrategyWhenResetPropertiesAreAbsent() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetResetStrategy(
                                new Properties(), new Properties(), OffsetsInitializer.earliest()))
                .isEqualTo("earliest");

        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetResetStrategy(
                                new Properties(), new Properties(), OffsetsInitializer.latest()))
                .isEqualTo("latest");
    }

    @Test
    void testClusterResetPropertyOverridesInitializerStrategy() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetResetStrategy(
                                new Properties(),
                                resetProperties("none"),
                                OffsetsInitializer.earliest()))
                .isEqualTo("none");
    }

    @Test
    void testClusterResetPropertyOverridesGlobalAndInitializerStrategies() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetResetStrategy(
                                resetProperties("none"),
                                resetProperties("earliest"),
                                OffsetsInitializer.latest()))
                .isEqualTo("earliest");
    }

    @Test
    void testDetectsOnlyOpposingPositionalResetStrategies() {
        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                "latest", OffsetsInitializer.earliest()))
                .isTrue();
        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                "earliest", OffsetsInitializer.latest()))
                .isTrue();

        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                "none", OffsetsInitializer.earliest()))
                .isFalse();
        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                "earliest", OffsetsInitializer.earliest()))
                .isFalse();
        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                "latest", OffsetsInitializer.committedOffsets()))
                .isFalse();
    }

    private static Properties resetProperties(String resetStrategy) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetStrategy);
        return properties;
    }
}
