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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaPropertiesUtil}. */
class KafkaPropertiesUtilTest {

    @Test
    void testUsesInitializerStrategyWhenResetPropertiesAreAbsent() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetReset(
                                new Properties(), new Properties(), OffsetsInitializer.earliest()))
                .isEqualTo("earliest");

        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetReset(
                                new Properties(), new Properties(), OffsetsInitializer.latest()))
                .isEqualTo("latest");
    }

    @Test
    void testClusterResetPropertyOverridesInitializerStrategy() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetReset(
                                new Properties(),
                                resetProperties("none"),
                                OffsetsInitializer.earliest()))
                .isEqualTo("none");
    }

    @Test
    void testClusterResetPropertyOverridesGlobalAndInitializerStrategies() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetReset(
                                resetProperties("none"),
                                resetProperties("earliest"),
                                OffsetsInitializer.latest()))
                .isEqualTo("earliest");
    }

    @Test
    void testResolvesByDurationReset() {
        assertThat(
                        KafkaPropertiesUtil.resolveAutoOffsetReset(
                                resetProperties("by_duration:PT1H"),
                                new Properties(),
                                OffsetsInitializer.earliest()))
                .isEqualTo("by_duration:PT1H");
    }

    @ParameterizedTest
    @ValueSource(strings = {"earliest", "EARLIEST", "Latest", "none"})
    void testNormalizesEnumStrategiesToLowerCase(String offsetReset) {
        assertThat(KafkaPropertiesUtil.normalizeAutoOffsetReset(offsetReset))
                .isEqualTo(offsetReset.toLowerCase(Locale.ROOT));
    }

    @ParameterizedTest
    @ValueSource(strings = {"by_duration:PT1H", "by_duration:P1DT30M"})
    void testAcceptsByDurationUnchanged(String offsetReset) {
        assertThat(KafkaPropertiesUtil.normalizeAutoOffsetReset(offsetReset))
                .isEqualTo(offsetReset);
        assertThat(KafkaPropertiesUtil.parseOffsetResetStrategy(offsetReset)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"errorStrategy", "by_duration:", "by_duration:-PT1H", " latest"})
    void testRejectsValuesTheConsumerRejects(String offsetReset) {
        assertThatThrownBy(() -> KafkaPropertiesUtil.normalizeAutoOffsetReset(offsetReset))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(
                        "auto.offset.reset can not be set to " + offsetReset + ". Valid values: ")
                .hasMessageContaining("by_duration");
    }

    @ParameterizedTest
    @MethodSource("allOffsetResetStrategyPairs")
    void testDetectsOnlyOpposingPositionalResetStrategies(
            OffsetResetStrategy configuredResetStrategy,
            OffsetResetStrategy initializerResetStrategy) {
        boolean expected =
                (configuredResetStrategy == OffsetResetStrategy.EARLIEST
                                && initializerResetStrategy == OffsetResetStrategy.LATEST)
                        || (configuredResetStrategy == OffsetResetStrategy.LATEST
                                && initializerResetStrategy == OffsetResetStrategy.EARLIEST);

        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                configuredResetStrategy.name().toLowerCase(Locale.ROOT),
                                OffsetsInitializer.committedOffsets(initializerResetStrategy)))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("allOffsetResetStrategies")
    void testByDurationNeverOpposesInitializerStrategy(
            OffsetResetStrategy initializerResetStrategy) {
        assertThat(
                        KafkaPropertiesUtil.hasOpposingOffsetResetStrategies(
                                "by_duration:PT1H",
                                OffsetsInitializer.committedOffsets(initializerResetStrategy)))
                .isFalse();
    }

    private static Stream<OffsetResetStrategy> allOffsetResetStrategies() {
        return Arrays.stream(OffsetResetStrategy.values());
    }

    private static Stream<Arguments> allOffsetResetStrategyPairs() {
        return Arrays.stream(OffsetResetStrategy.values())
                .flatMap(
                        configuredResetStrategy ->
                                Arrays.stream(OffsetResetStrategy.values())
                                        .map(
                                                initializerResetStrategy ->
                                                        Arguments.of(
                                                                configuredResetStrategy,
                                                                initializerResetStrategy)));
    }

    private static Properties resetProperties(String resetStrategy) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetStrategy);
        return properties;
    }
}
