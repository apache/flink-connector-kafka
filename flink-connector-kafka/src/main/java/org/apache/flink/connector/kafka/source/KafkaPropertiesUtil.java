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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

/** Utility class for modify Kafka properties. */
@Internal
public class KafkaPropertiesUtil {

    private KafkaPropertiesUtil() {}

    public static void copyProperties(@Nonnull Properties from, @Nonnull Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    /** Resolves an explicit cluster or global reset strategy before the initializer default. */
    public static OffsetResetStrategy resolveAutoOffsetResetStrategy(
            @Nonnull Properties globalProperties,
            @Nonnull Properties clusterProperties,
            @Nonnull OffsetsInitializer startingOffsetsInitializer) {
        return getResetStrategy(
                clusterProperties.getProperty(
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        globalProperties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                startingOffsetsInitializer.getAutoOffsetResetStrategy().name())));
    }

    /** Parses the configured auto offset reset strategy. */
    public static OffsetResetStrategy getResetStrategy(@Nonnull String offsetResetConfig) {
        return Arrays.stream(OffsetResetStrategy.values())
                .filter(
                        offsetResetStrategy ->
                                offsetResetStrategy
                                        .name()
                                        .equals(offsetResetConfig.toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "%s can not be set to %s. Valid values: [%s]",
                                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                offsetResetConfig,
                                                Arrays.stream(OffsetResetStrategy.values())
                                                        .map(Enum::name)
                                                        .map(String::toLowerCase)
                                                        .collect(Collectors.joining(",")))));
    }

    /** Returns whether the configured strategy opposes a positional initializer strategy. */
    public static boolean hasOpposingOffsetResetStrategies(
            @Nonnull OffsetResetStrategy configuredResetStrategy,
            @Nonnull OffsetsInitializer startingOffsetsInitializer) {
        OffsetResetStrategy initializerResetStrategy =
                startingOffsetsInitializer.getAutoOffsetResetStrategy();
        return isPositionalResetStrategy(configuredResetStrategy)
                && isPositionalResetStrategy(initializerResetStrategy)
                && configuredResetStrategy != initializerResetStrategy;
    }

    private static boolean isPositionalResetStrategy(OffsetResetStrategy resetStrategy) {
        return resetStrategy == OffsetResetStrategy.EARLIEST
                || resetStrategy == OffsetResetStrategy.LATEST;
    }

    /**
     * client.id is used for Kafka server side logging, see
     * https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_client.id
     *
     * <p>Set client id prefix to avoid mbean collision warning logs. There are multiple instances
     * of the AdminClient/KafkaConsumer so each instance requires a different client id (i.e. also
     * per cluster).
     *
     * <p>Flink internally configures the clientId, making this the only way to customize the Kafka
     * client id parameter.
     *
     * <p>If this is not done, we will encounter warning logs of the form:
     *
     * <p>WARN org.apache.kafka.common.utils.AppInfoParser [] - Error registering AppInfo mbean
     * javax.management.InstanceAlreadyExistsException:
     * kafka.consumer:type=app-info,id=null-enumerator-consumer
     *
     * <p>WARN org.apache.kafka.common.utils.AppInfoParser [] - Error registering AppInfo mbean
     * javax.management.InstanceAlreadyExistsException:
     * kafka.admin.client:type=app-info,id=null-enumerator-admin-client
     */
    public static void setClientIdPrefix(Properties properties, String kafkaClusterId) {
        String userClientIdPrefix =
                properties.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        properties.setProperty(
                KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
                userClientIdPrefix + "-" + kafkaClusterId);
    }
}
