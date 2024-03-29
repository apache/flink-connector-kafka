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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;

/** Configurations for KafkaSource. */
@Internal
public class KafkaSourceOptions {

    public static final ConfigOption<String> CLIENT_ID_PREFIX =
            ConfigOptions.key("client.id.prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prefix to use for the Kafka consumers.");

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key("partition.discovery.interval.ms")
                    .longType()
                    .defaultValue(Duration.ofMinutes(5).toMillis())
                    .withDescription(
                            "The interval in milliseconds for the Kafka source to discover "
                                    + "the new partitions. A non-positive value disables the partition discovery."
                                    + "The default value is 5 minutes, which is equal to the default value of metadata.max.age.ms in Kafka.");

    public static final ConfigOption<Boolean> REGISTER_KAFKA_CONSUMER_METRICS =
            ConfigOptions.key("register.consumer.metrics")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to register metrics of KafkaConsumer into Flink metric group");

    public static final ConfigOption<Boolean> COMMIT_OFFSETS_ON_CHECKPOINT =
            ConfigOptions.key("commit.offsets.on.checkpoint")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to commit consuming offset on checkpoint.");

    @SuppressWarnings("unchecked")
    public static <T> T getOption(
            Properties props, ConfigOption<?> configOption, Function<String, T> parser) {
        String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }
}
