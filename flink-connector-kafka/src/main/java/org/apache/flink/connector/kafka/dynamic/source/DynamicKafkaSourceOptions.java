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

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Properties;
import java.util.function.Function;

/**
 * The connector options for {@link DynamicKafkaSource} that can be passed through the source
 * properties e.g. {@link DynamicKafkaSourceBuilder#setProperties(Properties)}.
 */
@Internal
public class DynamicKafkaSourceOptions {

    private DynamicKafkaSourceOptions() {}

    public static final ConfigOption<Long> STREAM_METADATA_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key("stream-metadata-discovery-interval-ms")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The interval in milliseconds for the sink to discover "
                                    + "the changes in stream metadata. A non-positive value disables the stream metadata discovery.");

    public static final ConfigOption<Integer> STREAM_METADATA_DISCOVERY_FAILURE_THRESHOLD =
            ConfigOptions.key("stream-metadata-discovery-failure-threshold")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of consecutive failures before letting the exception from Kafka metadata service discovery "
                                    + "trigger jobmanager failure and global failover. The default is one to at least catch startup "
                                    + "failures. This is only implemented for the source");

    @Internal
    public static <T> T getOption(
            Properties props, ConfigOption<?> configOption, Function<String, T> parser) {
        String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }
}
