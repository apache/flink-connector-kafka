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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

/** Options for the Dynamic Kafka table connector. */
@PublicEvolving
public class DynamicKafkaConnectorOptions {

    public static final ConfigOption<List<String>> STREAM_IDS =
            ConfigOptions.key("stream-ids")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Stream id(s) to read data from when the table is used as source. "
                                    + "Only one of 'stream-ids' and 'stream-pattern' can be set.");

    public static final ConfigOption<String> STREAM_PATTERN =
            ConfigOptions.key("stream-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Regular expression pattern for stream ids to read data from. "
                                    + "Only one of 'stream-ids' and 'stream-pattern' can be set.");

    public static final ConfigOption<String> METADATA_SERVICE =
            ConfigOptions.key("metadata-service")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Metadata service implementation identifier. Use 'single-cluster' or "
                                    + "a fully qualified class name implementing KafkaMetadataService.");

    public static final ConfigOption<String> METADATA_SERVICE_CLUSTER_ID =
            ConfigOptions.key("metadata-service.cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka cluster id for the 'single-cluster' metadata service.");

    private DynamicKafkaConnectorOptions() {}
}
