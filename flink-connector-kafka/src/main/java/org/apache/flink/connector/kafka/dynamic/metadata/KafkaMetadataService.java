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

package org.apache.flink.connector.kafka.dynamic.metadata;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/** Metadata service that returns Kafka details. */
@Experimental
public interface KafkaMetadataService extends AutoCloseable, Serializable {
    /**
     * Get current metadata for all streams.
     *
     * @return set of all streams
     */
    Set<KafkaStream> getAllStreams();

    /**
     * Get current metadata for queried streams.
     *
     * @param streamIds stream full names
     * @return map of stream name to metadata
     */
    Map<String, KafkaStream> describeStreams(Collection<String> streamIds);

    /**
     * Check if the cluster is active.
     *
     * @param kafkaClusterId Kafka cluster id
     * @return boolean whether the cluster is active
     */
    boolean isClusterActive(String kafkaClusterId);
}
