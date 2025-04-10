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
import java.util.Map;
import java.util.Objects;

/**
 * Kafka stream represents multiple topics over multiple Kafka clusters and this class encapsulates
 * all the necessary information to initiate Kafka consumers to read a stream.
 */
@Experimental
public class KafkaStream implements Serializable {
    private final String streamId;
    private final Map<String, ClusterMetadata> clusterMetadataMap;

    /**
     * Construct a {@link KafkaStream} by passing Kafka information in order to connect to the
     * stream.
     *
     * @param streamId the stream id.
     * @param clusterMetadataMap the map of clusters to {@link ClusterMetadata} to connect to the
     *     stream.
     */
    public KafkaStream(String streamId, Map<String, ClusterMetadata> clusterMetadataMap) {
        this.streamId = streamId;
        this.clusterMetadataMap = clusterMetadataMap;
    }

    /**
     * Get the stream id.
     *
     * @return the stream id.
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Get the metadata to connect to the various cluster(s).
     *
     * @return the cluster metadata map.
     */
    public Map<String, ClusterMetadata> getClusterMetadataMap() {
        return clusterMetadataMap;
    }

    @Override
    public String toString() {
        return "KafkaStream{"
                + "streamId='"
                + streamId
                + '\''
                + ", clusterMetadataMap="
                + clusterMetadataMap
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaStream that = (KafkaStream) o;
        return Objects.equals(streamId, that.streamId)
                && Objects.equals(clusterMetadataMap, that.clusterMetadataMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, clusterMetadataMap);
    }
}
