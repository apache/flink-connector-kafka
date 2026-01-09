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
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * {@link ClusterMetadata} provides readers information about a cluster on what topics to read, how
 * to connect to a cluster, and optional offsets initializers.
 */
@Experimental
public class ClusterMetadata implements Serializable {
    private final Set<String> topics;
    private final Properties properties;
    @Nullable private final OffsetsInitializer startingOffsetsInitializer;
    @Nullable private final OffsetsInitializer stoppingOffsetsInitializer;

    /**
     * Constructs the {@link ClusterMetadata} with the required properties.
     *
     * @param topics the topics belonging to a cluster.
     * @param properties the properties to access a cluster.
     */
    public ClusterMetadata(Set<String> topics, Properties properties) {
        this(topics, properties, null, null);
    }

    /**
     * Constructs the {@link ClusterMetadata} with the required properties and offsets.
     *
     * @param topics the topics belonging to a cluster.
     * @param properties the properties to access a cluster.
     * @param startingOffsetsInitializer the starting offsets initializer for the cluster.
     * @param stoppingOffsetsInitializer the stopping offsets initializer for the cluster.
     */
    public ClusterMetadata(
            Set<String> topics,
            Properties properties,
            @Nullable OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer) {
        this.topics = topics;
        this.properties = properties;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
    }

    /**
     * Get the topics.
     *
     * @return the topics.
     */
    public Set<String> getTopics() {
        return topics;
    }

    /**
     * Get the properties.
     *
     * @return the properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Get the starting offsets initializer for the cluster.
     *
     * @return the starting offsets initializer, or null to use the source default.
     */
    @Nullable
    public OffsetsInitializer getStartingOffsetsInitializer() {
        return startingOffsetsInitializer;
    }

    /**
     * Get the stopping offsets initializer for the cluster.
     *
     * @return the stopping offsets initializer, or null to use the source default.
     */
    @Nullable
    public OffsetsInitializer getStoppingOffsetsInitializer() {
        return stoppingOffsetsInitializer;
    }

    @Override
    public String toString() {
        return "ClusterMetadata{"
                + "topics="
                + topics
                + ", properties="
                + properties
                + ", startingOffsetsInitializer="
                + startingOffsetsInitializer
                + ", stoppingOffsetsInitializer="
                + stoppingOffsetsInitializer
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
        ClusterMetadata that = (ClusterMetadata) o;
        return Objects.equals(topics, that.topics)
                && Objects.equals(properties, that.properties)
                && Objects.equals(startingOffsetsInitializer, that.startingOffsetsInitializer)
                && Objects.equals(stoppingOffsetsInitializer, that.stoppingOffsetsInitializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topics, properties, startingOffsetsInitializer, stoppingOffsetsInitializer);
    }
}
