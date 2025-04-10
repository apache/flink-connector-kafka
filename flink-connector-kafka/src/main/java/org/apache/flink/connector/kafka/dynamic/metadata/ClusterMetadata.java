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
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * {@link ClusterMetadata} provides readers information about a cluster on what topics to read and
 * how to connect to a cluster.
 */
@Experimental
public class ClusterMetadata implements Serializable {
    private final Set<String> topics;
    private final Properties properties;

    /**
     * Constructs the {@link ClusterMetadata} with the required properties.
     *
     * @param topics the topics belonging to a cluster.
     * @param properties the properties to access a cluster.
     */
    public ClusterMetadata(Set<String> topics, Properties properties) {
        this.topics = topics;
        this.properties = properties;
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

    @Override
    public String toString() {
        return "ClusterMetadata{" + "topics=" + topics + ", properties=" + properties + '}';
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
        return Objects.equals(topics, that.topics) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topics, properties);
    }
}
