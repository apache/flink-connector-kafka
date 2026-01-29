/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;

import java.util.Objects;
import java.util.Properties;

/** Default implementation of {@link KafkaDatasetFacet}. */
@PublicEvolving
public class DefaultKafkaDatasetFacet implements KafkaDatasetFacet {

    public static final String KAFKA_FACET_NAME = "kafka";

    private Properties properties;

    private final KafkaDatasetIdentifier topicIdentifier;

    public DefaultKafkaDatasetFacet(KafkaDatasetIdentifier topicIdentifier, Properties properties) {
        this(topicIdentifier);

        this.properties = new Properties();
        KafkaPropertiesUtil.copyProperties(properties, this.properties);
    }

    public DefaultKafkaDatasetFacet(KafkaDatasetIdentifier topicIdentifier) {
        this.topicIdentifier = topicIdentifier;
    }

    public void setProperties(Properties properties) {
        this.properties = new Properties();
        KafkaPropertiesUtil.copyProperties(properties, this.properties);
    }

    public Properties getProperties() {
        return properties;
    }

    public KafkaDatasetIdentifier getTopicIdentifier() {
        return topicIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultKafkaDatasetFacet that = (DefaultKafkaDatasetFacet) o;
        return Objects.equals(properties, that.properties)
                && Objects.equals(topicIdentifier, that.topicIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, topicIdentifier);
    }

    @Override
    public String name() {
        return KAFKA_FACET_NAME;
    }
}
