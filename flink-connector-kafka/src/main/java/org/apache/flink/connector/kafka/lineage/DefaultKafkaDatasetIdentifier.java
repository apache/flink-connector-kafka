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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Default implementation of {@link KafkaDatasetIdentifier}. */
@PublicEvolving
public class DefaultKafkaDatasetIdentifier implements KafkaDatasetIdentifier {

    @Nullable private final List<String> topics;
    @Nullable private final Pattern topicPattern;

    private DefaultKafkaDatasetIdentifier(
            @Nullable List<String> fixedTopics, @Nullable Pattern topicPattern) {
        this.topics = fixedTopics;
        this.topicPattern = topicPattern;
    }

    public static DefaultKafkaDatasetIdentifier ofPattern(Pattern pattern) {
        return new DefaultKafkaDatasetIdentifier(null, pattern);
    }

    public static DefaultKafkaDatasetIdentifier ofTopics(List<String> fixedTopics) {
        return new DefaultKafkaDatasetIdentifier(fixedTopics, null);
    }

    @Nullable
    public List<String> getTopics() {
        return topics;
    }

    @Nullable
    public Pattern getTopicPattern() {
        return topicPattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultKafkaDatasetIdentifier that = (DefaultKafkaDatasetIdentifier) o;
        return Objects.equals(topics, that.topics)
                && Objects.equals(topicPattern, that.topicPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topics, topicPattern);
    }
}
