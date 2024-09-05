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

package org.apache.flink.connector.kafka.lineage.facets;

import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Facet containing topic pattern. Can be used as an intermediate step for evaluating topics
 * involved in data processing.
 */
public class KafkaTopicPatternFacet implements LineageDatasetFacet {

    public static final String TOPIC_PATTERN_FACET_NAME = "topicPattern";
    public Pattern pattern;

    public KafkaTopicPatternFacet(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public String name() {
        return TOPIC_PATTERN_FACET_NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaTopicPatternFacet that = (KafkaTopicPatternFacet) o;
        return Objects.equals(pattern.pattern(), that.pattern.pattern());
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern.pattern());
    }
}
