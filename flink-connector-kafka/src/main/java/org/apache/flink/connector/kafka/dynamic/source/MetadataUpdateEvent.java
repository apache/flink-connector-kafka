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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.reader.DynamicKafkaSourceReader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Supplies the current subscription and authoritative retention deadlines to {@link
 * DynamicKafkaSourceReader}. Readers reconcile their active sub-readers with the subscription and
 * retain removed-cluster offsets only while the coordinator grants retention.
 *
 * <p>Returning splits are assigned separately after the retained-offset checkpoint handoff. A
 * subsequent metadata update removes the former owners' retained copies.
 */
@Internal
public class MetadataUpdateEvent implements SourceEvent {
    private final Set<KafkaStream> kafkaStreams;
    private final Map<String, Long> retainedClusterDeadlines;

    public MetadataUpdateEvent(Set<KafkaStream> kafkaStreams) {
        this(kafkaStreams, Collections.emptyMap());
    }

    public MetadataUpdateEvent(
            Set<KafkaStream> kafkaStreams, Map<String, Long> retainedClusterDeadlines) {
        this.kafkaStreams = kafkaStreams;
        this.retainedClusterDeadlines =
                Collections.unmodifiableMap(new HashMap<>(retainedClusterDeadlines));
    }

    public Set<KafkaStream> getKafkaStreams() {
        return kafkaStreams;
    }

    /** Retention epochs still owned by the coordinator, including pending handoffs. */
    public Map<String, Long> getRetainedClusterDeadlines() {
        return retainedClusterDeadlines;
    }

    @Override
    public String toString() {
        return "MetadataUpdateEvent{"
                + "kafkaStreams="
                + kafkaStreams
                + ", retainedClusterDeadlines="
                + retainedClusterDeadlines
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
        MetadataUpdateEvent that = (MetadataUpdateEvent) o;
        return Objects.equals(kafkaStreams, that.kafkaStreams)
                && Objects.equals(retainedClusterDeadlines, that.retainedClusterDeadlines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafkaStreams, retainedClusterDeadlines);
    }
}
