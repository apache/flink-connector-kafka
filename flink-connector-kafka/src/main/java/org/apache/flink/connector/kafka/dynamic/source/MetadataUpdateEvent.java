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

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Set;

/**
 * Signals {@link DynamicKafkaSourceReader} to stop their underlying readers. The restart process is
 * as follows:
 *
 * <p>1. Detect metadata change in enumerator 2. Stop sub enumerators and don't snapshot state 3.
 * Send this event to all readers 4. Stop sub readers and snapshot state (offsets) 5. Start new sub
 * enumerators with clean state and do total split reassignment to readers 6. Readers obtain splits,
 * starting sub readers dynamically, and do reconciliation of starting offsets with the cached
 * offsets
 *
 * <p>We don't snapshot enumerator state because we want to reassign previously assigned splits.
 * After restart, readers need to reinitialize the sub readers by using the received splits.
 */
@Internal
public class MetadataUpdateEvent implements SourceEvent {
    private final Set<KafkaStream> kafkaStreams;

    public MetadataUpdateEvent(Set<KafkaStream> kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public Set<KafkaStream> getKafkaStreams() {
        return kafkaStreams;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("kafkaStreams", kafkaStreams).toString();
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
        return Objects.equals(kafkaStreams, that.kafkaStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafkaStreams);
    }
}
