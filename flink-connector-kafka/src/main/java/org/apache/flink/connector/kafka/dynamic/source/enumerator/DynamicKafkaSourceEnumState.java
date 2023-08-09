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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The enumerator state keeps track of the state of the sub enumerators assigned splits and
 * metadata.
 */
@Internal
public class DynamicKafkaSourceEnumState {
    private final Set<KafkaStream> kafkaStreams;
    private final Map<String, KafkaSourceEnumState> clusterEnumeratorStates;

    public DynamicKafkaSourceEnumState() {
        this.kafkaStreams = new HashSet<>();
        this.clusterEnumeratorStates = new HashMap<>();
    }

    public DynamicKafkaSourceEnumState(
            Set<KafkaStream> kafkaStreams,
            Map<String, KafkaSourceEnumState> clusterEnumeratorStates) {
        this.kafkaStreams = kafkaStreams;
        this.clusterEnumeratorStates = clusterEnumeratorStates;
    }

    public Set<KafkaStream> getKafkaStreams() {
        return kafkaStreams;
    }

    public Map<String, KafkaSourceEnumState> getClusterEnumeratorStates() {
        return clusterEnumeratorStates;
    }
}
