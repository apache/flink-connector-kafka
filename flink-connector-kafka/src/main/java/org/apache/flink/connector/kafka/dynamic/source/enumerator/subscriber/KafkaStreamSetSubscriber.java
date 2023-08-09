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

package org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;

import java.util.HashSet;
import java.util.Set;

/** Subscribe to streams based on the set of ids. */
@Internal
public class KafkaStreamSetSubscriber implements KafkaStreamSubscriber {

    private final Set<String> streamIds;

    public KafkaStreamSetSubscriber(Set<String> streamIds) {
        this.streamIds = streamIds;
    }

    @Override
    public Set<KafkaStream> getSubscribedStreams(KafkaMetadataService kafkaMetadataService) {
        return new HashSet<>(kafkaMetadataService.describeStreams(streamIds).values());
    }
}
