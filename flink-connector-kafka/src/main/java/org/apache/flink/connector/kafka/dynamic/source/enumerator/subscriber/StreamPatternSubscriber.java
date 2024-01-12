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

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.regex.Pattern;

/** To subscribe to streams based on a pattern. */
@Internal
public class StreamPatternSubscriber implements KafkaStreamSubscriber {

    private final Pattern streamPattern;

    public StreamPatternSubscriber(Pattern streamPattern) {
        this.streamPattern = streamPattern;
    }

    @Override
    public Set<KafkaStream> getSubscribedStreams(KafkaMetadataService kafkaMetadataService) {
        Set<KafkaStream> allStreams = kafkaMetadataService.getAllStreams();
        ImmutableSet.Builder<KafkaStream> builder = ImmutableSet.builder();
        for (KafkaStream kafkaStream : allStreams) {
            String streamId = kafkaStream.getStreamId();
            if (streamPattern.matcher(streamId).find()) {
                builder.add(kafkaStream);
            }
        }

        return builder.build();
    }
}
