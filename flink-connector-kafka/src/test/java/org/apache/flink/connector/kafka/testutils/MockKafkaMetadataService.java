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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** A mock in-memory implementation of {@link KafkaMetadataService}. */
public class MockKafkaMetadataService implements KafkaMetadataService {

    private Set<KafkaStream> kafkaStreams;
    private Set<String> kafkaClusterIds;
    private boolean throwException = false;

    public MockKafkaMetadataService(boolean throwException) {
        this.throwException = throwException;
    }

    public MockKafkaMetadataService(Set<KafkaStream> kafkaStreams) {
        setKafkaStreams(kafkaStreams);
    }

    public void setKafkaStreams(Set<KafkaStream> kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
        this.kafkaClusterIds =
                kafkaStreams.stream()
                        .flatMap(
                                kafkaStream ->
                                        kafkaStream.getClusterMetadataMap().keySet().stream())
                        .collect(Collectors.toSet());
    }

    public void setThrowException(boolean throwException) {
        this.throwException = throwException;
    }

    private void checkAndThrowException() {
        if (throwException) {
            throw new RuntimeException("Mock exception");
        }
    }

    @Override
    public Set<KafkaStream> getAllStreams() {
        checkAndThrowException();
        return kafkaStreams;
    }

    @Override
    public Set<KafkaStream> getPatternStreams(Pattern streamPattern) {
        checkAndThrowException();
        ImmutableSet.Builder<KafkaStream> builder = ImmutableSet.builder();
        for (KafkaStream kafkaStream : kafkaStreams) {
            String streamId = kafkaStream.getStreamId();
            if (streamPattern.matcher(streamId).find()) {
                builder.add(kafkaStream);
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        checkAndThrowException();
        ImmutableMap.Builder<String, KafkaStream> builder = ImmutableMap.builder();
        for (KafkaStream stream : getAllStreams()) {
            if (streamIds.contains(stream.getStreamId())) {
                builder.put(stream.getStreamId(), stream);
            }
        }

        return builder.build();
    }

    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        checkAndThrowException();
        return kafkaClusterIds.contains(kafkaClusterId);
    }

    @Override
    public void close() throws Exception {}
}
