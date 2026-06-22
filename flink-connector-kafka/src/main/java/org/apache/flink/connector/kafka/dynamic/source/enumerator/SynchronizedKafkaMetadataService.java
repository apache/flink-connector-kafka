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
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Serializes Kafka metadata service calls across dynamic Kafka source worker threads.
 *
 * <p>Dynamic stream metadata refresh and stale-cluster error handling run on separate workers.
 * Metadata service implementations predate that split and are not required to be thread-safe.
 */
@Internal
final class SynchronizedKafkaMetadataService implements KafkaMetadataService {
    private final KafkaMetadataService delegate;

    SynchronizedKafkaMetadataService(KafkaMetadataService delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized Set<KafkaStream> getAllStreams() {
        return delegate.getAllStreams();
    }

    @Override
    public synchronized Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        return delegate.describeStreams(streamIds);
    }

    @Override
    public synchronized boolean isClusterActive(String kafkaClusterId) {
        return delegate.isClusterActive(kafkaClusterId);
    }

    @Override
    public void close() throws Exception {
        // Closing must be able to unblock an in-flight metadata call during source shutdown.
        delegate.close();
    }
}
