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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.DefaultKafkaDatasetIdentifier;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;

/** mock recordSerializer for KafkaSink. */
class IntegerRecordSerializer
        implements KafkaRecordSerializationSchema<Integer>, KafkaDatasetFacetProvider {
    private final String topic;

    IntegerRecordSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Integer element, KafkaSinkContext context, Long timestamp) {
        if (element == null) {
            // in general, serializers should be allowed to skip invalid elements
            return null;
        }
        byte[] bytes = ByteBuffer.allocate(4).putInt(element).array();
        return new ProducerRecord<>(topic, bytes, bytes);
    }

    @Override
    public Optional<KafkaDatasetFacet> getKafkaDatasetFacet() {
        return Optional.of(
                new DefaultKafkaDatasetFacet(
                        DefaultKafkaDatasetIdentifier.ofTopics(
                                Collections.singletonList(KafkaWriterTestBase.topic))));
    }
}
