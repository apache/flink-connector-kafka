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

import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamicKafkaSourceBuilder}. */
class DynamicKafkaSourceBuilderTest {

    @Test
    void testAutoOffsetResetDefaultsToInitializerStrategy() throws Exception {
        assertThat(
                        extractProperties(baseBuilder().build())
                                .getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
                .isEqualTo("earliest");
    }

    @Test
    void testAutoOffsetResetUsesExplicitProperty() throws Exception {
        assertThat(
                        extractProperties(
                                        baseBuilder()
                                                .setProperty(
                                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                        "none")
                                                .build())
                                .getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
                .isEqualTo("none");
    }

    @Test
    void testAutoOffsetResetExplicitPropertyOverridesInitializerStrategy() throws Exception {
        assertThat(
                        extractProperties(
                                        baseBuilder()
                                                .setStartingOffsets(OffsetsInitializer.latest())
                                                .setProperty(
                                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                        "none")
                                                .build())
                                .getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
                .isEqualTo("none");
    }

    private DynamicKafkaSourceBuilder<Integer> baseBuilder() {
        return DynamicKafkaSource.<Integer>builder()
                .setStreamIds(Collections.singleton("stream-1"))
                .setKafkaMetadataService(NoOpKafkaMetadataService.INSTANCE)
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(IntegerDeserializer.class));
    }

    private static Properties extractProperties(DynamicKafkaSource<?> source) throws Exception {
        Field field = DynamicKafkaSource.class.getDeclaredField("properties");
        field.setAccessible(true);
        return (Properties) field.get(source);
    }

    private enum NoOpKafkaMetadataService implements KafkaMetadataService {
        INSTANCE;

        @Override
        public Set<KafkaStream> getAllStreams() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
            return Collections.emptyMap();
        }

        @Override
        public boolean isClusterActive(String kafkaClusterId) {
            return true;
        }

        @Override
        public void close() {}
    }
}
