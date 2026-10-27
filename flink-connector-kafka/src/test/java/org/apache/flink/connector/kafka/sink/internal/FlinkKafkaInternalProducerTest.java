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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests restoring a producer's transaction state without connecting to a broker. */
class FlinkKafkaInternalProducerTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testResumeTransactionUsesExplicitProtocol(boolean useTransactionV2)
            throws ReflectiveOperationException {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(properties, "resume-protocol-test")) {
            final Field managerField = KafkaProducer.class.getDeclaredField("transactionManager");
            managerField.setAccessible(true);
            final TransactionManager manager = (TransactionManager) managerField.get(producer);
            assertThat(manager.isTransactionV2Enabled()).isFalse();

            producer.resumeTransaction(100, (short) 7, useTransactionV2);

            assertThat(manager.isTransactionV2Enabled()).isEqualTo(useTransactionV2);
            assertThat(producer.getProducerId()).isEqualTo(100);
            assertThat(producer.getEpoch()).isEqualTo((short) 7);
            assertThat(producer.isPrecommitted()).isTrue();
        }
    }
}
