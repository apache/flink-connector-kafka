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

import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.ReadableBackchannel;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Shared broker and checkpoint-harness setup for Kafka transaction recovery tests. */
final class KafkaRecoveryTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRecoveryTestUtils.class);

    private KafkaRecoveryTestUtils() {}

    static Properties getProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        return properties;
    }

    static void advanceEpoch(FlinkKafkaInternalProducer<?, ?> producer, int targetEpoch) {
        assertThat(producer.getEpoch()).isZero();
        long producerId = producer.getProducerId();
        // Advance the real coordinator state before writing any records. Setting only the
        // client's epoch via reflection would not reproduce broker-side producer ID rotation.
        for (int epoch = 1; epoch <= targetEpoch; epoch++) {
            producer.setTransactionId(producer.getTransactionalId());
            producer.initTransactions();
            if (epoch % 4096 == 0) {
                LOG.info("Advanced producer epoch to {} of {}", epoch, targetEpoch);
            }
        }
        assertThat(producer.getProducerId()).isEqualTo(producerId);
        assertThat(producer.getEpoch()).isEqualTo((short) targetEpoch);
    }

    static OneInputStreamOperatorTestHarness<
                    CommittableMessage<KafkaCommittable>, CommittableMessage<KafkaCommittable>>
            createHarness(KafkaSink<byte[]> sink) throws Exception {
        OneInputStreamOperatorTestHarness<
                        CommittableMessage<KafkaCommittable>, CommittableMessage<KafkaCommittable>>
                harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CommitterOperatorFactory<>(sink, false, true));
        // Serialize emitted committables with the sink's serializer, which omits live producers.
        harness.setup(
                CommittableMessageTypeInfo.of(KafkaCommittableSerializer::new)
                        .createSerializer(harness.getExecutionConfig().getSerializerConfig()));
        return harness;
    }

    static void recoverCheckpoint(
            KafkaSink<byte[]> sink,
            OperatorSubtaskState checkpoint,
            long checkpointId,
            String prefix,
            TransactionFinished expectedOutcome)
            throws Exception {
        try (ReadableBackchannel<TransactionFinished> backchannel =
                        BackchannelFactory.getInstance().getReadableBackchannel(0, 0, prefix);
                OneInputStreamOperatorTestHarness<
                                CommittableMessage<KafkaCommittable>,
                                CommittableMessage<KafkaCommittable>>
                        recovered = createHarness(sink)) {
            recovered.setRestoredCheckpointId(checkpointId);
            recovered.initializeState(checkpoint);
            recovered.open();
            // The caller must distinguish successful replay from a known failed commit.
            assertThat(backchannel.poll()).isEqualTo(expectedOutcome);
            assertThat(backchannel.poll()).isNull();
        }
    }
}
