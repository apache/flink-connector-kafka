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

package org.apache.flink.connector.kafka.share;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Builder for {@link KafkaShareAckSink}. */
@PublicEvolving
public class KafkaShareAckSinkBuilder {

    private static final Duration DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(1);
    private static final int MAXIMUM_PREFIX_BYTES = 64000;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    private final Properties kafkaProducerConfig = new Properties();
    private String shareExactlyOnceId;
    private boolean transactionTimeoutExplicitlyConfigured;

    KafkaShareAckSinkBuilder() {
        kafkaProducerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerConfig.put(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                (int) DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMillis());
    }

    public KafkaShareAckSinkBuilder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    public KafkaShareAckSinkBuilder setKafkaProducerConfig(Properties props) {
        Properties producerConfig = copyOf(props);
        Object shareExactlyOnceId =
                producerConfig.remove(KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG);
        if (shareExactlyOnceId != null) {
            setShareExactlyOnceId(shareExactlyOnceId.toString());
        }
        kafkaProducerConfig.putAll(producerConfig);
        transactionTimeoutExplicitlyConfigured |=
                producerConfig.containsKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        return this;
    }

    public KafkaShareAckSinkBuilder setProperty(String key, String value) {
        checkNotNull(key, "key");
        checkNotNull(value, "value");
        if (KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG.equals(key)) {
            return setShareExactlyOnceId(value);
        }
        kafkaProducerConfig.setProperty(key, value);
        transactionTimeoutExplicitlyConfigured |=
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG.equals(key);
        return this;
    }

    public KafkaShareAckSinkBuilder setBootstrapServers(String bootstrapServers) {
        return setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public KafkaShareAckSinkBuilder setShareExactlyOnceId(String shareExactlyOnceId) {
        this.shareExactlyOnceId = checkNotNull(shareExactlyOnceId, "shareExactlyOnceId");
        return this;
    }

    public KafkaShareAckSink build() {
        sanityCheck();
        Properties finalKafkaProducerConfig = copyOf(kafkaProducerConfig);
        if (isTwoPhaseCommitEnabled(finalKafkaProducerConfig)) {
            finalKafkaProducerConfig.remove(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        }
        return new KafkaShareAckSink(
                deliveryGuarantee, finalKafkaProducerConfig, shareExactlyOnceId);
    }

    private void sanityCheck() {
        checkNotNull(
                kafkaProducerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                "bootstrapServers");
        checkNotNull(shareExactlyOnceId, KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG);
        checkState(
                deliveryGuarantee != DeliveryGuarantee.NONE,
                "%s does not support %s because share acknowledgements must wait for checkpoint completion.",
                KafkaShareAckSink.class.getSimpleName(),
                DeliveryGuarantee.NONE);
        checkState(
                shareExactlyOnceId.getBytes(StandardCharsets.UTF_8).length <= MAXIMUM_PREFIX_BYTES,
                "%s is too long and may exceed Kafka's transactional id size limit.",
                KafkaShareAckSink.SHARE_EXACTLY_ONCE_ID_CONFIG);
        checkState(
                !isTwoPhaseCommitEnabled(kafkaProducerConfig) || !transactionTimeoutExplicitlyConfigured,
                "%s cannot be configured when %s is set to true.",
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG);
    }

    private static boolean isTwoPhaseCommitEnabled(Properties properties) {
        Object value = properties.get(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return value != null && Boolean.parseBoolean(value.toString());
    }

    private static Properties copyOf(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(Objects.requireNonNull(properties, "properties"));
        return copy;
    }
}
