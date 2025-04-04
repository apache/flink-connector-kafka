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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct {@link KafkaSink}.
 *
 * <p>The following example shows the minimum setup to create a KafkaSink that writes String values
 * to a Kafka topic.
 *
 * <pre>{@code
 * KafkaSink<String> sink = KafkaSink
 *     .<String>builder
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setRecordSerializer(MY_RECORD_SERIALIZER)
 *     .build();
 * }</pre>
 *
 * <p>One can also configure different {@link DeliveryGuarantee} by using {@link
 * #setDeliveryGuarantee(DeliveryGuarantee)} but keep in mind when using {@link
 * DeliveryGuarantee#EXACTLY_ONCE} one must set the transactionalIdPrefix {@link
 * #setTransactionalIdPrefix(String)}.
 *
 * @see KafkaSink for a more detailed explanation of the different guarantees.
 * @param <IN> type of the records written to Kafka
 */
@PublicEvolving
public class KafkaSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkBuilder.class);
    private static final Duration DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(1);
    private static final String[] warnKeys =
            new String[] {
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
            };
    private static final int MAXIMUM_PREFIX_BYTES = 64000;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private String transactionalIdPrefix;

    private final Properties kafkaProducerConfig;
    private KafkaRecordSerializationSchema<IN> recordSerializer;
    private TransactionNamingStrategy transactionNamingStrategy = TransactionNamingStrategy.DEFAULT;

    KafkaSinkBuilder() {
        kafkaProducerConfig = new Properties();
        kafkaProducerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerConfig.put(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                (int) DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMillis());
    }

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    /**
     * Sets the configuration which used to instantiate all used {@link
     * org.apache.kafka.clients.producer.KafkaProducer}.
     *
     * @param props
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setKafkaProducerConfig(Properties props) {
        checkNotNull(props);
        Arrays.stream(warnKeys)
                .filter(props::containsKey)
                .forEach(k -> LOG.warn("Overwriting the '{}' is not recommended", k));

        kafkaProducerConfig.putAll(props);
        return this;
    }

    public KafkaSinkBuilder<IN> setProperty(String key, String value) {
        checkNotNull(key);
        Arrays.stream(warnKeys)
                .filter(key::equals)
                .forEach(k -> LOG.warn("Overwriting the '{}' is not recommended", k));

        kafkaProducerConfig.setProperty(key, value);
        return this;
    }

    /**
     * Sets the {@link TransactionNamingStrategy} that is used to name the transactions.
     *
     * <p>By default {@link TransactionNamingStrategy#DEFAULT} is used. It's recommended to change
     * the strategy only if specific issues occur.
     */
    public KafkaSinkBuilder<IN> setTransactionNamingStrategy(
            TransactionNamingStrategy transactionNamingStrategy) {
        this.transactionNamingStrategy =
                checkNotNull(
                        transactionNamingStrategy, "transactionNamingStrategy must not be null");
        return this;
    }

    /**
     * Sets the {@link KafkaRecordSerializationSchema} that transforms incoming records to {@link
     * org.apache.kafka.clients.producer.ProducerRecord}s.
     *
     * @param recordSerializer
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setRecordSerializer(
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        ClosureCleaner.clean(
                this.recordSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Sets the prefix for all created transactionalIds if {@link DeliveryGuarantee#EXACTLY_ONCE} is
     * configured.
     *
     * <p>It is mandatory to always set this value with {@link DeliveryGuarantee#EXACTLY_ONCE} to
     * prevent corrupted transactions if multiple jobs using the KafkaSink run against the same
     * Kafka Cluster. The default prefix is {@link #transactionalIdPrefix}.
     *
     * <p>The size of the prefix is capped by {@link #MAXIMUM_PREFIX_BYTES} formatted with UTF-8.
     *
     * <p>It is important to keep the prefix stable across application restarts. If the prefix
     * changes it might happen that lingering transactions are not correctly aborted and newly
     * written messages are not immediately consumable until the transactions timeout.
     *
     * @param transactionalIdPrefix
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        checkState(
                transactionalIdPrefix.getBytes(StandardCharsets.UTF_8).length
                        <= MAXIMUM_PREFIX_BYTES,
                "The configured prefix is too long and the resulting transactionalIdPrefix might exceed Kafka's transactionalIdPrefix size.");
        return this;
    }

    /**
     * Sets the Kafka bootstrap servers.
     *
     * @param bootstrapServers a comma separated list of valid URIs to reach the Kafka broker
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setBootstrapServers(String bootstrapServers) {
        return setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    private void sanityCheck() {
        checkNotNull(
                kafkaProducerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                "bootstrapServers");
        checkNotNull(recordSerializer, "recordSerializer");
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            checkState(
                    transactionalIdPrefix != null,
                    "EXACTLY_ONCE delivery guarantee requires a transactionalIdPrefix to be set to provide unique transaction names across multiple KafkaSinks writing to the same Kafka cluster.");
            if (transactionNamingStrategy.getImpl().requiresKnownTopics()) {
                checkState(
                        recordSerializer instanceof KafkaDatasetFacetProvider,
                        "For %s naming strategy, the recordSerializer needs to expose the target topics though implementing KafkaDatasetFacetProvider.",
                        transactionNamingStrategy);
            }
        }
    }

    /**
     * Constructs the {@link KafkaSink} with the configured properties.
     *
     * @return {@link KafkaSink}
     */
    public KafkaSink<IN> build() {
        sanityCheck();
        return new KafkaSink<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                recordSerializer,
                transactionNamingStrategy);
    }
}
