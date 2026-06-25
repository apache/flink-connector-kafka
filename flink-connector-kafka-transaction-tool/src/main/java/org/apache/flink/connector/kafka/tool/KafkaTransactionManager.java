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

package org.apache.flink.connector.kafka.tool;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Manager to handle lingering Kafka transactions.
 *
 * <p>Allows operators to manually intervene when a Flink job is no longer running and has left
 * transactions in an {@code ONGOING} state, blocking downstream consumers (LSO).
 *
 * <p>It supports two operations:
 *
 * <ul>
 *   <li><b>Abort:</b> Connects with the same {@code transactional.id} to "fence" the previous
 *       producer, forcing the broker to abort the open transaction.
 *   <li><b>Commit:</b> Resumes the specific transaction using Flink's internal producer logic and
 *       commits it. <b>WARNING:</b> This requires the exact {@code producerId} and {@code epoch}
 *       from the Flink checkpoint state. Committing incorrectly can lead to data loss or
 *       duplication.
 * </ul>
 */
@Internal
public final class KafkaTransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransactionManager.class);
    private static final String TIMEOUT_MS = "10000";

    /**
     * Aborts an ongoing transaction for the given transactional ID.
     *
     * <p>This method utilizes the Kafka protocol's "fencing" mechanism. By initializing a new
     * producer with the same {@code transactional.id}, the Kafka Broker will increment the epoch
     * and automatically abort any lingering transactions from previous epochs.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092").
     * @param transactionalId The transactional ID of the transaction to abort.
     * @throws RuntimeException if the abort operation fails.
     */
    public void abortTransaction(String bootstrapServers, String transactionalId) {
        validateInput(bootstrapServers, transactionalId);
        LOG.info(
                "Attempting to ABORT transaction: '{}' on servers: '{}'",
                transactionalId,
                bootstrapServers);

        Properties props = createProducerProperties(bootstrapServers, transactionalId);

        // Use standard KafkaProducer. The side-effect of initTransactions() performs the abort.
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();
            LOG.info("Successfully fenced and aborted transaction: {}", transactionalId);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to abort transaction '%s'", transactionalId), e);
        }
    }

    /**
     * Commits a transaction for the given transactional ID, producer ID, and epoch.
     *
     * <p><b>Note:</b> This method uses Flink's internal {@link FlinkKafkaInternalProducer}, which
     * internally uses reflection to resume the transaction state. It is critical that the {@code
     * producerId} and {@code epoch} match exactly what was recorded in the Flink checkpoint,
     * otherwise, the broker will reject the commit or fence this attempt.
     *
     * @param bootstrapServers Kafka bootstrap servers (e.g., "localhost:9092").
     * @param transactionalId The transactional ID of the transaction to commit.
     * @param producerId The internal Kafka Producer ID (PID) obtained from Flink state/logs.
     * @param epoch The internal Kafka Producer Epoch obtained from Flink state/logs.
     * @throws RuntimeException if the commit operation fails.
     */
    public void commitTransaction(
            String bootstrapServers, String transactionalId, long producerId, short epoch) {
        validateInput(bootstrapServers, transactionalId);
        Preconditions.checkArgument(producerId >= 0, "Producer ID must be non-negative");
        Preconditions.checkArgument(epoch >= 0, "Epoch must be non-negative");

        LOG.info(
                "Attempting to RESUME and COMMIT transaction: '{}' (PID: {}, Epoch: {}) on servers: '{}'",
                transactionalId,
                producerId,
                epoch,
                bootstrapServers);

        Properties props = createProducerProperties(bootstrapServers, transactionalId);

        try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(props, transactionalId)) {

            producer.resumeTransaction(producerId, epoch);
            producer.commitTransaction();

            LOG.info("Successfully committed transaction: {}", transactionalId);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to commit transaction '%s'. Possible causes: incorrect ProducerId (%d) or Epoch (%d), transaction already aborted or timed out, or producer fenced by another instance. Check Kafka broker logs for details.",
                            transactionalId, producerId, epoch),
                    e);
        }
    }

    private Properties createProducerProperties(String bootstrapServers, String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, TIMEOUT_MS);
        // We use ByteArraySerializer as we are only managing transaction control messages,
        // not sending actual data records.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    private void validateInput(String bootstrapServers, String transactionalId) {
        Preconditions.checkNotNull(bootstrapServers, "Bootstrap servers cannot be null");
        Preconditions.checkArgument(
                !bootstrapServers.isEmpty(), "Bootstrap servers cannot be empty");

        Preconditions.checkNotNull(transactionalId, "Transactional ID cannot be null");
        Preconditions.checkArgument(!transactionalId.isEmpty(), "Transactional ID cannot be empty");
    }
}
