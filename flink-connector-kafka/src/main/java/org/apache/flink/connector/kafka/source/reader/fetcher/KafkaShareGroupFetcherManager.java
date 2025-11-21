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

package org.apache.flink.connector.kafka.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaShareGroupSplitReader;
import org.apache.flink.connector.kafka.source.split.ShareGroupSubscriptionState;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fetcher manager for Kafka share group sources with ShareConsumer exposure.
 *
 * <h2>Key Responsibilities</h2>
 *
 * <ul>
 *   <li>Creates and manages {@link KafkaShareGroupSplitReader} instances
 *   <li>Exposes {@link ShareConsumer} to {@link
 *       org.apache.flink.connector.kafka.source.reader.KafkaShareGroupSourceReader}
 *   <li>Provides single-threaded fetch coordination for share group consumption
 *   <li>Integrates with Flink's unified source interface
 * </ul>
 *
 * <h2>ShareConsumer Access Pattern</h2>
 *
 * <p>The ShareConsumer is NOT thread-safe. This manager ensures thread-safe access by:
 *
 * <ol>
 *   <li>Running split reader in dedicated fetcher thread
 *   <li>Storing ShareConsumer reference obtained from split reader
 *   <li>Providing {@link #getShareConsumer()} method for SourceReader acknowledgment
 *   <li>Ensuring all ShareConsumer operations happen from correct thread context
 * </ol>
 *
 * <h2>Architecture Change from Traditional Implementation</h2>
 *
 * <table border="1">
 * <tr>
 *   <th>Aspect</th>
 *   <th>Traditional</th>
 *   <th>Share Group</th>
 * </tr>
 * <tr>
 *   <td>Split Type</td>
 *   <td>KafkaPartitionSplit</td>
 *   <td>ShareGroupSubscriptionState</td>
 * </tr>
 * <tr>
 *   <td>Split Reader</td>
 *   <td>Multiple partition readers</td>
 *   <td>Single subscription reader</td>
 * </tr>
 * <tr>
 *   <td>Consumer Access</td>
 *   <td>Not exposed (offset-based)</td>
 *   <td>Exposed for acknowledgment</td>
 * </tr>
 * <tr>
 *   <td>State Management</td>
 *   <td>Partition offsets</td>
 *   <td>Subscription + metadata buffer</td>
 * </tr>
 * </table>
 *
 * @see KafkaShareGroupSplitReader
 * @see ShareGroupSubscriptionState
 */
@Internal
public class KafkaShareGroupFetcherManager
        extends SingleThreadFetcherManager<
                ConsumerRecord<byte[], byte[]>, ShareGroupSubscriptionState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupFetcherManager.class);

    /**
     * Static registry to store split reader references across fetcher manager instances. This
     * registry enables ShareConsumer access without violating Java constructor initialization
     * rules.
     *
     * <p>Pattern: Since we must pass a supplier to super() before initializing instance fields, we
     * use a static map where the supplier can register the created reader. The instance then
     * retrieves it using its unique ID.
     */
    private static final ConcurrentHashMap<Long, KafkaShareGroupSplitReader> READER_REGISTRY =
            new ConcurrentHashMap<>();

    /** Generator for unique instance IDs */
    private static final AtomicLong INSTANCE_ID_GENERATOR = new AtomicLong(0);

    /** Unique identifier for this fetcher manager instance */
    private final long instanceId;

    private final Properties consumerProperties;
    private final SourceReaderContext context;
    private final KafkaShareGroupSourceMetrics metrics;

    /**
     * Creates a new fetcher manager for Kafka share group sources.
     *
     * @param consumerProperties Kafka consumer properties configured for share groups
     * @param context the source reader context
     * @param metrics metrics collector for share group operations (can be null)
     */
    public KafkaShareGroupFetcherManager(
            Properties consumerProperties,
            SourceReaderContext context,
            @Nullable KafkaShareGroupSourceMetrics metrics) {

        // Generate unique instance ID BEFORE super() call (allowed - just assignment)
        this(INSTANCE_ID_GENERATOR.getAndIncrement(), consumerProperties, context, metrics);
    }

    /**
     * Private constructor that accepts pre-generated instance ID. This allows us to generate the ID
     * before super() while still passing it to the supplier.
     */
    private KafkaShareGroupFetcherManager(
            long instanceId,
            Properties consumerProperties,
            SourceReaderContext context,
            @Nullable KafkaShareGroupSourceMetrics metrics) {

        super(
                createSplitReaderSupplier(instanceId, consumerProperties, context, metrics),
                new org.apache.flink.configuration.Configuration());

        // Initialize instance fields AFTER super() call
        this.instanceId = instanceId;
        this.consumerProperties = consumerProperties;
        this.context = context;
        this.metrics = metrics;

        LOG.info(
                "Created KafkaShareGroupFetcherManager (instance {}) for subtask {}",
                instanceId,
                context.getIndexOfSubtask());
    }

    /**
     * Creates a split reader supplier that registers the reader in the static registry. This static
     * method creates a supplier that can be passed to super() while ensuring the created reader is
     * accessible via the registry using the instance ID.
     *
     * @param instanceId unique identifier for this fetcher manager instance
     * @param props consumer properties
     * @param context source reader context
     * @param metrics metrics collector (can be null)
     * @return supplier that creates and registers split readers
     */
    private static java.util.function.Supplier<
                    org.apache.flink.connector.base.source.reader.splitreader.SplitReader<
                            org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>,
                            ShareGroupSubscriptionState>>
            createSplitReaderSupplier(
                    final long instanceId,
                    final Properties props,
                    final SourceReaderContext context,
                    @Nullable final KafkaShareGroupSourceMetrics metrics) {

        return () -> {
            // Create the split reader
            KafkaShareGroupSplitReader reader =
                    new KafkaShareGroupSplitReader(props, context, metrics);

            // Register in static map for later retrieval
            READER_REGISTRY.put(instanceId, reader);

            LOG.debug("Registered split reader for instance {} in static registry", instanceId);

            return reader;
        };
    }

    // ===========================================================================================
    // ShareConsumer Access for Acknowledgment
    // ===========================================================================================

    /**
     * Gets the ShareConsumer from the split reader for acknowledgment operations.
     *
     * <p>This method is called by {@link
     * org.apache.flink.connector.kafka.source.reader.KafkaShareGroupSourceReader} to access the
     * ShareConsumer for calling {@code acknowledge()} and {@code commitSync()} during checkpoint
     * completion.
     *
     * <p><b>Implementation:</b> Retrieves the split reader from the static registry using this
     * instance's unique ID, then calls {@code getShareConsumer()} on the reader.
     *
     * <p><b>Thread Safety:</b> The ShareConsumer itself is NOT thread-safe. However, this method
     * can be called safely from any thread. The actual ShareConsumer operations must be performed
     * from the fetcher thread context.
     *
     * @return the ShareConsumer instance, or null if reader not yet created
     */
    @Nullable
    public ShareConsumer<byte[], byte[]> getShareConsumer() {
        KafkaShareGroupSplitReader reader = READER_REGISTRY.get(instanceId);

        if (reader == null) {
            LOG.debug(
                    "Split reader not yet created for instance {} - ShareConsumer not available",
                    instanceId);
            return null;
        }

        ShareConsumer<byte[], byte[]> shareConsumer = reader.getShareConsumer();

        if (shareConsumer == null) {
            LOG.warn(
                    "ShareConsumer is null for instance {} - this should not happen after reader creation",
                    instanceId);
        }

        return shareConsumer;
    }

    // ===========================================================================================
    // Configuration Access
    // ===========================================================================================

    /**
     * Gets the consumer properties used by this fetcher manager.
     *
     * @return a copy of the consumer properties
     */
    public Properties getConsumerProperties() {
        return new Properties(consumerProperties);
    }

    /**
     * Gets the share group metrics collector.
     *
     * @return the metrics collector, or null if not configured
     */
    @Nullable
    public KafkaShareGroupSourceMetrics getMetrics() {
        return metrics;
    }

    /**
     * Gets the source reader context.
     *
     * @return the source reader context
     */
    public SourceReaderContext getContext() {
        return context;
    }

    // ===========================================================================================
    // Lifecycle Management
    // ===========================================================================================

    /**
     * Closes the fetcher manager and cleans up resources.
     *
     * <p>This method removes the split reader from the static registry to prevent memory leaks. The
     * static registry is necessary for ShareConsumer access, but entries must be cleaned up when
     * instances are destroyed.
     *
     * @param timeoutMs timeout in milliseconds for closing fetchers
     */
    @Override
    public void close(long timeoutMs) {
        try {
            // Remove from registry to prevent memory leak
            KafkaShareGroupSplitReader removed = READER_REGISTRY.remove(instanceId);

            if (removed != null) {
                LOG.debug("Removed split reader for instance {} from static registry", instanceId);
            } else {
                LOG.warn(
                        "Split reader for instance {} was not found in registry during close",
                        instanceId);
            }

            // Call parent close to shut down fetchers
            super.close(timeoutMs);

            LOG.info("Closed KafkaShareGroupFetcherManager (instance {})", instanceId);

        } catch (Exception e) {
            LOG.error("Error closing KafkaShareGroupFetcherManager (instance {})", instanceId, e);
            throw new RuntimeException("Failed to close fetcher manager", e);
        }
    }
}
