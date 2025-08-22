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
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kafka.source.metrics.KafkaShareGroupSourceMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaShareGroupSplitReader;
import org.apache.flink.connector.kafka.source.split.KafkaShareGroupSplit;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.function.Supplier;

/**
 * Fetcher manager specifically designed for Kafka share group sources using KafkaShareConsumer.
 * 
 * <p>This fetcher manager creates and manages {@link KafkaShareConsumerSplitReader} instances
 * that use the Kafka 4.1.0+ KafkaShareConsumer API for true share group semantics.
 * 
 * <p>Unlike traditional Kafka sources that use partition-based assignment, this fetcher
 * manager coordinates share group consumers that receive messages distributed at the
 * message level by Kafka's share group coordinator.
 * 
 * <p><strong>Key features:</strong>
 * <ul>
 *   <li>Single-threaded fetcher optimized for share group message consumption</li>
 *   <li>Integration with share group metrics collection</li>
 *   <li>Automatic handling of share group consumer lifecycle</li>
 *   <li>Compatible with Flink's unified source interface</li>
 * </ul>
 */
@Internal
public class KafkaShareGroupFetcherManager extends SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaShareGroupSplit> {
    
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
            KafkaShareGroupSourceMetrics metrics) {
        
        super(
            createSplitReaderSupplier(consumerProperties, context, metrics),
            new org.apache.flink.configuration.Configuration()
        );
        this.consumerProperties = consumerProperties;
        this.context = context;
        this.metrics = metrics;
    }
    
    /**
     * Creates a supplier for share consumer split readers.
     * 
     * @param consumerProperties consumer properties configured for share groups
     * @param context source reader context
     * @param metrics metrics collector (can be null)
     * @return supplier that creates KafkaShareGroupSplitReader instances
     */
    public static Supplier<SplitReader<ConsumerRecord<byte[], byte[]>, KafkaShareGroupSplit>> 
            createSplitReaderSupplier(
                    Properties consumerProperties,
                    SourceReaderContext context,
                    KafkaShareGroupSourceMetrics metrics) {
        
        return () -> new KafkaShareGroupSplitReader(consumerProperties, context, metrics);
    }
    
    /**
     * Gets the consumer properties used by this fetcher manager.
     */
    public Properties getConsumerProperties() {
        return new Properties(consumerProperties);
    }
    
    /**
     * Gets the share group metrics collector.
     */
    public KafkaShareGroupSourceMetrics getMetrics() {
        return metrics;
    }
}