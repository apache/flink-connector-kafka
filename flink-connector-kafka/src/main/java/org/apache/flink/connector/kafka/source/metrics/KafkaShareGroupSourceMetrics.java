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

package org.apache.flink.connector.kafka.source.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collector for Kafka share group sources.
 *
 * <p>This class provides specialized metrics for monitoring share group consumption
 * patterns, including message distribution statistics, share group coordinator
 * interactions, and performance characteristics specific to share group semantics.
 *
 * <p>Share group metrics complement the standard Kafka source metrics by tracking
 * additional information relevant to message-level load balancing and distribution.
 */
@Internal
public class KafkaShareGroupSourceMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaShareGroupSourceMetrics.class);

    private final MetricGroup metricGroup;

    // Share group specific counters
    private final Counter messagesReceived;
    private final Counter messagesAcknowledged;
    private final Counter messagesRejected;
    private final Counter shareGroupCoordinatorRequests;
    private final Counter shareGroupRebalances;

    // Performance metrics
    private final AtomicLong lastMessageTimestamp;
    private final AtomicLong totalProcessingTime;
    private final AtomicLong messageCount;

    // Share group state metrics
    private final AtomicLong activeConsumersInGroup;
    private final AtomicLong messagesInFlight;

    /**
     * Creates a new metrics collector for the given metric group.
     *
     * @param parentMetricGroup the parent metric group to register metrics under
     */
    public KafkaShareGroupSourceMetrics(MetricGroup parentMetricGroup) {
        this.metricGroup = parentMetricGroup.addGroup("sharegroup");

        // Initialize counters
        this.messagesReceived = metricGroup.counter("messagesReceived");
        this.messagesAcknowledged = metricGroup.counter("messagesAcknowledged");
        this.messagesRejected = metricGroup.counter("messagesRejected");
        this.shareGroupCoordinatorRequests = metricGroup.counter("coordinatorRequests");
        this.shareGroupRebalances = metricGroup.counter("rebalances");

        // Initialize atomic metrics
        this.lastMessageTimestamp = new AtomicLong(0);
        this.totalProcessingTime = new AtomicLong(0);
        this.messageCount = new AtomicLong(0);
        this.activeConsumersInGroup = new AtomicLong(0);
        this.messagesInFlight = new AtomicLong(0);

        // Register gauges
        registerGauges();

        LOG.info("Initialized KafkaShareGroupSourceMetrics");
    }

    /**
     * Records that a message was received from the share group.
     */
    public void recordMessageReceived() {
        messagesReceived.inc();
        lastMessageTimestamp.set(System.currentTimeMillis());
        messagesInFlight.incrementAndGet();
    }

    /**
     * Records that a message was successfully acknowledged.
     *
     * @param processingTimeMs the time taken to process the message in milliseconds
     */
    public void recordMessageAcknowledged(long processingTimeMs) {
        messagesAcknowledged.inc();
        messagesInFlight.decrementAndGet();
        messageCount.incrementAndGet();
        totalProcessingTime.addAndGet(processingTimeMs);
    }

    /**
     * Records that a message was rejected (failed processing).
     */
    public void recordMessageRejected() {
        messagesRejected.inc();
        messagesInFlight.decrementAndGet();
    }

    /**
     * Records a request to the share group coordinator.
     */
    public void recordCoordinatorRequest() {
        shareGroupCoordinatorRequests.inc();
    }

    /**
     * Records a share group rebalance event.
     */
    public void recordRebalance() {
        shareGroupRebalances.inc();
        LOG.debug("Share group rebalance recorded");
    }

    /**
     * Updates the count of active consumers in the share group.
     *
     * @param count the current number of active consumers
     */
    public void updateActiveConsumersCount(long count) {
        activeConsumersInGroup.set(count);
    }

    /**
     * Gets the current number of messages in flight (received but not yet acknowledged).
     *
     * @return the number of messages currently being processed
     */
    public long getMessagesInFlight() {
        return messagesInFlight.get();
    }

    /**
     * Gets the total number of messages received.
     *
     * @return the total message count
     */
    public long getTotalMessagesReceived() {
        return messagesReceived.getCount();
    }

    /**
     * Gets the total number of messages acknowledged.
     *
     * @return the total acknowledged message count
     */
    public long getTotalMessagesAcknowledged() {
        return messagesAcknowledged.getCount();
    }

    /**
     * Gets the current processing rate in messages per second.
     *
     * @return the processing rate, or 0 if no messages have been processed
     */
    public double getCurrentProcessingRate() {
        long count = messageCount.get();
        long totalTime = totalProcessingTime.get();

        if (count == 0 || totalTime == 0) {
            return 0.0;
        }

        return (double) count / (totalTime / 1000.0);
    }

    /**
     * Gets the average message processing time in milliseconds.
     *
     * @return the average processing time, or 0 if no messages have been processed
     */
    public double getAverageProcessingTime() {
        long count = messageCount.get();
        long totalTime = totalProcessingTime.get();

        if (count == 0) {
            return 0.0;
        }

        return (double) totalTime / count;
    }

    private void registerGauges() {
        // Share group state gauges
        metricGroup.gauge("activeConsumers", () -> activeConsumersInGroup.get());
        metricGroup.gauge("messagesInFlight", () -> messagesInFlight.get());

        // Performance gauges
        metricGroup.gauge("averageProcessingTimeMs", this::getAverageProcessingTime);
        metricGroup.gauge("processingRatePerSecond", this::getCurrentProcessingRate);

        // Timing gauges
        metricGroup.gauge("lastMessageTimestamp", () -> lastMessageTimestamp.get());
        metricGroup.gauge("timeSinceLastMessage", () -> {
            long last = lastMessageTimestamp.get();
            return last > 0 ? System.currentTimeMillis() - last : -1;
        });

        // Efficiency gauges
        metricGroup.gauge("messageSuccessRate", () -> {
            long received = messagesReceived.getCount();
            long acknowledged = messagesAcknowledged.getCount();
            return received > 0 ? (double) acknowledged / received : 0.0;
        });

        metricGroup.gauge("messageRejectionRate", () -> {
            long received = messagesReceived.getCount();
            long rejected = messagesRejected.getCount();
            return received > 0 ? (double) rejected / received : 0.0;
        });
    }

    /**
     * Resets all metrics. Used primarily for testing or when starting fresh.
     */
    public void reset() {
        // Note: Counters cannot be reset in Flink metrics, but we can reset our internal state
        lastMessageTimestamp.set(0);
        totalProcessingTime.set(0);
        messageCount.set(0);
        activeConsumersInGroup.set(0);
        messagesInFlight.set(0);

        LOG.info("KafkaShareGroupSourceMetrics reset");
    }

    /**
     * Returns a summary of current metrics as a string.
     *
     * @return formatted metrics summary
     */
    public String getMetricsSummary() {
        return String.format(
            "ShareGroupMetrics{" +
            "received=%d, acknowledged=%d, rejected=%d, " +
            "inFlight=%d, activeConsumers=%d, " +
            "avgProcessingTime=%.2fms, processingRate=%.2f/s, " +
            "successRate=%.2f%%, rejectionRate=%.2f%%}",
            messagesReceived.getCount(),
            messagesAcknowledged.getCount(),
            messagesRejected.getCount(),
            messagesInFlight.get(),
            activeConsumersInGroup.get(),
            getAverageProcessingTime(),
            getCurrentProcessingRate(),
            getSuccessRatePercentage(),
            getRejectionRatePercentage()
        );
    }

    private double getSuccessRatePercentage() {
        long received = messagesReceived.getCount();
        long acknowledged = messagesAcknowledged.getCount();
        return received > 0 ? ((double) acknowledged / received) * 100.0 : 0.0;
    }

    private double getRejectionRatePercentage() {
        long received = messagesReceived.getCount();
        long rejected = messagesRejected.getCount();
        return received > 0 ? ((double) rejected / received) * 100.0 : 0.0;
    }
    
    /**
     * Records a successful commit acknowledgment.
     */
    public void recordSuccessfulCommit() {
        LOG.debug("Recorded successful acknowledgment commit");
    }
    
    /**
     * Records a failed commit acknowledgment.
     */
    public void recordFailedCommit() {
        LOG.debug("Recorded failed acknowledgment commit");
    }
}