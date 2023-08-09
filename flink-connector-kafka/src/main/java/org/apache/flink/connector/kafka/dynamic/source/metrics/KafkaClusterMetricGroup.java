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

package org.apache.flink.connector.kafka.dynamic.source.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import java.util.Map;

/**
 * A custom proxy metric group in order to group {@link
 * org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics} by Kafka cluster.
 *
 * <p>Reports pending records per cluster under DynamicKafkaSource metric group, motivated by
 * standardized connector metrics:
 * https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics.
 */
@Internal
public class KafkaClusterMetricGroup implements SourceReaderMetricGroup {

    public static final String DYNAMIC_KAFKA_SOURCE_METRIC_GROUP = "DynamicKafkaSource";
    public static final String KAFKA_CLUSTER_GROUP_NAME = "kafkaCluster";

    private final MetricGroup internalClusterSpecificMetricGroup;
    private final OperatorIOMetricGroup delegateIOMetricGroup;

    public KafkaClusterMetricGroup(
            MetricGroup dynamicKafkaSourceMetricGroup,
            SourceReaderMetricGroup delegateSourceReaderMetricGroup,
            String kafkaClusterId) {
        this.internalClusterSpecificMetricGroup =
                dynamicKafkaSourceMetricGroup.addGroup(KAFKA_CLUSTER_GROUP_NAME, kafkaClusterId);
        this.delegateIOMetricGroup = delegateSourceReaderMetricGroup.getIOMetricGroup();
    }

    MetricGroup getInternalClusterSpecificMetricGroup() {
        return internalClusterSpecificMetricGroup;
    }

    @Override
    public Counter getNumRecordsInErrorsCounter() {
        throw new UnsupportedOperationException(
                "This is not invoked/supported by KafkaSourceReader as of Flink 1.14.");
    }

    @Override
    public void setPendingBytesGauge(Gauge<Long> gauge) {
        throw new UnsupportedOperationException(
                "This is not invoked/supported by KafkaSourceReader as of Flink 1.14.");
    }

    @Override
    public void setPendingRecordsGauge(Gauge<Long> pendingRecordsGauge) {
        gauge(MetricNames.PENDING_RECORDS, pendingRecordsGauge);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return delegateIOMetricGroup;
    }

    // --------------- MetricGroup methods ---------------

    @Override
    public Counter counter(String name) {
        return internalClusterSpecificMetricGroup.counter(name);
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        return internalClusterSpecificMetricGroup.counter(name, counter);
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        return internalClusterSpecificMetricGroup.gauge(name, gauge);
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
        return internalClusterSpecificMetricGroup.histogram(name, histogram);
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        return internalClusterSpecificMetricGroup.meter(name, meter);
    }

    @Override
    public MetricGroup addGroup(String name) {
        return internalClusterSpecificMetricGroup.addGroup(name);
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        return internalClusterSpecificMetricGroup.addGroup(key, value);
    }

    @Override
    public String[] getScopeComponents() {
        return internalClusterSpecificMetricGroup.getScopeComponents();
    }

    @Override
    public Map<String, String> getAllVariables() {
        return internalClusterSpecificMetricGroup.getAllVariables();
    }

    @Override
    public String getMetricIdentifier(String metricName) {
        return internalClusterSpecificMetricGroup.getMetricIdentifier(metricName);
    }

    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        return internalClusterSpecificMetricGroup.getMetricIdentifier(metricName, filter);
    }
}
