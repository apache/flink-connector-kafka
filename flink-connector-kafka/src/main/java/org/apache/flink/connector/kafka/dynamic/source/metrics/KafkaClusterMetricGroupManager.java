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
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Manages metric groups for each cluster. */
@Internal
public class KafkaClusterMetricGroupManager implements AutoCloseable {
    private static final Logger logger =
            LoggerFactory.getLogger(KafkaClusterMetricGroupManager.class);
    private final Map<String, AbstractMetricGroup> metricGroups;

    public KafkaClusterMetricGroupManager() {
        this.metricGroups = new HashMap<>();
    }

    public void register(String kafkaClusterId, KafkaClusterMetricGroup kafkaClusterMetricGroup) {
        if (kafkaClusterMetricGroup.getInternalClusterSpecificMetricGroup()
                instanceof AbstractMetricGroup) {
            metricGroups.put(
                    kafkaClusterId,
                    (AbstractMetricGroup)
                            kafkaClusterMetricGroup.getInternalClusterSpecificMetricGroup());
        } else {
            logger.warn(
                    "MetricGroup {} is an instance of {}, which is not supported. Please use an implementation of AbstractMetricGroup.",
                    kafkaClusterMetricGroup.getInternalClusterSpecificMetricGroup(),
                    kafkaClusterMetricGroup
                            .getInternalClusterSpecificMetricGroup()
                            .getClass()
                            .getSimpleName());
        }
    }

    public void close(String kafkaClusterId) {
        AbstractMetricGroup metricGroup = metricGroups.remove(kafkaClusterId);
        if (metricGroup != null) {
            metricGroup.close();
        } else {
            logger.warn(
                    "Tried to close metric group for {} but it is not registered for lifecycle management",
                    kafkaClusterId);
        }
    }

    @Override
    public void close() throws Exception {
        for (AbstractMetricGroup metricGroup : metricGroups.values()) {
            metricGroup.close();
        }
    }
}
