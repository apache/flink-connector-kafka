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

package org.apache.flink.connector.kafka.dynamic.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import java.util.Objects;

/** Split that wraps {@link KafkaPartitionSplit} with Kafka cluster information. */
@Internal
public class DynamicKafkaSourceSplit extends KafkaPartitionSplit {

    private final String kafkaClusterId;
    private final KafkaPartitionSplit kafkaPartitionSplit;

    public DynamicKafkaSourceSplit(String kafkaClusterId, KafkaPartitionSplit kafkaPartitionSplit) {
        super(
                kafkaPartitionSplit.getTopicPartition(),
                kafkaPartitionSplit.getStartingOffset(),
                kafkaPartitionSplit.getStoppingOffset().orElse(NO_STOPPING_OFFSET));
        this.kafkaClusterId = kafkaClusterId;
        this.kafkaPartitionSplit = kafkaPartitionSplit;
    }

    @Override
    public String splitId() {
        return kafkaClusterId + "-" + kafkaPartitionSplit.splitId();
    }

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    public KafkaPartitionSplit getKafkaPartitionSplit() {
        return kafkaPartitionSplit;
    }

    @Override
    public String toString() {
        return "DynamicKafkaSourceSplit{"
                + "kafkaClusterId='"
                + kafkaClusterId
                + '\''
                + ", kafkaPartitionSplit="
                + kafkaPartitionSplit
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DynamicKafkaSourceSplit that = (DynamicKafkaSourceSplit) o;
        return Objects.equals(kafkaClusterId, that.kafkaClusterId)
                && Objects.equals(kafkaPartitionSplit, that.kafkaPartitionSplit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), kafkaClusterId, kafkaPartitionSplit);
    }
}
