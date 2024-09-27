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

import java.io.Serializable;

/**
 * A {@code KafkaPartitioner} wraps logic on how to partition records across partitions of multiple
 * Kafka topics.
 */
@PublicEvolving
public interface KafkaPartitioner<T> extends Serializable {
    /**
     * Initializer for the partitioner. This is called once on each parallel sink instance of the
     * Flink Kafka producer. This method should be overridden if necessary.
     *
     * @param parallelInstanceId 0-indexed id of the parallel sink instance in Flink
     * @param parallelInstances the total number of parallel instances
     */
    default void open(int parallelInstanceId, int parallelInstances) {}

    /**
     * Determine the id of the partition that the record should be written to.
     *
     * @param record the record value
     * @param key serialized key of the record
     * @param value serialized value of the record
     * @param targetTopic target topic for the record
     * @param partitions found partitions for the target topic
     * @return the id of the target partition
     */
    int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions);
}
