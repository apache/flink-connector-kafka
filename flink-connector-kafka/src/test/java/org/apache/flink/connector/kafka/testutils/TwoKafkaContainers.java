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

package org.apache.flink.connector.kafka.testutils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Wraps 2 Kafka containers into one for test utilities that only accept one container. */
public class TwoKafkaContainers extends GenericContainer<TwoKafkaContainers> {
    private final KafkaContainer kafka0;
    private final KafkaContainer kafka1;

    public TwoKafkaContainers() {
        DockerImageName dockerImageName = DockerImageName.parse(DockerImageVersions.KAFKA);
        this.kafka0 = new KafkaContainer(dockerImageName);
        this.kafka1 = new KafkaContainer(dockerImageName);
    }

    @Override
    public boolean isRunning() {
        return kafka0.isRunning() && kafka1.isRunning();
    }

    @Override
    public void start() {
        kafka0.start();
        kafka1.start();
    }

    @Override
    public void stop() {
        kafka0.stop();
        kafka1.stop();
    }

    public KafkaContainer getKafka0() {
        return kafka0;
    }

    public KafkaContainer getKafka1() {
        return kafka1;
    }
}
