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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.util.NetUtils;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/** Some utilities methods around Kafka. */
public class KafkaUtils {
    public static Properties getPropertiesFromBrokerList(String brokerList) {
        String[] elements = brokerList.split(",");

        // validate the broker addresses
        for (String broker : elements) {
            NetUtils.getCorrectHostnamePort(broker);
        }

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return props;
    }
}
