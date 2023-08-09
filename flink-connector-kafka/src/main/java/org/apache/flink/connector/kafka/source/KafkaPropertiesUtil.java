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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nonnull;

import java.util.Properties;

/** Utility class for modify Kafka properties. */
@Internal
public class KafkaPropertiesUtil {

    private KafkaPropertiesUtil() {}

    public static void copyProperties(@Nonnull Properties from, @Nonnull Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    /**
     * client.id is used for Kafka server side logging, see
     * https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_client.id
     *
     * <p>Set client id prefix to avoid mbean collision warning logs. There are multiple instances
     * of the AdminClient/KafkaConsumer so each instance requires a different client id (i.e. also
     * per cluster).
     *
     * <p>Flink internally configures the clientId, making this the only way to customize the Kafka
     * client id parameter.
     *
     * <p>If this is not done, we will encounter warning logs of the form:
     *
     * <p>WARN org.apache.kafka.common.utils.AppInfoParser [] - Error registering AppInfo mbean
     * javax.management.InstanceAlreadyExistsException:
     * kafka.consumer:type=app-info,id=null-enumerator-consumer
     *
     * <p>WARN org.apache.kafka.common.utils.AppInfoParser [] - Error registering AppInfo mbean
     * javax.management.InstanceAlreadyExistsException:
     * kafka.admin.client:type=app-info,id=null-enumerator-admin-client
     */
    public static void setClientIdPrefix(Properties properties, String kafkaClusterId) {
        String userClientIdPrefix =
                properties.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        properties.setProperty(
                KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
                userClientIdPrefix + "-" + kafkaClusterId);
    }
}
