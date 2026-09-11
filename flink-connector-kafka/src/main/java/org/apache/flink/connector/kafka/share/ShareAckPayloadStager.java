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

package org.apache.flink.connector.kafka.share;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Internal
public final class ShareAckPayloadStager {

    private static final String SHARE_ACKNOWLEDGEMENTS_CLASS =
            "org.apache.kafka.clients.consumer.ShareAcknowledgements";
    private static final String SHARE_ACKNOWLEDGEMENT_BATCH_CLASS =
            "org.apache.kafka.clients.consumer.ShareAcknowledgementBatch";
    private static final String SHARE_GROUP_METADATA_CLASS =
            "org.apache.kafka.clients.consumer.ShareGroupMetadata";
    private static final String TOPIC_ID_PARTITION_CLASS = "org.apache.kafka.common.TopicIdPartition";
    private static final String UUID_CLASS = "org.apache.kafka.common.Uuid";

    private ShareAckPayloadStager() {}

    public static void stage(Object producer, ShareAckPayload payload) throws IOException {
        try {
            Object acknowledgements = acknowledgements(payload);
            Object groupMetadata = groupMetadata(payload);
            Method method =
                    producer.getClass()
                            .getMethod(
                                    "sendShareAcknowledgementsToTransaction",
                                    acknowledgements.getClass(),
                                    groupMetadata.getClass());
            method.invoke(producer, acknowledgements, groupMetadata);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IOException("Kafka client does not expose transactional share ack APIs.", e);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IOException("Failed to construct transactional share ack request.", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException("Failed to stage transactional share acknowledgements.", cause);
        }
    }

    private static Object groupMetadata(ShareAckPayload payload)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
                    InstantiationException, IllegalAccessException {
        Class<?> metadataClass = Class.forName(SHARE_GROUP_METADATA_CLASS);
        Constructor<?> constructor =
                metadataClass.getConstructor(String.class, String.class, Integer.TYPE);
        return constructor.newInstance(
                payload.getGroupId(), payload.getMemberId(), payload.getMemberEpoch());
    }

    private static Object acknowledgements(ShareAckPayload payload)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
                    InstantiationException, IllegalAccessException {
        Class<?> uuidClass = Class.forName(UUID_CLASS);
        Class<?> topicIdPartitionClass = Class.forName(TOPIC_ID_PARTITION_CLASS);
        Class<?> batchClass = Class.forName(SHARE_ACKNOWLEDGEMENT_BATCH_CLASS);
        Class<?> acknowledgementsClass = Class.forName(SHARE_ACKNOWLEDGEMENTS_CLASS);

        Method uuidFromString = uuidClass.getMethod("fromString", String.class);
        Constructor<?> topicIdPartitionConstructor =
                topicIdPartitionClass.getConstructor(uuidClass, Integer.TYPE, String.class);
        Constructor<?> batchConstructor =
                batchClass.getConstructor(Long.TYPE, Long.TYPE, List.class);
        Constructor<?> acknowledgementsConstructor = acknowledgementsClass.getConstructor(Map.class);

        Map<Object, List<Object>> acknowledgementsByPartition = new LinkedHashMap<>();
        for (ShareAckPayload.TopicPartitionAcknowledgements partition :
                payload.getAcknowledgements()) {
            Object topicId = uuidFromString.invoke(null, partition.getTopicId());
            Object topicIdPartition =
                    topicIdPartitionConstructor.newInstance(
                            topicId, partition.getPartition(), partition.getTopic());
            List<Object> batches = new ArrayList<>();
            for (ShareAckPayload.AcknowledgementBatch batch : partition.getBatches()) {
                batches.add(
                        batchConstructor.newInstance(
                                batch.getFirstOffset(),
                                batch.getLastOffset(),
                                batch.getAcknowledgeTypes()));
            }
            acknowledgementsByPartition.put(topicIdPartition, batches);
        }

        return acknowledgementsConstructor.newInstance(acknowledgementsByPartition);
    }
}
