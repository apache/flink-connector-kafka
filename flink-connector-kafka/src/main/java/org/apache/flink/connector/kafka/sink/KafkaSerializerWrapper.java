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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Wrapper for Kafka {@link Serializer}. */
class KafkaSerializerWrapper<IN> implements SerializationSchema<IN> {

    private final Class<? extends Serializer<? super IN>> serializerClass;
    // Whether the serializer is for key or value.
    private final boolean isKey;
    private final Map<String, String> config;
    private final Function<? super IN, String> topicSelector;

    private transient Serializer<? super IN> serializer;

    KafkaSerializerWrapper(
            Class<? extends Serializer<? super IN>> serializerClass,
            boolean isKey,
            Map<String, String> config,
            Function<? super IN, String> topicSelector) {
        this.serializerClass = checkNotNull(serializerClass);
        this.isKey = isKey;
        this.config = checkNotNull(config);
        this.topicSelector = checkNotNull(topicSelector);
    }

    KafkaSerializerWrapper(
            Class<? extends Serializer<? super IN>> serializerClass,
            boolean isKey,
            Function<? super IN, String> topicSelector) {
        this(serializerClass, isKey, Collections.emptyMap(), topicSelector);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final ClassLoader userCodeClassLoader = selectClassLoader(context);
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(userCodeClassLoader)) {
            initializeSerializer(userCodeClassLoader);

            if (serializer instanceof Configurable) {
                ((Configurable) serializer).configure(config);
            } else {
                serializer.configure(config, isKey);
            }
        } catch (Exception e) {
            throw new IOException("Failed to instantiate the serializer of class " + serializer, e);
        }
    }

    @Override
    public byte[] serialize(IN element) {
        checkState(serializer != null, "Call open() once before trying to serialize elements.");
        return serializer.serialize(topicSelector.apply(element), element);
    }

    /**
     * Selects the class loader to be used when instantiating the serializer. Using a class loader
     * with user code allows users to customize the serializer.
     */
    protected ClassLoader selectClassLoader(InitializationContext context) {
        return context.getUserCodeClassLoader().asClassLoader();
    }

    @SuppressWarnings("unchecked")
    protected void initializeSerializer(ClassLoader classLoader) throws Exception {
        serializer =
                InstantiationUtil.instantiate(
                        serializerClass.getName(), Serializer.class, classLoader);
    }
}
