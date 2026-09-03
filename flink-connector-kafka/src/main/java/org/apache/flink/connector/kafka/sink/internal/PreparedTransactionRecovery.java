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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.PreparedTxnState;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

final class PreparedTransactionRecovery {

    private PreparedTransactionRecovery() {}

    static String prepare(KafkaProducer<?, ?> producer) {
        return invoke(producer, "prepareTransaction").toString();
    }

    static void initialize(KafkaProducer<?, ?> producer) {
        invoke(
                producer,
                "initTransactions",
                new Class<?>[] {Boolean.TYPE},
                new Object[] {true});
    }

    static void complete(KafkaProducer<?, ?> producer, String preparedTransactionState) {
        invoke(
                producer,
                "completeTransaction",
                new Class<?>[] {PreparedTxnState.class},
                new Object[] {new PreparedTxnState(preparedTransactionState)});
    }

    private static Object invoke(KafkaProducer<?, ?> producer, String methodName) {
        return invoke(producer, methodName, new Class<?>[0], new Object[0]);
    }

    private static Object invoke(
            KafkaProducer<?, ?> producer, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = KafkaProducer.class.getMethod(methodName, argTypes);
            return method.invoke(producer, args);
        } catch (NoSuchMethodException e) {
            throw new UnsupportedOperationException(
                    "The configured Kafka client does not expose public transaction 2PC recovery APIs.",
                    e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access Kafka transaction 2PC API.", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }
}
