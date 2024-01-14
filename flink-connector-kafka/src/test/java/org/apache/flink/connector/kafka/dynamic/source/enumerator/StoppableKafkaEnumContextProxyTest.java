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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.testutils.MockKafkaMetadataService;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

/** A test for {@link StoppableKafkaEnumContextProxy}. */
public class StoppableKafkaEnumContextProxyTest {

    private static final String ACTIVE_KAFKA_CLUSTER = "mock-kafka-cluster";
    private static final String INACTIVE_KAFKA_CLUSTER = "mock-inactive-kafka-cluster";

    private volatile boolean throwExceptionFromMainCallable;

    @BeforeEach
    public void beforeEach() {
        throwExceptionFromMainCallable = true;
    }

    @AfterAll
    public static void afterAll() throws Exception {}

    @Test
    public void testOneTimeCallableErrorHandling() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext =
                        new MockSplitEnumeratorContext<>(2);
                StoppableKafkaEnumContextProxy enumContextProxy =
                        createStoppableKafkaEnumContextProxy(enumContext)) {

            AtomicBoolean isCallbackInvoked = new AtomicBoolean();
            setupKafkaTopicPartitionDiscoveryMockCallable(enumContextProxy, isCallbackInvoked);

            CommonTestUtils.waitUtil(
                    () -> enumContext.getOneTimeCallables().size() == 1,
                    Duration.ofSeconds(15),
                    "Could not schedule callable within timeout");

            // not running the next periodic callable, since StoppableKafkaEnumContextProxy has the
            // callable that periodically schedules the proxy task. So the proxy task (e.g. split
            // discovery) is a one time callable in the context of source coordinator.
            enumContext.runNextOneTimeCallable();
            assertThat(isCallbackInvoked)
                    .as("callback should be skipped upon swallowing the error.")
                    .isFalse();
        }
    }

    @Test
    public void testPeriodicCallableErrorHandling() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext =
                        new MockSplitEnumeratorContext<>(2);
                StoppableKafkaEnumContextProxy enumContextProxy =
                        createStoppableKafkaEnumContextProxy(enumContext)) {

            AtomicBoolean isCallbackInvoked = new AtomicBoolean();
            setupKafkaTopicPartitionDiscoveryMockCallable(enumContextProxy, isCallbackInvoked);

            CommonTestUtils.waitUtil(
                    () -> enumContext.getOneTimeCallables().size() == 1,
                    Duration.ofSeconds(15),
                    "Could not schedule callable within timeout");

            enumContext.runNextOneTimeCallable();
            assertThat(isCallbackInvoked)
                    .as("callback should be skipped upon swallowing the error.")
                    .isFalse();
        }
    }

    @Test
    public void testPeriodicCallableThrowsExceptionOnActiveCluster() throws Throwable {
        try (MockSplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext =
                        new MockSplitEnumeratorContext<>(2);
                StoppableKafkaEnumContextProxy enumContextProxy =
                        createStoppableKafkaEnumContextProxy(enumContext, ACTIVE_KAFKA_CLUSTER)) {

            AtomicBoolean isCallbackInvoked = new AtomicBoolean();
            setupKafkaTopicPartitionDiscoveryMockCallable(enumContextProxy, isCallbackInvoked);

            CommonTestUtils.waitUtil(
                    () -> enumContext.getOneTimeCallables().size() == 1,
                    Duration.ofSeconds(15),
                    "Could not schedule callable within timeout");

            assertThatThrownBy(() -> runNextOneTimeCallableAndCatchException(enumContext))
                    .isExactlyInstanceOf(FlinkRuntimeException.class);
            assertThat(isCallbackInvoked)
                    .as("error callback should be invoked since splits have not been assigned yet.")
                    .isTrue();
        }
    }

    private StoppableKafkaEnumContextProxy createStoppableKafkaEnumContextProxy(
            SplitEnumeratorContext enumContext) {
        return createStoppableKafkaEnumContextProxy(enumContext, INACTIVE_KAFKA_CLUSTER);
    }

    private StoppableKafkaEnumContextProxy createStoppableKafkaEnumContextProxy(
            SplitEnumeratorContext enumContext, String contextKafkaCluster) {

        KafkaStream mockStream =
                new KafkaStream(
                        "mock-stream",
                        ImmutableMap.of(
                                ACTIVE_KAFKA_CLUSTER,
                                new ClusterMetadata(
                                        ImmutableSet.of("mock-topic"), new Properties())));

        return new StoppableKafkaEnumContextProxy(
                contextKafkaCluster,
                new MockKafkaMetadataService(Collections.singleton(mockStream)),
                enumContext,
                null);
    }

    // this modeled after `KafkaSourceEnumerator` topic partition subscription to throw the same
    // exceptions
    private void setupKafkaTopicPartitionDiscoveryMockCallable(
            StoppableKafkaEnumContextProxy enumContextProxy, AtomicBoolean isCallbackInvoked) {
        enumContextProxy.callAsync(
                () -> {
                    if (throwExceptionFromMainCallable) {
                        // mock Kafka Exception
                        throw new TimeoutException("Kafka server timed out");
                    } else {
                        // ignore output
                        return null;
                    }
                },
                (res, t) -> {
                    isCallbackInvoked.set(true);
                    if (t != null) {
                        throw new FlinkRuntimeException(t);
                    }
                },
                0,
                1000);
    }

    private void runNextOneTimeCallableAndCatchException(MockSplitEnumeratorContext enumContext)
            throws Throwable {
        try {
            enumContext.runNextOneTimeCallable();
            fail("TimeoutException should have been thrown");
        } catch (TimeoutException e) {
            // catch only Kafka Timeout exceptions since it will be rethrown by
            // `MockSplitEnumeratorContext`
            AtomicReference<Throwable> errorInMainThread =
                    (AtomicReference<Throwable>)
                            Whitebox.getInternalState(enumContext, "errorInMainThread");
            AtomicReference<Throwable> errorInWorkerThread =
                    (AtomicReference<Throwable>)
                            Whitebox.getInternalState(enumContext, "errorInWorkerThread");

            assertThat(errorInMainThread.get())
                    .as("Should be error in main executor thread for async io")
                    .isNotNull();
            assertThat(errorInWorkerThread.get())
                    .as(
                            "Should not be error in worker thread that corresponds to source coordinator thread")
                    .isNull();
        } finally {
            // reset MockSplitEnumeratorContext error state
            Whitebox.setInternalState(
                    enumContext, "errorInMainThread", new AtomicReference<Throwable>());
            Whitebox.setInternalState(
                    enumContext, "errorInWorkerThread", new AtomicReference<Throwable>());
        }
    }
}
