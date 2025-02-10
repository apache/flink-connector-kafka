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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class BackchannelImplTest {

    private static final String PREFIX = "PREFIX";
    private static final int ATTEMPT = 0;
    private static final BackchannelFactory FACTORY = BackchannelFactory.getInstance();

    @Test
    public void testBasicSend() {
        try (WritableBackchannel<String> writable =
                        FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX);
                ReadableBackchannel<String> readable =
                        FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
            String message = "Test message";

            writable.send(message);

            assertThat(readable.poll()).isEqualTo(message);
        }
    }

    @Test
    public void testSendBeforeEstablish() {
        try (WritableBackchannel<String> writable =
                FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
            assertThat(writable.isEstablished()).isFalse();

            String message = "Test message";

            writable.send(message);

            try (ReadableBackchannel<String> readable =
                    FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
                assertThat(readable.isEstablished()).isTrue();
                assertThat(readable.poll()).isEqualTo(message);
            }
        }
    }

    @Test
    public void testPollBeforeEstablish() {
        try (ReadableBackchannel<String> readable =
                FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
            assertThat(readable.isEstablished()).isFalse();
            assertThat(readable.poll()).isNull();
        }
    }

    @Test
    public void testBasicSendTwoThreads() throws InterruptedException, BrokenBarrierException {
        try (WritableBackchannel<String> writable =
                FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
            CyclicBarrier beforeSend = new CyclicBarrier(2);
            CyclicBarrier afterSend = new CyclicBarrier(2);

            String message = "Test message";
            ForkJoinTask<?> task =
                    runInParallel(
                            () -> {
                                try (ReadableBackchannel<String> readable =
                                        FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
                                    beforeSend.await();
                                    afterSend.await();
                                    assertThat(readable.poll()).isEqualTo(message);
                                }
                            });

            beforeSend.await();
            writable.send(message);
            afterSend.await();
            task.join();
        }
    }

    @Test
    public void testSendBeforeEstablishTwoThreads()
            throws BrokenBarrierException, InterruptedException {
        try (WritableBackchannel<String> writable =
                FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
            CyclicBarrier beforeEstablish = new CyclicBarrier(2);

            String message = "Test message";
            ForkJoinTask<?> task =
                    runInParallel(
                            () -> {
                                beforeEstablish.await();
                                try (ReadableBackchannel<String> readable =
                                        FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
                                    assertThat(readable.poll()).isEqualTo(message);
                                }
                            });

            writable.send(message);
            beforeEstablish.await();
            task.join();
        }
    }

    @Test
    public void testPollBeforeEstablishTwoThreads()
            throws BrokenBarrierException, InterruptedException {
        try (ReadableBackchannel<String> readable =
                FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
            CyclicBarrier beforeEstablish = new CyclicBarrier(2);
            CyclicBarrier afterEstablish = new CyclicBarrier(2);
            CyclicBarrier afterSend = new CyclicBarrier(2);

            String message = "Test message";
            ForkJoinTask<?> task =
                    runInParallel(
                            () -> {
                                beforeEstablish.await();
                                try (WritableBackchannel<String> writable =
                                        FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
                                    afterEstablish.await();
                                    writable.send(message);
                                    afterSend.await();
                                }
                            });

            try {
                assertThat(readable.isEstablished()).isFalse();
                assertThat(readable.poll()).isNull();
                beforeEstablish.await();
                afterEstablish.await();
                assertThat(readable.isEstablished()).isTrue();
                afterSend.await();
                assertThat(readable.poll()).isEqualTo(message);
            } finally {
                // make sure to join first before exiting the test or else cleanup did not properly
                // happen
                task.join();
                // writable channel cleaned up
                assertThat(readable.isEstablished()).isFalse();
            }
        }
    }

    @Test
    void testDuplicatePrefix() {
        try (WritableBackchannel<String> writable =
                FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
            assertThatCode(() -> FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX))
                    .hasMessageContaining("duplicate");
        }
    }

    @Test
    void testPrefixReuse() {
        try (ReadableBackchannel<String> readable =
                FACTORY.getReadableBackchannel(1, ATTEMPT, PREFIX)) {
            assertThat(readable.isEstablished()).isFalse();
            try (WritableBackchannel<String> writable =
                    FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
                assertThat(readable.isEstablished()).isTrue();
            }
            assertThat(readable.isEstablished()).isFalse();
            try (WritableBackchannel<String> writable =
                    FACTORY.getWritableBackchannel(1, ATTEMPT, PREFIX)) {
                assertThat(readable.isEstablished()).isTrue();
            }
            assertThat(readable.isEstablished()).isFalse();
        }
    }

    private static ForkJoinTask<?> runInParallel(RunnableWithException r) {
        // convert to callable to allow exceptions
        return ForkJoinPool.commonPool()
                .submit(
                        () -> {
                            r.run();
                            return true;
                        });
    }
}
