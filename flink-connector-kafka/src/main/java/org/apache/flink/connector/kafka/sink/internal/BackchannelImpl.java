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

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * A backchannel for communication between the Kafka committer -> writer. It's used to recycle
 * producer and signal that certain transactions have been committed on recovery.
 *
 * <p>A backchannel provides two views: a readable view for the committer to read messages, and a
 * writable view for the writer to send messages. Both views have a separate lifecycle. The
 * lifecycle of this backchannel is bound to the joint lifecycle of the readable and writable views.
 */
@Internal
@ThreadSafe
public final class BackchannelImpl<T> {
    /**
     * The action to be executed when the backchannel is closed. The channel is classed if both
     * readable and writable channels are closed.
     */
    private final Runnable closeAction;

    /**
     * The messages to be sent from the writer to the committer. It's a thread safe deque in case
     * committer and writer are not chained.
     */
    private final Deque<T> messages = new ConcurrentLinkedDeque<>();

    /** The readable backchannel. */
    private volatile ReadableBackchannel<T> readableBackchannel;

    /** The writable backchannel. */
    private volatile WritableBackchannel<T> writableBackchannel;

    BackchannelImpl(Runnable closeAction) {
        this.closeAction = closeAction;
    }

    /**
     * True iff the backchannel is established, i.e. both readable and writable channels are
     * created.
     */
    private boolean isEstablished() {
        return readableBackchannel != null && writableBackchannel != null;
    }

    /**
     * Closes the readable channel. If the writable channel is also closed, the backchannel is
     * closed.
     */
    private void closeReadableChannel() {
        if (readableBackchannel == null) {
            throw new IllegalStateException("Readable backchannel does not exist.");
        }
        readableBackchannel = null;
        checkClosed();
    }

    /** Checks if the backchannel is considered closed. If so, executes the close action. */
    private void checkClosed() {
        if (readableBackchannel == null && writableBackchannel == null) {
            closeAction.run();
        }
    }

    ReadableBackchannel<T> createReadableBackchannel() {
        if (readableBackchannel != null) {
            throw new IllegalStateException("Readable backchannel already exists.");
        }
        return readableBackchannel = new Readable();
    }

    WritableBackchannel<T> createWritableBackchannel() {
        if (writableBackchannel != null) {
            throw new IllegalStateException("Writable backchannel already exists.");
        }
        return writableBackchannel = new Writable();
    }

    private void closeWritableChannel() {
        if (writableBackchannel == null) {
            throw new IllegalStateException("Writable backchannel does not exist.");
        }
        writableBackchannel = null;
        checkClosed();
    }

    private class Writable implements WritableBackchannel<T> {
        @Override
        public void send(T message) {
            messages.add(message);
        }

        public boolean isEstablished() {
            return BackchannelImpl.this.isEstablished();
        }

        @Override
        public void close() {
            closeWritableChannel();
        }
    }

    private class Readable implements ReadableBackchannel<T> {
        @Nullable
        @Override
        public T poll() {
            return messages.poll();
        }

        @Override
        public boolean isEstablished() {
            return BackchannelImpl.this.isEstablished();
        }

        @Override
        public void close() {
            closeReadableChannel();
        }
    }
}
