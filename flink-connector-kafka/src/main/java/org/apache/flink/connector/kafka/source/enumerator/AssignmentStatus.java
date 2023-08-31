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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;

/** status of partition assignment. */
@Internal
public enum AssignmentStatus {

    /** Partitions that have been assigned to readers. */
    ASSIGNED(0),
    /**
     * The partitions that have been discovered during initialization but not assigned to readers
     * yet.
     */
    UNASSIGNED_INITIAL(1);
    private final int statusCode;

    AssignmentStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public static AssignmentStatus ofStatusCode(int statusCode) {
        for (AssignmentStatus statusEnum : AssignmentStatus.values()) {
            if (statusEnum.getStatusCode() == statusCode) {
                return statusEnum;
            }
        }
        throw new IllegalArgumentException("statusCode is invalid.");
    }
}
