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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.connector.testframe.container.FlinkContainerTestEnvironment;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Logs the output of the JobManager, TaskManager and external system containers when a test fails.
 *
 * <p>The containers relay their stdout through {@code FlinkContainerTestEnvironment}'s logger at
 * INFO, which the CI log configuration suppresses, and CI keeps no container log files. This
 * extension logs the tail of each container's output at WARN on the first failed test of a class,
 * so the cause of a failure inside the Flink cluster is visible in the CI job log. Later failures
 * of the same class are not dumped again: each container's output covers the whole class, and a
 * systemic failure would otherwise repeat it for every remaining test.
 */
class ContainerLogsOnFailure implements TestWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerLogsOnFailure.class);

    /** Bounds the output per container; a container's log covers every test of the class. */
    private static final int MAX_LINES_PER_CONTAINER = 3000;

    private final FlinkContainerTestEnvironment flink;
    private final List<GenericContainer<?>> externalContainers;
    private boolean logged;

    ContainerLogsOnFailure(
            FlinkContainerTestEnvironment flink, GenericContainer<?>... externalContainers) {
        this.flink = flink;
        this.externalContainers = Arrays.asList(externalContainers);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        if (logged) {
            LOG.warn(
                    "{} failed; container output was logged at the first failure of this class",
                    context.getDisplayName());
            return;
        }
        logged = true;
        final Map<String, GenericContainer<?>> containers = new LinkedHashMap<>();
        containers.put("JobManager", flink.getFlinkContainers().getJobManager());
        final List<GenericContainer<?>> taskManagers = flink.getFlinkContainers().getTaskManagers();
        for (int i = 0; i < taskManagers.size(); i++) {
            containers.put("TaskManager-" + i, taskManagers.get(i));
        }
        for (GenericContainer<?> container : externalContainers) {
            containers.put(container.getDockerImageName(), container);
        }
        containers.forEach((name, container) -> logContainer(context, name, container));
    }

    private static void logContainer(
            ExtensionContext context, String name, GenericContainer<?> container) {
        if (container == null || !container.isRunning()) {
            LOG.warn("{}: container {} is not running", context.getDisplayName(), name);
            return;
        }
        try {
            LOG.warn(
                    "{}: output of container {}\n{}",
                    context.getDisplayName(),
                    name,
                    tail(container.getLogs(), MAX_LINES_PER_CONTAINER));
        } catch (RuntimeException e) {
            LOG.warn("Cannot read the output of container {}", name, e);
        }
    }

    private static String tail(String text, int maxLines) {
        final List<String> lines = new ArrayList<>(Arrays.asList(text.split("\n", -1)));
        if (lines.size() <= maxLines) {
            return text;
        }
        final int skipped = lines.size() - maxLines;
        return "["
                + skipped
                + " earlier lines omitted]\n"
                + String.join("\n", lines.subList(skipped, lines.size()));
    }
}
