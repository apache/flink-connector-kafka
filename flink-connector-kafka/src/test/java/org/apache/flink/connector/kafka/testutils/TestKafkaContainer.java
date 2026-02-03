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

package org.apache.flink.connector.kafka.testutils;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * A wrapper around Kafka test containers that automatically selects between {@link KafkaContainer}
 * (for Apache Kafka images) and {@link ConfluentKafkaContainer} (for Confluent Platform images)
 * based on the image name.
 *
 * <p>The wrapper configures Kafka in KRaft mode (without Zookeeper) with a multi-listener
 * architecture:
 *
 * <ul>
 *   <li>PLAINTEXT listener (port 9092) for host access via localhost
 *   <li>BROKER listener (port 9093) for container-to-container communication
 *   <li>CONTROLLER listener (port 9094) for KRaft consensus
 * </ul>
 *
 * <p>The wrapper delegates all operations to the underlying testcontainers implementation.
 *
 * <p>This class implements both {@link Startable} (for JUnit 5's {@code @Container}) and {@link
 * TestRule} (for JUnit 4's {@code @ClassRule}), ensuring proper lifecycle management and preventing
 * orphan containers in both testing frameworks.
 */
public class TestKafkaContainer implements AutoCloseable, Startable, TestRule {

    private static final String CONTAINER_STARTUP_CHECK = "/bin/kafka-topics --list --bootstrap-server 0.0.0.0:9093 || exit 1";
    private static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(1);
    private final GenericContainer<?> delegate;
    private final boolean isConfluentImage;
    private String networkAlias;

    public TestKafkaContainer(String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public TestKafkaContainer(DockerImageName dockerImageName) {
        // Validate that the image is supported
        if (isConfluentImage(dockerImageName)) {
            this.isConfluentImage = true;
            // Create ConfluentKafkaContainer with Confluent-specific configuration
            // The advertised listeners will be configured in start() when network alias is known
            this.delegate =
                    new ConfluentKafkaContainer(dockerImageName)
                            .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                            .withEnv(
                                    "KAFKA_LISTENERS",
                                    "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094")
                            .withEnv(
                                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                    "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
                            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                            .waitingFor(Wait.forSuccessfulCommand(CONTAINER_STARTUP_CHECK).withStartupTimeout(CONTAINER_STARTUP_TIMEOUT));
        } else if (isApacheKafkaImage(dockerImageName)) {
            this.isConfluentImage = false;
            // Apache Kafka images use the new KafkaContainer from testcontainers
            // which provides native KRaft support
            this.delegate = new KafkaContainer(dockerImageName);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported Kafka image: %s. "
                                    + "TestKafkaContainer currently supports only Confluent Platform (confluentinc/*) "
                                    + "and Apache Kafka (apache/kafka) images.",
                            dockerImageName.asCanonicalNameString()));
        }

        applyCommonConfiguration();
    }

    /**
     * Checks if the provided Docker image is a Confluent Platform image.
     *
     * @param dockerImageName the Docker image name to check
     * @return true if the image is a Confluent Platform image, false otherwise
     */
    private static boolean isConfluentImage(DockerImageName dockerImageName) {
        String repository = dockerImageName.getRepository().toLowerCase();
        return repository.startsWith("confluentinc/");
    }

    /**
     * Checks if the provided Docker image is an Apache Kafka image.
     *
     * @param dockerImageName the Docker image name to check
     * @return true if the image is an Apache Kafka image, false otherwise
     */
    private static boolean isApacheKafkaImage(DockerImageName dockerImageName) {
        String repository = dockerImageName.getRepository().toLowerCase();
        return repository.startsWith("apache/kafka");
    }

    /**
     * Applies common Kafka configuration that is shared between both Confluent and Apache Kafka
     * containers.
     */
    private void applyCommonConfiguration() {
        delegate.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv(
                        "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                        String.valueOf(Duration.ofHours(2).toMillis()));
    }

    /**
     * Returns the bootstrap servers connection string for connecting to Kafka.
     *
     * @return the bootstrap servers in the format {@code host:port}
     */
    public String getBootstrapServers() {
        return isConfluentImage
                ? ((ConfluentKafkaContainer) delegate).getBootstrapServers()
                : ((KafkaContainer) delegate).getBootstrapServers();
    }

    /** Starts the container. Required for @Container support. */
    @Override
    public void start() {
        if (networkAlias != null && isConfluentImage) {
            delegate.withEnv(
                    "KAFKA_ADVERTISED_LISTENERS",
                    "PLAINTEXT://localhost:9092,BROKER://" + networkAlias + ":9093");
        }
        // Apache Kafka container handles network aliases automatically through withNetworkAliases()
        // No additional configuration needed here
        delegate.start();
    }

    /** Stops the container. Required for correct @Container support. */
    @Override
    public void stop() {
        delegate.stop();
    }

    /**
     * Implements TestRule for JUnit 4 @ClassRule support. This ensures proper lifecycle management
     * when used with JUnit 4 tests.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                start();
                try {
                    base.evaluate();
                } finally {
                    stop();
                }
            }
        };
    }

    /** Closes the container. Delegates to {@link #stop()}. */
    @Override
    public void close() {
        stop();
    }

    /**
     * Sets an environment variable in the container.
     *
     * @param key the environment variable name
     * @param value the environment variable value
     * @return this {@link TestKafkaContainer} instance for method chaining
     */
    public TestKafkaContainer withEnv(String key, String value) {
        delegate.withEnv(key, value);
        return this;
    }

    /**
     * Attaches the container to a network.
     *
     * @param network the network to attach to
     * @return this {@link TestKafkaContainer} instance for method chaining
     */
    public TestKafkaContainer withNetwork(Network network) {
        delegate.withNetwork(network);
        return this;
    }

    /**
     * Sets network aliases for the container.
     *
     * @param aliases the network aliases
     * @return this {@link TestKafkaContainer} instance for method chaining
     */
    public TestKafkaContainer withNetworkAliases(String... aliases) {
        delegate.withNetworkAliases(aliases);
        // Store the first network alias for later use in configuring advertised listeners
        if (aliases.length > 0) {
            this.networkAlias = aliases[0];
        }
        return this;
    }

    /**
     * Adds a log consumer to receive container log output.
     *
     * @param consumer the log consumer
     * @return this {@link TestKafkaContainer} instance for method chaining
     */
    public TestKafkaContainer withLogConsumer(Consumer<OutputFrame> consumer) {
        delegate.withLogConsumer(consumer);
        return this;
    }

    /**
     * Returns the network aliases for this container.
     *
     * @return the list of network aliases
     */
    public List<String> getNetworkAliases() {
        return delegate.getNetworkAliases();
    }

    /**
     * Sets Kafka logging level. Currently only supported for Confluent Platform images. Apache
     * Kafka images use different logging configuration and this setting will be ignored.
     *
     * @param logLevel the log level (TRACE, DEBUG, INFO, WARN, ERROR, OFF)
     * @return this {@link TestKafkaContainer} instance for method chaining
     */
    public TestKafkaContainer withKafkaLogLevel(String logLevel) {
        if (isConfluentImage) {
            delegate.withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", logLevel);
            delegate.withEnv("KAFKA_LOG4J_LOGGERS", "state.change.logger=" + logLevel);
            delegate.withEnv("KAFKA_LOG4J_TOOLS_ROOT_LOGLEVEL", logLevel);
        }
        return this;
    }

    /**
     * Checks if the container is currently running.
     *
     * @return true if the container is running, false otherwise
     */
    public Boolean isRunning() {
        return delegate.isRunning();
    }

    /**
     * Returns the container ID.
     *
     * @return the container ID
     */
    public String getContainerId() {
        return delegate.getContainerId();
    }

    /**
     * Returns the underlying GenericContainer for advanced use cases.
     *
     * <p>This is useful when you need to pass the container to APIs that require {@code
     * GenericContainer<?>} type, such as test frameworks or other testcontainers utilities.
     *
     * @return the underlying container instance (either {@link KafkaContainer} or {@link
     *     ConfluentKafkaContainer})
     */
    public GenericContainer<?> getContainer() {
        return delegate;
    }
}
