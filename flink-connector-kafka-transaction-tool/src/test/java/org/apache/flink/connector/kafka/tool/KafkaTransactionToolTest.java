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

package org.apache.flink.connector.kafka.tool;

import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaTransactionTool} CLI argument parsing. */
class KafkaTransactionToolTest {

    @TempDir private Path temporaryDirectory;

    @Test
    void testLoadsCommandConfig() throws Exception {
        final Path configuration = temporaryDirectory.resolve("client.properties");
        Files.writeString(
                configuration,
                "# Client settings\nsecurity.protocol=SASL_SSL\nsasl.mechanism=GSSAPI\nmax.block.ms=30000\n");

        assertThat(KafkaTransactionTool.loadCommandConfig(configuration.toString()))
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "GSSAPI")
                .containsEntry("max.block.ms", "30000");
    }

    @Test
    void testCommandConfigIsOptional() throws Exception {
        assertThat(KafkaTransactionTool.loadCommandConfig(null)).isEmpty();
    }

    @Test
    void testMissingCommandConfigFails() {
        assertThatThrownBy(
                        () ->
                                KafkaTransactionTool.loadCommandConfig(
                                        temporaryDirectory
                                                .resolve("missing.properties")
                                                .toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot load --command-config");
    }

    @Test
    void testMalformedCommandConfigFails() throws Exception {
        final Path configuration = temporaryDirectory.resolve("malformed.properties");
        Files.writeString(configuration, "client.id=\\uZZZZ\n");

        assertThatThrownBy(() -> KafkaTransactionTool.loadCommandConfig(configuration.toString()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot load --command-config")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDuplicateCommandConfigIsRejectedBeforeLoading() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "abort",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--command-config", "first.properties",
                                    "--command-config", "second.properties"
                                }))
                .isEqualTo(1);
    }

    @Test
    void testDuplicateDestructiveActionsAreRejected() {
        assertThatThrownBy(
                        () ->
                                KafkaTransactionTool.parseArguments(
                                        new String[] {
                                            "--action", "abort",
                                            "--action", "commit",
                                            "--bootstrap-servers", "localhost:9092",
                                            "--transactional-id", "tx-1"
                                        }))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("--action must be specified only once");
    }

    @Test
    void testHelpDoesNotAcceptAmbiguousArguments() {
        assertThat(KafkaTransactionTool.run(new String[] {"--help", "--help"})).isEqualTo(1);
        assertThat(KafkaTransactionTool.run(new String[] {"--help", "extra"})).isEqualTo(1);
    }

    @Test
    void testCommandConfigIsAcceptedBeforeNumericValidation() throws Exception {
        final Path configuration = temporaryDirectory.resolve("client.properties");
        Files.writeString(configuration, "security.protocol=SSL\nmax.block.ms=30000\n");

        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "invalid",
                                    "--epoch", "5",
                                    "--command-config", configuration.toString()
                                }))
                .isEqualTo(2);
    }

    @Test
    void testDuplicateOptionsAreRejected() {
        for (String option :
                Arrays.asList(
                        "action",
                        "bootstrap-servers",
                        "transactional-id",
                        "producer-id",
                        "epoch")) {
            final List<String> arguments =
                    new ArrayList<>(
                            Arrays.asList(
                                    "--action", "unknown",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100",
                                    "--epoch", "5"));
            arguments.add("--" + option);
            arguments.add("unexpected");

            assertThat(KafkaTransactionTool.run(arguments.toArray(new String[0])))
                    .as("duplicate --%s", option)
                    .isEqualTo(1);
        }
    }

    @Test
    void testLeftoverArgumentsAreRejected() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "unknown",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "extra"
                                }))
                .isEqualTo(1);
    }

    @Test
    void testHelpIncludesOperationalPreconditions() {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final PrintStream originalOutput = System.out;
        try (PrintStream stream = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(stream);
            assertThat(KafkaTransactionTool.run(new String[] {"--help"})).isZero();
        } finally {
            System.setOut(originalOutput);
        }

        assertThat(output.toString(StandardCharsets.UTF_8).replaceAll("\\s+", " "))
                .contains(
                        "job is stopped",
                        "cannot restart",
                        "data loss",
                        "new active transaction",
                        "outcome is unknown",
                        "unknown transactional ID",
                        "0 = success/help",
                        "1 = command-line parsing error",
                        "2 = operation or validation error",
                        "3 = unknown commit outcome",
                        "not proof that the transaction was not committed",
                        "--command-config");
    }

    @Test
    void testHelpReturnsZero() {
        assertThat(KafkaTransactionTool.run(new String[] {"--help"})).isEqualTo(0);
    }

    @Test
    void testEmptyArgsReturnsZero() {
        assertThat(KafkaTransactionTool.run(new String[] {})).isEqualTo(0);
    }

    @Test
    void testMissingRequiredArgsReturnsOne() {
        assertThat(KafkaTransactionTool.run(new String[] {"--action", "abort"})).isEqualTo(1);
    }

    @Test
    void testUnknownActionReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "unknown",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testCommitWithoutProducerIdReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--epoch", "5"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testCommitWithoutEpochReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testNonNumericProducerIdReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "abc",
                                    "--epoch", "5"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testEpochOverflowReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100",
                                    "--epoch", "50000"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testNonNumericEpochReturnsTwo() {
        assertThat(
                        KafkaTransactionTool.run(
                                new String[] {
                                    "--action", "commit",
                                    "--bootstrap-servers", "localhost:9092",
                                    "--transactional-id", "tx-1",
                                    "--producer-id", "100",
                                    "--epoch", "xyz"
                                }))
                .isEqualTo(2);
    }

    @Test
    void testCommitTimeoutReturnsThree() {
        assertThat(runCommitWithFailure(new TimeoutException("Commit acknowledgement timed out")))
                .isEqualTo(3);
    }

    @Test
    void testCommitInterruptionReturnsThreeAndRestoresInterrupt() {
        try {
            assertThat(runCommitWithFailure(new InterruptException("Commit interrupted")))
                    .isEqualTo(3);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void testFencedCommitReturnsTwo() {
        assertThat(runCommitWithFailure(new ProducerFencedException("Producer fenced")))
                .isEqualTo(2);
    }

    private static int runCommitWithFailure(RuntimeException failure) {
        return KafkaTransactionTool.run(
                new String[] {
                    "--action", "commit",
                    "--bootstrap-servers", "localhost:9092",
                    "--transactional-id", "tx-1",
                    "--producer-id", "100",
                    "--epoch", "5"
                },
                properties ->
                        new KafkaTransactionManager(
                                properties,
                                (producerProperties, id) -> {
                                    throw failure;
                                }));
    }
}
