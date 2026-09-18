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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * Command-line tool for manually managing lingering Kafka transactions.
 *
 * <p>This tool is designed for scenarios where a Flink job has failed or been stopped, but the
 * Kafka transaction remains {@code ONGOING}.
 *
 * <p><b>Impact of Lingering Transactions:</b>
 *
 * <ul>
 *   <li><b>LSO Blocking:</b> Downstream consumers reading with {@code
 *       isolation.level=read_committed} will be blocked at the Last Stable Offset (LSO) and stop
 *       processing new data.
 *   <li><b>Data Unavailability:</b> The data is persisted in Kafka but invisible to consumers with
 *       {@code isolation.level=read_committed}. Aborting this transaction results in <b>data
 *       loss</b> for these consumers (as they will skip the aborted data). Committing it makes the
 *       data visible.
 * </ul>
 *
 * <h3>Building the Tool</h3>
 *
 * <p>The tool is packaged as a standalone "Uber Jar" in the {@code
 * flink-connector-kafka-transaction-tool} module, which bundles all necessary dependencies like
 * {@code kafka-clients} and {@code commons-cli}.
 *
 * <pre>
 * ./mvnw clean package -pl flink-connector-kafka-transaction-tool -am -DskipTests
 * </pre>
 *
 * <p>The resulting JAR will be located in the {@code target/} directory, usually named: {@code
 * flink-connector-kafka-transaction-tool-X.Y-SNAPSHOT-uber-jar.jar}.
 *
 * <h3>Usage</h3>
 *
 * <p>You can execute the JAR directly using Java.
 *
 * <p><b>Abort Transaction:</b><br>
 * Uses the Kafka "fencing" mechanism to force-abort a lingering transaction.
 *
 * <pre>
 * java -jar flink-connector-kafka-transaction-tool-*-uber-jar.jar \
 * --action abort \
 * --bootstrap-servers localhost:9092 \
 * --transactional-id flink-tx-1
 * </pre>
 *
 * <p><b>Commit Transaction:</b><br>
 * Resumes and commits a specific transaction state. <br>
 * <b>WARNING:</b> You must provide the exact {@code producer-id} and {@code epoch} from <b>Flink
 * logs</b> or Checkpoints. <br>
 * <i>It is possible to retrieve these values from the active Kafka Broker state (e.g. via {@code
 * kafka-transactions.sh}). However, if the job has restarted, the broker state reflects the
 * <b>new</b> running transaction. Manually committing the new transaction using this tool will
 * corrupt the state of the running job.</i>
 *
 * <pre>
 * java -jar flink-connector-kafka-transaction-tool-*-uber-jar.jar \
 * --action commit \
 * --bootstrap-servers localhost:9092 \
 * --transactional-id flink-tx-1 \
 * --producer-id 1005 \
 * --epoch 4
 * </pre>
 */
@Internal
public final class KafkaTransactionTool {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransactionTool.class);

    static final String OPTION_ACTION = "action";
    static final String OPTION_BOOTSTRAP_SERVERS = "bootstrap-servers";
    static final String OPTION_TRANSACTIONAL_ID = "transactional-id";
    static final String OPTION_PRODUCER_ID = "producer-id";
    static final String OPTION_EPOCH = "epoch";
    static final String OPTION_COMMAND_CONFIG = "command-config";
    static final String OPTION_HELP = "help";

    private KafkaTransactionTool() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        return run(args, KafkaTransactionManager::new);
    }

    @VisibleForTesting
    static int run(String[] args, Function<Properties, KafkaTransactionManager> managerFactory) {
        final Options options = getOptions(true);
        final HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);

        if (args.length == 0) {
            printHelp(formatter, options);
            return 0;
        }

        try {
            final CommandLine cmd = parseArguments(args);
            if (cmd.hasOption(OPTION_HELP)) {
                printHelp(formatter, options);
                return 0;
            }

            final String action = cmd.getOptionValue(OPTION_ACTION);
            final String bootstrapServers = cmd.getOptionValue(OPTION_BOOTSTRAP_SERVERS);
            final String transactionalId = cmd.getOptionValue(OPTION_TRANSACTIONAL_ID);

            final KafkaTransactionManager manager =
                    managerFactory.apply(
                            loadCommandConfig(cmd.getOptionValue(OPTION_COMMAND_CONFIG)));

            if ("abort".equalsIgnoreCase(action)) {
                manager.abortTransaction(bootstrapServers, transactionalId);
            } else if ("commit".equalsIgnoreCase(action)) {
                // Conditional validation: Commit requires extra args
                if (!cmd.hasOption(OPTION_PRODUCER_ID) || !cmd.hasOption(OPTION_EPOCH)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Action 'commit' requires --%s and --%s.",
                                    OPTION_PRODUCER_ID, OPTION_EPOCH));
                }

                final long producerId = Long.parseLong(cmd.getOptionValue(OPTION_PRODUCER_ID));
                final short epoch = Short.parseShort(cmd.getOptionValue(OPTION_EPOCH));

                manager.commitTransaction(bootstrapServers, transactionalId, producerId, epoch);
            } else {
                throw new IllegalArgumentException(
                        "Unknown action: " + action + ". Supported actions: 'abort', 'commit'.");
            }

            return 0;
        } catch (ParseException e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());
            formatter.printHelp("KafkaTransactionTool", options);
            return 1;
        } catch (NumberFormatException e) {
            System.err.println(
                    "Invalid numeric value: "
                            + e.getMessage()
                            + ". --producer-id must be a valid long,"
                            + " --epoch must be a valid short (0-32767).");
            return 2;
        } catch (CommitOutcomeUnknownException e) {
            LOG.error("{}", e.getMessage(), e);
            return 3;
        } catch (Exception e) {
            LOG.error("{}", e.getMessage(), e);
            return 2;
        }
    }

    static CommandLine parseArguments(String[] args) throws ParseException {
        final boolean helpRequested = Arrays.asList(args).contains("--" + OPTION_HELP);
        final CommandLine commandLine = new DefaultParser().parse(getOptions(!helpRequested), args);
        final Set<String> seenOptions = new HashSet<>();
        for (Option option : commandLine.getOptions()) {
            if (!seenOptions.add(option.getLongOpt())) {
                throw new ParseException(
                        "Option --" + option.getLongOpt() + " must be specified only once.");
            }
        }
        if (!commandLine.getArgList().isEmpty()) {
            throw new ParseException(
                    "Unexpected positional arguments; only named options are supported.");
        }
        return commandLine;
    }

    static Properties loadCommandConfig(String file) throws IOException {
        final Properties properties = new Properties();
        if (file != null) {
            try (InputStream input = Files.newInputStream(Path.of(file))) {
                properties.load(input);
            } catch (IOException | IllegalArgumentException e) {
                throw new IOException("Cannot load --command-config file '" + file + "'.", e);
            }
        }
        return properties;
    }

    private static Options getOptions(boolean required) {
        final Options options = new Options();

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_ACTION)
                        .hasArg()
                        .required(required)
                        .desc("Operation to perform: 'abort' or 'commit'.")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_BOOTSTRAP_SERVERS)
                        .hasArg()
                        .required(required)
                        .desc("Kafka brokers list (e.g. localhost:9092).")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_TRANSACTIONAL_ID)
                        .hasArg()
                        .required(required)
                        .desc("The Kafka Transactional ID.")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_PRODUCER_ID)
                        .hasArg()
                        .desc("(Commit only) Internal Producer ID from Flink State.")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_EPOCH)
                        .hasArg()
                        .desc("(Commit only) Internal Producer Epoch from Flink State.")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_COMMAND_CONFIG)
                        .hasArg()
                        .desc(
                                "Kafka client properties file (e.g. SSL/SASL settings and max.block.ms). CLI target and tool serializers take precedence.")
                        .build());

        options.addOption(
                Option.builder().longOpt(OPTION_HELP).desc("Show this help message.").build());

        return options;
    }

    private static void printHelp(HelpFormatter formatter, Options options) {
        formatter.printHelp(
                "KafkaTransactionTool",
                "Tool to manually manage lingering Flink Kafka transactions.",
                options,
                "Safety: Ensure the Flink job is stopped and cannot restart before using either action.\n"
                        + "Aborting can cause data loss for read_committed consumers. Committing can conflict with Flink recovery.\n"
                        + "Use the exact producer ID and epoch from the failed transaction's Flink logs or checkpoint state.\n"
                        + "Broker metadata may describe a new active transaction after a job restart; never use it without verifying the target.\n"
                        + "If a commit times out or is interrupted, its outcome is unknown. Retry only the same commit; do not abort.\n"
                        + "Abort rejects an unknown transactional ID when the broker supports the existence check; older brokers trigger a warning.\n"
                        + "Exit codes: 0 = success/help; 1 = command-line parsing error; 2 = operation or validation error; 3 = unknown commit outcome.\n"
                        + "A nonzero exit code is not proof that the transaction was not committed.\n"
                        + "See the Kafka transaction tool runbook in the connector documentation.\n\n"
                        + "Examples:\n"
                        + "  abort:  --action abort --bootstrap-servers localhost:9092 --transactional-id flink-tx-1\n"
                        + "  commit: --action commit --bootstrap-servers localhost:9092 --transactional-id flink-tx-1 --producer-id 100 --epoch 5\n"
                        + "  secured cluster: add --command-config client.properties to either command",
                true);
    }
}
