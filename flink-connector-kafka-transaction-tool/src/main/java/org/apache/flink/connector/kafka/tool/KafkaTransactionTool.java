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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

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
 * mvn clean package -pl flink-connector-kafka-transaction-tool -DskipTests
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
    static final String OPTION_HELP = "help";

    private KafkaTransactionTool() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);

        // Pre-check for --help
        if (Arrays.asList(args).contains("--" + OPTION_HELP) || args.length == 0) {
            printHelp(formatter, options);
            return 0;
        }

        try {
            CommandLine cmd = parser.parse(options, args);

            String action = cmd.getOptionValue(OPTION_ACTION);
            String bootstrapServers = cmd.getOptionValue(OPTION_BOOTSTRAP_SERVERS);
            String transactionalId = cmd.getOptionValue(OPTION_TRANSACTIONAL_ID);

            KafkaTransactionManager manager = new KafkaTransactionManager();

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
        } catch (Exception e) {
            LOG.error("Execution failed", e);
            return 2;
        }
    }

    private static Options getOptions() {
        final Options options = new Options();

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_ACTION)
                        .hasArg()
                        .required()
                        .desc("Operation to perform: 'abort' or 'commit'.")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_BOOTSTRAP_SERVERS)
                        .hasArg()
                        .required()
                        .desc("Kafka brokers list (e.g. localhost:9092).")
                        .build());

        options.addOption(
                Option.builder()
                        .longOpt(OPTION_TRANSACTIONAL_ID)
                        .hasArg()
                        .required()
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
                Option.builder().longOpt(OPTION_HELP).desc("Show this help message.").build());

        return options;
    }

    private static void printHelp(HelpFormatter formatter, Options options) {
        formatter.printHelp(
                "KafkaTransactionTool",
                "Tool to manually manage lingering Flink Kafka transactions.",
                options,
                "Examples:\n"
                        + "  abort:  --action abort --bootstrap-servers localhost:9092 --transactional-id flink-tx-1\n"
                        + "  commit: --action commit --bootstrap-servers localhost:9092 --transactional-id flink-tx-1 --producer-id 100 --epoch 5",
                true);
    }
}
