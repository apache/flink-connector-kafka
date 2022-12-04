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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;

/** End-to-end test for the kafka SQL connectors. */
@RunWith(Parameterized.class)
public class SQLClientKafkaITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SQLClientKafkaITCase.class);

    private static final String KAFKA_E2E_SQL = "kafka_e2e.sql";

    @Parameterized.Parameters(name = "{index}: kafka-version:{0} kafka-sql-version:{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{"3.2.3", "universal", "kafka", ".*kafka.jar"}});
    }

    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    public static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @ClassRule
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withLogConsumer(LOG_CONSUMER);

    public final TestcontainersSettings testcontainersSettings =
            TestcontainersSettings.builder().network(NETWORK).logger(LOG).dependsOn(KAFKA).build();

    public final FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    // we have to enable checkpoint to trigger flushing for
                                    // filesystem sink
                                    .numSlotsPerTaskManager(2)
                                    .setConfigOption(
                                            ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                                            Duration.ofSeconds(5L))
                                    .build())
                    .withTestcontainersSettings(testcontainersSettings)
                    .build();

    private KafkaContainerClient kafkaClient;

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final String kafkaVersion;
    private final String kafkaSQLVersion;
    private final String kafkaIdentifier;
    private Path result;

    private static final Path sqlAvroJar = ResourceTestUtils.getResource(".*avro.jar");
    private static final Path sqlToolBoxJar = ResourceTestUtils.getResource(".*SqlToolbox.jar");
    private final List<Path> apacheAvroJars = new ArrayList<>();
    private final Path sqlConnectorKafkaJar;

    public SQLClientKafkaITCase(
            String kafkaVersion,
            String kafkaSQLVersion,
            String kafkaIdentifier,
            String kafkaSQLJarPattern) {
        this.kafkaVersion = kafkaVersion;
        this.kafkaSQLVersion = kafkaSQLVersion;
        this.kafkaIdentifier = kafkaIdentifier;

        this.sqlConnectorKafkaJar = ResourceTestUtils.getResource(kafkaSQLJarPattern);
    }

    @Before
    public void before() throws Exception {
        flink.start();
        kafkaClient = new KafkaContainerClient(KAFKA);
        Path tmpPath = tmp.getRoot().toPath();
        LOG.info("The current temporary path: {}", tmpPath);
        this.result = tmpPath.resolve("result");
    }

    @After
    public void tearDown() {
        flink.stop();
    }

    @Test
    public void testKafka() throws Exception {

        // Create topic and send message
        String testJsonTopic = "test-json-" + kafkaVersion + "-" + UUID.randomUUID().toString();
        String testAvroTopic = "test-avro-" + kafkaVersion + "-" + UUID.randomUUID().toString();
        kafkaClient.createTopic(1, 1, testJsonTopic);
        String[] messages =
                new String[] {
                    "{\"rowtime\": \"2018-03-12 08:00:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 08:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:00:00\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:10:00\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:20:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"rowtime\": \"2018-03-12 09:30:00\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
                    "{\"rowtime\": \"2018-03-12 10:40:00\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
                };
        kafkaClient.sendMessages(testJsonTopic, new StringSerializer(), messages);

        // Create topic test-avro
        kafkaClient.createTopic(1, 1, testAvroTopic);

        // Initialize the SQL statements from "kafka_e2e.sql" file
        Map<String, String> varsMap = new HashMap<>();
        varsMap.put("$KAFKA_IDENTIFIER", this.kafkaIdentifier);
        varsMap.put("$TOPIC_JSON_NAME", testJsonTopic);
        varsMap.put("$TOPIC_AVRO_NAME", testAvroTopic);
        varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
        varsMap.put("$KAFKA_BOOTSTRAP_SERVERS", INTER_CONTAINER_KAFKA_ALIAS + ":9092");
        List<String> sqlLines = initializeSqlLines(varsMap);

        // Execute SQL statements in "kafka_e2e.sql" file
        executeSqlStatements(sqlLines);

        // Wait until all the results flushed to the CSV file.
        LOG.info("Verify the CSV result.");
        checkCsvResultFile();
        LOG.info("The Kafka({}) SQL client test run successfully.", this.kafkaSQLVersion);
    }

    private void executeSqlStatements(List<String> sqlLines) throws Exception {
        LOG.info("Executing Kafka {} end-to-end SQL statements.", kafkaSQLVersion);
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJar(sqlAvroJar)
                        .addJars(apacheAvroJars)
                        .addJar(sqlConnectorKafkaJar)
                        .addJar(sqlToolBoxJar)
                        .build());
    }

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = SQLClientKafkaITCase.class.getClassLoader().getResource(KAFKA_E2E_SQL);
        if (url == null) {
            throw new FileNotFoundException(KAFKA_E2E_SQL);
        }

        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }

        return result;
    }

    private void checkCsvResultFile() throws Exception {
        boolean success = false;
        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(300));
        while (deadline.hasTimeLeft()) {
            if (Files.exists(result)) {
                List<String> lines = readCsvResultFiles(result);
                if (lines.size() == 4) {
                    success = true;
                    assertThat(
                            lines.toArray(new String[0]),
                            arrayContainingInAnyOrder(
                                    "2018-03-12 08:00:00.000,Alice,This was a warning.,2,Success constant folding.",
                                    "2018-03-12 09:00:00.000,Bob,This was another warning.,1,Success constant folding.",
                                    "2018-03-12 09:00:00.000,Steve,This was another info.,2,Success constant folding.",
                                    "2018-03-12 09:00:00.000,Alice,This was a info.,1,Success constant folding."));
                    break;
                } else {
                    LOG.info(
                            "The target CSV {} does not contain enough records, current {} records, left time: {}s",
                            result,
                            lines.size(),
                            deadline.timeLeft().getSeconds());
                }
            } else {
                LOG.info("The target CSV {} does not exist now", result);
            }
            Thread.sleep(500);
        }
        Assert.assertTrue("Did not get expected results before timeout.", success);
    }

    private static List<String> readCsvResultFiles(Path path) throws IOException {
        File filePath = path.toFile();
        // list all the non-hidden files
        File[] csvFiles = filePath.listFiles((dir, name) -> !name.startsWith("."));
        List<String> result = new ArrayList<>();
        if (csvFiles != null) {
            for (File file : csvFiles) {
                result.addAll(Files.readAllLines(file.toPath()));
            }
        }
        return result;
    }
}
