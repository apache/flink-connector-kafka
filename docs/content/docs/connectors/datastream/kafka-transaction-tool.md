---
title: Kafka Transaction Tool
weight: 4
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kafka Transaction Tool

The Kafka transaction tool commits or aborts a lingering transaction from Flink's exactly-once
Kafka sink. Such a transaction can prevent consumers using `isolation.level=read_committed`
from progressing past the last stable offset. The tool runs as a standalone Java application;
it does not require a running Flink cluster.

{{< hint warning >}}
Use this tool only after the producing job has stopped and cannot restart. If normal Flink
recovery is possible, let Flink resolve the transaction. Committing exposes its records to
`read_committed` consumers; aborting permanently excludes them. Either operation can break
consistency with Flink checkpoints if applied to the wrong transaction.
{{< /hint >}}

## Obtain the tool

The executable artifact is `org.apache.flink:flink-connector-kafka-transaction-tool`, with
classifier `uber-jar`. For a connector release that publishes this artifact to Maven Central,
replace `<version>` with that release's version and download it using Maven:

```bash
mvn dependency:copy \
  '-Dartifact=org.apache.flink:flink-connector-kafka-transaction-tool:<version>:jar:uber-jar' \
  -DoutputDirectory=.
```

The downloaded file is `flink-connector-kafka-transaction-tool-<version>-uber-jar.jar`.
The plain JAR does not bundle the required dependencies. Do not assume the tool is present in
older connector releases; for an unreleased version, build it from source as described below.

To build a development version, run the following from the root of a Kafka connector checkout
that contains the `flink-connector-kafka-transaction-tool` module:

```bash
./mvnw clean package -pl flink-connector-kafka-transaction-tool -am -DskipTests
```

The executable JAR is produced under `flink-connector-kafka-transaction-tool/target/` as
`flink-connector-kafka-transaction-tool-<version>-uber-jar.jar`. The `-am` flag also builds the
required reactor modules. Use a Java runtime supported by the connector version you built.
In the commands below, set `TOOL_JAR` to the path of that executable JAR, then check its help:

```bash
java -jar "$TOOL_JAR" --help
```

## Configure access to Kafka

The tool must be able to reach the brokers and authenticate with the permissions required to
manage the target transaction. Aborting also checks whether the transactional ID exists using
Kafka's Admin API; the client needs `Describe` permission on that transactional ID.
Supply Kafka client properties through `--command-config client.properties`. For example, a
cluster using TLS with a PEM truststore can use:

```properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/ca.pem
max.block.ms=60000
```

Add the SSL client authentication or SASL settings required by your cluster, such as
`sasl.mechanism` and `sasl.jaas.config` for SASL authentication. Restrict access to files that
contain credentials. Without `--command-config`, Kafka's defaults apply.

Properties are loaded first. The command-line bootstrap servers and transactional ID override
`bootstrap.servers` and `transactional.id` in the file. The tool also always uses
`ByteArraySerializer` for `key.serializer` and `value.serializer`, overriding file settings.
Other producer settings are retained. Set `max.block.ms` in the file to control the blocking
bound for transaction operations and the abort existence check; if omitted, the Kafka client
default applies. This bound is different from the broker's transaction expiry controlled by
`transaction.timeout.ms`.

## Resolve a lingering transaction

### 1. Stop the job and identify the original transaction

Confirm the producing job is stopped and disable automatic restarts, including restarts by an
external scheduler or operator. Ensure no other producer can reuse the transactional ID during
the intervention.

Use the job's checkpoint/savepoint information and logs to identify the exact transactional ID
and the checkpoint associated with the pending records. For a commit, obtain the original
producer ID and epoch recorded for that transaction in Flink state or logs. This tool does not
extract these values from checkpoints or list transactions.

Kafka administration tools can help inspect transaction state, but the broker's current
producer ID and epoch may describe a newer transaction after a job restart. Do not substitute
those values for the original transaction's metadata. If you cannot establish which transaction
belongs to the relevant Flink checkpoint, do not commit it manually.

### 2. Choose an outcome consistent with recovery

Commit only when the records belong to a completed checkpoint or savepoint whose output must
be retained, and verify how any later job restoration will account for those records. Committing
an uncheckpointed transaction can expose records that Flink will replay, producing duplicates.

Abort only when discarding the transaction's records is the intended recovery decision. If
Flink has already checkpointed the corresponding input progress, restoring that state will not
replay those records, so aborting can cause data loss. The abort operation fences the previous
producer for the supplied transactional ID; it does not select an old epoch. A restarted job
using that ID would also be affected.

### 3. Run the selected operation

Replace all example values with the verified transaction details. To resume and commit the
original transaction:

```bash
java -jar "$TOOL_JAR" \
  --action commit \
  --bootstrap-servers broker1:9093 \
  --transactional-id flink-tx-1 \
  --producer-id 1005 \
  --epoch 4 \
  --command-config client.properties
```

Alternatively, to fence the producer and abort its open transaction:

```bash
java -jar "$TOOL_JAR" \
  --action abort \
  --bootstrap-servers broker1:9093 \
  --transactional-id flink-tx-1 \
  --command-config client.properties
```

Run only the selected operation. `--producer-id` and `--epoch` are required for commit.
Each option may occur only once, and positional arguments are not accepted.

Before aborting, the tool checks that the transactional ID is known to the cluster. An unknown
ID is rejected without fencing or initializing a producer. If an older broker does not support
this check, the tool warns that it cannot verify the ID and proceeds with fencing. In that case,
verify the ID carefully: fencing an unknown ID can create an empty transaction entry without
resolving the intended transaction. Other check failures stop the operation before fencing.

### 4. Handle an uncertain result

A commit timeout or interruption returns exit code `3` and means the outcome is **unknown**:
Kafka may already have accepted or completed the commit. Keep the job stopped and inspect
Kafka's transaction state and broker logs. If retrying, retry only `commit` with the **same
transactional ID, producer ID, and epoch**, against the same cluster. Never switch to `abort`
after an uncertain commit.
Do not interpret a nonzero exit code as proof that the transaction was not committed.

The tool uses the following exit codes:

| Exit code | Meaning |
| --- | --- |
| `0` | The requested operation completed successfully, or help was displayed. |
| `1` | Command-line parsing failed. |
| `2` | Configuration, validation, or another operation error occurred. Inspect the error and broker state before deciding what to do next. |
| `3` | A commit timed out or was interrupted; its outcome is unknown. Do not abort. |

### 5. Verify before restarting

Confirm the intended transaction outcome using Kafka administration tools and broker logs.
Check that affected `read_committed` consumers progress and that the expected records are
visible after commit, or excluded after abort. Consumer progress alone does not prove that the
correct transaction was resolved. Record the transaction identifiers, chosen action, and result,
then verify that the planned Flink restore point is consistent with that outcome before allowing
the job to restart.
