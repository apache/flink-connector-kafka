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

# Flink Kafka Connector AI Agent Instructions

This file provides guidance for AI coding agents working with the Apache Flink Kafka connector codebase.

## Prerequisites

- Java 11, 17, or 21. Java 11 syntax must be used everywhere: the build compiles with source level 11 (target 17). On JDK 11 pass `-Pjava11-target`, as CI does.
- Maven 3.8.6 (Maven wrapper `./mvnw` included; prefer it)
- Git
- Docker (every `*ITCase` starts Kafka through Testcontainers)
- Python 3.9 to 3.11 with tox, only for `flink-python`
- Unix-like environment (Linux, macOS, WSL)
- The connector builds against `flink.version` in the root `pom.xml`, which is the lowest supported Flink minor version. A change must also work against every Flink version in `.github/workflows/push_pr.yml` and the `main` rows of `.github/workflows/weekly.yml`.

## Commands

### Build

- Build without tests: `./mvnw clean install -DskipTests`
- Full build with tests: `./mvnw clean verify`
- Build against another Flink version (what CI does): `./mvnw clean install -DskipTests -Dflink.version=<version>`
- Single module: `./mvnw clean install -DskipTests -pl flink-connector-kafka`
- `-Dfast` skips RAT, checkstyle, spotless, enforcer and javadoc, but not japicmp in this repository.
- Dependency convergence (CI runs this on PRs): `./mvnw clean install -DskipTests -Pcheck-convergence -Dflink.convergence.phase=install`

### Testing

- `*Test` classes run in the `test` phase, everything else (`*ITCase`) in the `integration-test` phase through a second Surefire execution; there is no Failsafe plugin.
- Single unit test class: `./mvnw test -pl flink-connector-kafka -Dtest=KafkaSourceReaderTest`
- Single test method: `./mvnw test -pl flink-connector-kafka -Dtest=KafkaSourceReaderTest#testCommitOffsetsWithoutAliveFetchers`
- Single ITCase: `./mvnw test -pl flink-connector-kafka -Dtest=KafkaSinkITCase` (`-Dtest` overrides the phase filter; `verify -Dtest=...` runs the class twice)
- ArchUnit rules: `./mvnw test -pl flink-connector-kafka -Dtest='*ArchitectureTest'`, then check `git status flink-connector-kafka/archunit-violations/` (see Testing Standards)
- End-to-end tests need a Flink distribution: `./mvnw clean verify -Prun-end-to-end-tests -DdistDir=<path to flink-<version>>`. CI runs them on every PR.
- PyFlink tests: `./mvnw clean install -DskipTests`, then `cd flink-python && chmod a+x dev/* && ./dev/lint-python.sh -e mypy,sphinx`. The scripts are downloaded into `flink-python/dev/` during the Maven `validate` phase without the executable bit, which is why CI sets it first.
- CI is defined by `apache/flink-connector-shared-utils` (`.github/workflows/ci.yml@ci_utils`); its Maven command line, including the license check, is the reference when a local run differs from CI.

### Code Quality

- Format code: `./mvnw spotless:apply` (skipped automatically on JDK 21; run it on 11 or 17)
- Check formatting: `./mvnw spotless:check`
- Checkstyle: `./mvnw checkstyle:check`
- Checkstyle config: `tools/maven/checkstyle.xml`
- License headers: `./mvnw apache-rat:check`
- `verify` also runs `dependency:analyze` with `failOnWarning=true`: a new import from a transitively available artifact fails the build until the dependency is declared in the module's `pom.xml`.
- japicmp compares against `japicmp.referenceVersion` from the root `pom.xml` and only checks `@Public` API.

### Documentation

- There is no docs build in this repository. The Flink docs build (`docs/setup_docs.sh` in `apache/flink`) clones the release branch of this repository and renders `docs/content` and `docs/content.zh`.
- Option tables are hand-written HTML rows; nothing is generated from `ConfigOption` definitions.

## Repository Structure

### Modules

- `flink-connector-kafka` — The connector: `KafkaSource`, `KafkaSink`, `DynamicKafkaSource`, and the Table/SQL factories. Also publishes a test-jar with `KafkaTestEnvironment*` and `testutils`.
- `flink-sql-connector-kafka` — Shaded SQL jar; relocates `org.apache.kafka`. Bundled dependencies are listed in `src/main/resources/META-INF/NOTICE`.
- `flink-connector-kafka-e2e-tests/` — `flink-streaming-kafka-test` (the job), `flink-streaming-kafka-test-base` (shared classes), `flink-end-to-end-tests-common-kafka` (the tests).
- `flink-python` — PyFlink wrappers (`pyflink/datastream/connectors/kafka.py`) and their tests (`pyflink/datastream/connectors/tests/test_kafka.py`). Maven packaging `pom`, no Java.

### Supporting directories

- `docs/content/docs/connectors/` and `docs/content.zh/docs/connectors/` — DataStream and Table docs, English and Chinese, same file set.
- `tools/maven/` — checkstyle and suppressions.
- `tools/releasing/shared` — Git submodule with the release scripts (`git submodule update --init`).
- `.github/workflows/` — `push_pr.yml` (PR CI), `weekly.yml` (release branches and Flink snapshots).

### Key packages in `flink-connector-kafka/src/main/java`

- `org.apache.flink.connector.kafka.source` — FLIP-27 source. `@PublicEvolving`: `KafkaSource`, `KafkaSourceBuilder`, `enumerator.initializer.OffsetsInitializer`, `enumerator.subscriber.KafkaSubscriber`, `reader.deserializer.KafkaRecordDeserializationSchema`, `metrics.KafkaSourceReaderMetrics`. `@Internal`: `enumerator/`, `reader/`, `split/`, `KafkaSourceOptions`, `enumerator.metadata` (topic integrity).
- `org.apache.flink.connector.kafka.sink` — Sink V2 sink. `@PublicEvolving`: `KafkaSink`, `KafkaSinkBuilder`, `KafkaRecordSerializationSchema` and its builder, `KafkaPartitioner`, `TopicSelector`, `HeaderProvider`, `TransactionNamingStrategy`. `sink.internal` (transactions, producer pool, backchannel) is `@Internal`.
- `org.apache.flink.connector.kafka.dynamic` — Multi-cluster source. `@Experimental`: `DynamicKafkaSource`, `DynamicKafkaSourceBuilder`, `metadata.KafkaMetadataService`, `metadata.KafkaStream`, `metadata.ClusterMetadata`, `metadata.SingleClusterTopicMetadataService`, `KafkaStreamSubscriber`. Enumerator, reader and split classes are `@Internal`.
- `org.apache.flink.connector.kafka.lineage` — OpenLineage facets, `@PublicEvolving`.
- `org.apache.flink.streaming.connectors.kafka.table` — Table/SQL layer. The three factories (`kafka`, `upsert-kafka`, `dynamic-kafka`) are registered in `META-INF/services/org.apache.flink.table.factories.Factory`. `KafkaConnectorOptions` and `DynamicKafkaConnectorOptions` are `@PublicEvolving`; the rest is `@Internal`.
- `org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema` — `@PublicEvolving`.
- The legacy `FlinkKafkaConsumer` and `FlinkKafkaProducer` exist only on the `v3.x` branches. Do not reintroduce them.

## Architecture Boundaries

1. **Source (FLIP-27).** `KafkaSourceEnumerator` runs on the coordinator thread and does all broker I/O through `context.callAsync` (the split lifecycle is documented in its class Javadoc). `KafkaSourceReader` runs on the task thread; the `SplitFetcher` threads own the `KafkaConsumer`, which must only be accessed from one thread. Offset commits are therefore enqueued onto the fetcher by `KafkaSourceFetcherManager.commitOffsets`. A failed commit is logged and counted in the metrics; it never fails the job.
2. **Sink (Sink V2).** `KafkaWriter` runs on the mailbox thread. Producer callbacks run on the Kafka network thread: `onCompletion` stores the first exception in a `volatile` field and schedules the rethrow on the mailbox through `MailboxExecutor`, and it increments `numRecordsSent` on that same callback thread. `numRecordsSent` is a plain `long`, deliberately not volatile to keep the callback cheap, and is only read for logging, so do not rely on it as an accurate counter. `checkAsyncException()` must be called from the mailbox thread, as its Javadoc says.
3. **Exactly-once.** `ExactlyOnceKafkaWriter`, `KafkaCommitter`, `ProducerPoolImpl` (`@NotThreadSafe`, owned by the writer), `Backchannel` (rebuilt from committer state on recovery, never checkpointed) and `FlinkKafkaInternalProducer`. The producer uses reflection into Kafka's `TransactionManager`; re-check it on every `kafka.version` bump. The committer distinguishes retriable Kafka exceptions from fatal ones (`ProducerFencedException`, `InvalidTxnStateException`); keep that taxonomy intact.
4. **Dynamic source.** `StoppableKafkaEnumContextProxy` keeps the enumerator context single-threaded across metadata changes; `DynamicKafkaSourceReader` restarts its sub-readers on a `MetadataUpdateEvent`.
5. **Table layer.** `KafkaDynamicSource` and `KafkaDynamicSink` wrap the DataStream connectors. Options live only in the `*Options` classes (enforced by ArchUnit).
6. **Connector vs Flink.** Production code may depend only on `@Public` and `@PublicEvolving` Flink API outside connector and util packages (ArchUnit rule with a frozen exemption list). Every Flink API used must exist with the same annotation in `flink.version`, because the connector is released for several Flink minor versions. `flink-connector-base` stays `provided` and is never bundled.

## Common Change Patterns

### Adding a Table/SQL option

1. Define the `ConfigOption<T>` in `KafkaConnectorOptions` (or `DynamicKafkaConnectorOptions`)
2. Register it in the factory's `optionalOptions()` and validate it in `KafkaConnectorOptionsUtil`
3. Add a factory test and, when behaviour changes, an ITCase in `KafkaTableITCase` or `UpsertKafkaTableITCase`
4. Add the option row to `docs/content/docs/connectors/table/*.md` and the same file under `docs/content.zh/`
5. Fill in the Release Notes field on the JIRA ticket

### Adding a DataStream builder option

1. Add it to `KafkaSourceBuilder` or `KafkaSinkBuilder` with validation in `build()`
2. Add a builder unit test and, when behaviour changes, an ITCase
3. Mirror it in the PyFlink wrapper (`flink-python/pyflink/datastream/connectors/kafka.py`) and `test_kafka.py`
4. Document it in `docs/content/docs/connectors/datastream/kafka.md` and the `.zh` copy

### Changing checkpointed state

Splits, enumerator state, writer state and committables are written by `SimpleVersionedSerializer` implementations. Current versions: `KafkaPartitionSplitSerializer` 0, `KafkaSourceEnumStateSerializer` 4, `KafkaWriterStateSerializer` 2, `KafkaCommittableSerializer` 1, `DynamicKafkaSourceSplitSerializer` 2, `DynamicKafkaSourceEnumStateSerializer` 3.

1. Bump the version and keep a read path for every older version
2. Enum ordinals written to state (for example `TransactionOwnership`) must not be reordered
3. Verify: serializer test with bytes of the previous version; `KafkaSourceMigrationITCase` for the source

### Bumping `kafka.version`

1. Re-check the reflection in `FlinkKafkaInternalProducer`
2. Update `flink-sql-connector-kafka/src/main/resources/META-INF/NOTICE`
3. Update `DockerImageVersions` in the test utils: `APACHE_KAFKA` follows `kafka.version`, `CP_KAFKA` and `SCHEMA_REGISTRY` follow `confluent.version`
4. Verify: `dependency:tree` for new transitive dependencies, the license check from CI, the full ITCase suite

### Bumping `flink.version` or changing the CI matrix

1. Get consensus on the JIRA ticket first; this changes which Flink versions the branch supports
2. Update `push_pr.yml` and `weekly.yml`; the workflows must stay under the ASF limit of 20 concurrent jobs
3. Verify: the build against every Flink version in the matrix

### Fixing a flaky test

1. Name the race or ordering that fails, with the CI log excerpt
2. Wait on the condition (`CommonTestUtils.waitUtil`, `KafkaUtil.createNewTopicAndWaitForPartitionAssignment`), never on time
3. Do not add `Thread.sleep`, larger timeouts, retries or `@Disabled`
4. Verify: run the test repeatedly and state the number of runs in the PR

## Coding Standards

- **Format Java files with Spotless immediately after editing:** `./mvnw spotless:apply`. Uses google-java-format with AOSP style.
- **Checkstyle:** `tools/maven/checkstyle.xml`. Do not suppress rules; fix the code instead.
- **Apache License 2.0 header** required on all new files (enforced by Apache Rat). Use an HTML comment for markdown files.
- **API stability annotations:** Every user-facing API class and method must have a stability annotation. `@Public` (stable across minor releases), `@PublicEvolving` (may change in minor releases), `@Experimental` (may change at any time). `@Internal` marks APIs with no stability guarantees that users should not depend on.
- **Kafka clients:** The `KafkaConsumer` is accessed from one thread only; the producer is thread-safe but its callbacks run on the Kafka network thread, so hand results back to the mailbox thread.
- **Logging:** Use parameterized log statements (SLF4J `{}` placeholders), never string concatenation.
- **No Java serialization** for new features.
- **Use `final`** for variables and fields where applicable.
- **Comments:** Do not add unnecessary comments that restate what the code does. Add comments that explain "the why" where relevant.
- **Reuse existing code.** Before implementing new utilities, search for existing ones: `AdminUtils`, `MetricUtil`, `KafkaPropertiesUtil` in production code, `KafkaUtil` in the test utils.
- Full code style guide: https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/

## Testing Standards

- Add tests for new behavior, covering success, failure, and edge cases.
- Use **JUnit 5** + **AssertJ** assertions. JUnit 4 is banned by an ArchUnit rule; Mockito and PowerMock are banned by the Maven enforcer.
- **Integration tests:** Name classes with `ITCase` suffix and use `MiniClusterExtension`.
- **Kafka in tests:** `KafkaUtil.createKafkaContainer` with the image constants from this repository's `DockerImageVersions`, not Flink's. Test bases: `KafkaTestBase` (starts the container cluster), `KafkaTableTestBase` (Table API), `KafkaSourceTestEnv`, `KafkaWriterTestBase`.
- **Connector testing framework:** `KafkaSourceITCase` and `KafkaSinkITCase` each carry a nested `IntegrationTests` class extending `SourceTestSuiteBase` or `SinkTestSuiteBase` from `flink-connector-test-utils`; the end-to-end module reuses the same external contexts.
- **Red-green verification:** For bug fixes, verify that new tests actually fail without the fix before confirming they pass with it.
- **ArchUnit:** The violation stores under `flink-connector-kafka/archunit-violations/` are frozen. A local run updates them when violations disappear; commit removed lines together with the change, never add lines.
- **Migration fixtures** (savepoints, serialized state of older versions) live under `flink-connector-kafka/src/test/resources/`; add a new fixture when a serializer version changes.
- Test logging is switched off in `flink-connector-kafka/src/test/resources/log4j2-test.properties`; enable it locally when debugging.
- Follow the testing conventions at https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#7-testing

## Commits and PRs

### Commit message format

- `[FLINK-XXXX][component] Description` where FLINK-XXXX is the JIRA issue number
- `[hotfix][component] Description` for typo fixes without JIRA
- Each commit must have a meaningful message including the JIRA ID. If you don't know the ticket number, ask.
- Separate cleanup/refactoring from functional changes into distinct commits
- When AI tools were used: add `Generated-by: <Tool Name and Version>` trailer per [ASF generative tooling guidance](https://www.apache.org/legal/generative-tooling.html)

### Pull request conventions

- Title format: `[FLINK-XXXX][component] Title of the pull request`; backports use `[BP-<major>.<minor>][FLINK-XXXX][component] ...`
- A corresponding JIRA issue is required (except hotfixes for typos)
- Fill out the PR template completely but concisely: describe purpose, change log, testing approach, impact assessment
- Each PR should address exactly one issue
- Ensure `./mvnw clean verify` passes before opening a PR
- Always push to your fork, not directly to `apache/flink-connector-kafka`
- Rebase onto the latest target branch before submitting; the repository only accepts squash and rebase merges
- Branches: `main` is the next major connector version; `v<major>.<minor>` are release branches. Artifacts are versioned `<connector version>-<flink major.minor>`.
- For user-visible behaviour changes, breaking changes, or new options: fill in the **Release Notes** field on the JIRA ticket.

### AI-assisted contributions

- Disclose AI usage by checking the AI disclosure checkbox and filling in the `Generated-by` line in the PR template
- Add `Generated-by: <Tool Name and Version>` to commit messages
- Never add `Co-Authored-By` with an AI agent as co-author; agents are assistants, not authors
- You must be able to explain the design, code, and tests, debug them, and respond to review feedback substantively
- Reviewer-ready quality bar: the author owns PR quality. PRs that look AI-generated without author refinement (walls of unreviewed prose, scaffolding without behaviour, tests that do not exercise the change, padded commit messages) will be closed without review

## Boundaries

### Ask first

- Adding or changing `@Public`, `@PublicEvolving`, or `@Experimental` API, including Table options. Discuss on the JIRA ticket; a new source or sink, or a change of connector-wide semantics, requires a FLIP.
- Changes to checkpointed state or its serializers
- Changes to the exactly-once path (`sink.internal`)
- Bumping `kafka.version`, `flink.version`, or changing the CI matrix
- New dependencies
- Changes on the per-record path (split reader, record emitter, writer, serialization schemas)

### Never

- Commit secrets, credentials, or tokens
- Push directly to `apache/flink-connector-kafka`; always work from your fork
- Mix unrelated changes into one PR
- Use the legacy `SourceFunction` or `SinkFunction` interfaces, or reintroduce `FlinkKafkaConsumer`/`FlinkKafkaProducer`
- Use `@Internal` Flink classes, or Flink API that does not exist in `flink.version`
- Add lines to `flink-connector-kafka/archunit-violations/`, `tools/maven/suppressions.xml`, or the RAT excludes
- Edit `flink-sql-connector-kafka/src/main/resources/META-INF/NOTICE` without a matching dependency change
- Bundle `flink-connector-base` in the SQL jar
- Add `Co-Authored-By` with an AI agent as co-author in commit messages; use `Generated-by: <Tool Name and Version>` instead
- Use destructive git operations unless explicitly requested

## References

- [README.md](README.md) — Build instructions and project overview
- [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) — PR checklist
- [Kafka connector documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/) — User-facing docs
- [Externalized Connector development](https://cwiki.apache.org/confluence/display/FLINK/Externalized+Connector+development) — Versioning, branching, Flink compatibility, and common review issues for connector repositories
- [Code Style Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/) — Detailed coding guidelines
- [Flink Improvement Proposals](https://cwiki.apache.org/confluence/display/FLINK/Flink+Improvement+Proposals) — When a FLIP is required
- [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) — AI tooling policy
