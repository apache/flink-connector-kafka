<!--
*Thank you very much for contributing to the Apache Flink Kafka connector - we are happy that you want to help us improve Flink. To help the community review your contribution in the best possible way, please go through the checklist below, which will get the contribution into a shape in which it can be best reviewed.*

*Please understand that we do not do this to make contributions to Flink a hassle. In order to uphold a high standard of quality for code contributions, while at the same time managing a large number of contributions, we need contributors to prepare the contributions well, and give reviewers enough contextual information for the review. Please also understand that contributions that do not follow this guide will take longer to review and thus typically be picked up with lower priority by the community.*

## Contribution Checklist

  - Make sure that the pull request corresponds to a [JIRA issue](https://issues.apache.org/jira/projects/FLINK/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no JIRA issue.

  - Name the pull request in the form "[FLINK-XXXX] [component] Title of the pull request", where *FLINK-XXXX* should be replaced by the actual issue number. Skip *component* if you are unsure about which is the best component.
  Typo fixes that have no associated JIRA issue should be named following this pattern: `[hotfix] [docs] Fix typo in event time introduction` or `[hotfix] [javadocs] Expand JavaDoc for PuncuatedWatermarkGenerator`.

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.

  - Make sure that the change passes the automated tests, i.e., `./mvnw clean verify` passes. GitHub Actions runs the same build for every push and pull request against the Flink versions and JDKs listed in `.github/workflows/push_pr.yml`.

  - Each pull request should address only one issue, not mix up code from multiple issues.

  - Each commit in the pull request has a meaningful commit message (including the JIRA id)

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.


**(The sections below can be removed for hotfixes of typos)**
-->

## What is the purpose of the change

*(For example: This pull request makes the Kafka source commit offsets on checkpoint completion only when the reader still owns the partition. That way we avoid committing stale offsets after a partition has been reassigned.)*


## Brief change log

*(for example:)*
  - *The reader tracks the partitions assigned to it in `KafkaSourceReader`*
  - *`KafkaSourceFetcherManager` skips commits for partitions that are no longer assigned*
  - *A metric counts skipped commits*


## Verifying this change

Please make sure both new and modified tests in this PR follow [the conventions for tests defined in our code quality guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-common/#7-testing).

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added a unit test for the offset commit filter in `KafkaSourceReaderTest`*
  - *Extended `KafkaSourceITCase` with a partition reassignment during a checkpoint*
  - *Manually verified the change by running a job against a 3-broker cluster, moving partitions between readers, and checking the committed offsets in Kafka.*

## Does this pull request potentially affect one of the following parts:

  - Dependencies (does it add or upgrade a dependency, including `kafka.version` or `flink.version`): (yes / no)
  - The public API, i.e., is any changed class annotated with `@Public(Evolving)` or `@Experimental`, or are the Table options or the PyFlink wrappers changed: (yes / no)
  - Checkpointed state, its serializers, or exactly-once delivery (splits, enumerator state, writer state, committables, transactions): (yes / no / don't know)
  - The per-record code paths (split reader, record emitter, writer, serialization schemas; performance sensitive): (yes / no / don't know)

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)
  - If the docs changed, are both `docs/content` and `docs/content.zh` updated? (yes / no / not applicable)

---

##### Was generative AI tooling used to co-author this PR?

<!--
If generative AI tooling has been used in the process of authoring this PR, please
change the checkbox below to `[X]` and replace the placeholder in the "Generated-by"
line with the tool name and version. Otherwise remove the "Generated-by" line.
See the ASF Generative Tooling Guidance for details:
https://www.apache.org/legal/generative-tooling.html

You are responsible for the quality and correctness of every change in this PR
regardless of the tooling used. Low-effort AI-generated PRs will be closed. See
AGENTS.md for the full guidance.
-->

- [ ] Yes (please specify the tool below)

Generated-by: [Tool Name and Version]
