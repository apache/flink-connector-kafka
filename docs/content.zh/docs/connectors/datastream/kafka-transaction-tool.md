---
title: Kafka 事务工具
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

# Kafka 事务工具

Kafka 事务工具可以提交或中止 Flink 精确一次 Kafka sink 遗留的事务。
这类事务可能使配置了 `isolation.level=read_committed` 的消费者无法越过最后稳定偏移量。
该工具是独立的 Java 应用程序，无需运行中的 Flink 集群。

{{< hint warning >}}
仅在生产者作业已经停止且不会重启时使用此工具。如果可以通过 Flink 正常恢复作业，应由 Flink 处理事务。
提交事务会使其记录对 `read_committed` 消费者可见；中止事务会永久排除这些记录。
对错误的事务执行任一操作都可能破坏与 Flink checkpoint 的一致性。
{{< /hint >}}

## 获取工具

可执行构件的 Maven 坐标为 `org.apache.flink:flink-connector-kafka-transaction-tool`，
classifier 为 `uber-jar`。如果所选连接器发行版已将此构件发布到 Maven Central，
请将 `<version>` 替换为该发行版的版本号，然后使用 Maven 下载：

```bash
mvn dependency:copy \
  '-Dartifact=org.apache.flink:flink-connector-kafka-transaction-tool:<version>:jar:uber-jar' \
  -DoutputDirectory=.
```

下载的文件名为 `flink-connector-kafka-transaction-tool-<version>-uber-jar.jar`。
普通 JAR 不包含运行所需的依赖；较旧的连接器发行版不一定包含该工具。
对于尚未发布的版本，请按以下说明从源码构建。

要构建开发版本，请在包含 `flink-connector-kafka-transaction-tool` 模块的 Kafka 连接器源码根目录执行：

```bash
./mvnw clean package -pl flink-connector-kafka-transaction-tool -am -DskipTests
```

生成的可执行 JAR 位于 `flink-connector-kafka-transaction-tool/target/`，文件名为
`flink-connector-kafka-transaction-tool-<version>-uber-jar.jar`。`-am` 同时构建所需的 reactor 模块。
请使用所构建连接器版本支持的 Java 运行环境。以下命令中的 `TOOL_JAR` 应设置为该可执行 JAR 的路径。
先查看帮助：

```bash
java -jar "$TOOL_JAR" --help
```

## 配置 Kafka 访问

运行工具的环境必须能够连接 broker，并具有管理目标事务所需的认证信息和权限。
中止操作还会通过 Kafka Admin API 检查事务 ID 是否存在；客户端需要该事务 ID 的 `Describe` 权限。
通过 `--command-config client.properties` 提供 Kafka 客户端属性。
例如，使用 TLS 和 PEM 格式信任库的集群可以配置：

```properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/ca.pem
max.block.ms=60000
```

请按集群要求添加 SSL 客户端认证或 SASL 配置，例如 SASL 认证所需的 `sasl.mechanism` 和
`sasl.jaas.config`。限制对包含凭据的文件的访问。未指定 `--command-config` 时，使用 Kafka 的默认配置。

工具先加载文件属性，再用命令行指定的 bootstrap servers 和事务 ID 覆盖文件中的
`bootstrap.servers` 和 `transactional.id`。工具始终为 `key.serializer` 和 `value.serializer`
使用 `ByteArraySerializer`，覆盖文件中的对应配置，其他 producer 属性保持不变。
可以在文件中设置 `max.block.ms`，以控制事务操作和中止前存在性检查的阻塞时间上限；未设置时使用 Kafka 客户端默认值。
该上限不同于 `transaction.timeout.ms` 控制的 broker 事务过期时间。

## 处理遗留事务

### 1. 停止作业并确定原始事务

确认生产者作业已停止，并禁用自动重启，包括外部调度器或 operator 发起的重启。
确保在人工处理期间没有其他生产者复用该事务 ID。

结合 Flink checkpoint/savepoint 信息和作业日志，确定准确的事务 ID，以及待处理记录对应的 checkpoint。
若要提交事务，还必须从 Flink 状态或日志中获取该原始事务记录的 producer ID 和 epoch。
该工具不会从 checkpoint 中提取这些值，也不提供事务列表功能。

Kafka 管理工具可以辅助检查事务状态，但作业重启后，broker 当前报告的 producer ID 和 epoch
可能属于较新的事务。不要用这些值替代原始事务的元数据。
如果无法确定事务与相关 Flink checkpoint 的对应关系，请勿手动提交。

### 2. 选择与恢复计划一致的结果

仅当记录属于已完成的 checkpoint 或 savepoint，且需要保留其输出时，才应提交事务，
同时确认后续恢复作业将如何处理这些记录。提交尚未完成 checkpoint 的事务，
可能使 Flink 恢复时重新处理的记录提前可见，从而产生重复数据。

仅当恢复计划明确要求丢弃事务中的记录时，才应中止事务。
如果 Flink 已经将对应的输入进度保存到 checkpoint，从该状态恢复时不会重新处理这些记录，
因此中止事务可能导致数据丢失。中止操作会隔离给定事务 ID 的前一个生产者，而不是选择某个旧 epoch。
重启后复用该 ID 的作业也会受到影响。

### 3. 执行所选操作

将以下示例值替换为已核实的事务信息。恢复并提交原始事务：

```bash
java -jar "$TOOL_JAR" \
  --action commit \
  --bootstrap-servers broker1:9093 \
  --transactional-id flink-tx-1 \
  --producer-id 1005 \
  --epoch 4 \
  --command-config client.properties
```

或者，隔离生产者并中止其未完成的事务：

```bash
java -jar "$TOOL_JAR" \
  --action abort \
  --bootstrap-servers broker1:9093 \
  --transactional-id flink-tx-1 \
  --command-config client.properties
```

只执行所选的一种操作。提交操作必须指定 `--producer-id` 和 `--epoch`。
每个选项只能出现一次，且不接受位置参数。

中止前，工具会检查集群是否已知该事务 ID。对于未知 ID，工具会拒绝操作，不会隔离或初始化生产者。
如果较旧的 broker 不支持此检查，工具会警告无法核实 ID，然后继续隔离生产者。
此时必须仔细核实 ID：隔离未知 ID 的生产者可能只创建一个空事务条目，而没有解决目标事务。
检查中的其他错误会在隔离生产者之前终止操作。

### 4. 处理不确定的结果

提交超时或被中断时，工具返回退出码 `3`，表示结果**未知**：Kafka 可能已经接受或完成提交。
保持作业停止，检查 Kafka 事务状态和 broker 日志。如果需要重试，只能针对同一集群，
使用**相同的事务 ID、producer ID 和 epoch** 重试 `commit`。
提交结果不确定时，绝不能改为执行 `abort`。非零退出码并不能证明事务没有提交。

工具使用以下退出码：

| 退出码 | 含义 |
| --- | --- |
| `0` | 所选操作成功完成，或已显示帮助。 |
| `1` | 命令行参数解析失败。 |
| `2` | 配置、验证或其他操作错误。确定下一步操作前，应检查错误和 broker 状态。 |
| `3` | 提交超时或被中断，结果未知。不要中止事务。 |

### 5. 重启前验证

使用 Kafka 管理工具和 broker 日志确认目标事务的最终结果。
检查受影响的 `read_committed` 消费者是否继续推进，以及预期记录是否在提交后可见，或在中止后被排除。
消费者恢复推进本身不能证明处理的是正确的事务。记录事务标识、所选操作及其结果，
然后确认计划使用的 Flink 恢复点与该结果一致，再允许作业重启。
