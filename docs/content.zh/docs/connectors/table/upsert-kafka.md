---
title: Upsert Kafka
weight: 4
type: docs
aliases:
  - /zh/dev/table/connectors/upsert-kafka.html
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

# Upsert Kafka SQL 连接器

{{< label "Scan Source: Unbounded" >}}
{{< label "Sink: Streaming Upsert Mode" >}}

Upsert Kafka 连接器支持以 upsert 方式从 Kafka topic 中读取数据并将数据写入 Kafka topic。

作为 source，upsert-kafka 连接器生产 changelog 流，其中每条数据记录代表一个更新或删除事件。更准确地说，数据记录中的 value 被解释为同一 key 的最后一个 value 的 UPDATE，如果有这个 key（如果不存在相应的 key，则该更新被视为 INSERT）。用表来类比，changelog 流中的数据记录被解释为 UPSERT，也称为 INSERT/UPDATE，因为任何具有相同 key 的现有行都被覆盖。另外，value 为空的消息将会被视作为 DELETE 消息。

作为 sink，upsert-kafka 连接器可以消费 changelog 流。它会将 INSERT/UPDATE_AFTER 数据作为正常的 Kafka 消息写入，并将 DELETE 数据以 value 为空的 Kafka 消息写入（表示对应 key 的消息被删除）。Flink 将根据主键列的值对数据进行分区，从而保证主键上的消息有序，因此同一主键上的更新/删除消息将落在同一分区中。

依赖
------------

{{< sql_connector_download_table "kafka" >}}

Upsert Kafka 连接器不是二进制发行版的一部分，请查阅[这里]({{< ref "docs/dev/configuration/overview" >}})了解如何在集群运行中引用 Upsert Kafka 连接器。

完整示例
----------------

下面的示例展示了如何创建和使用 Upsert Kafka 表：

```sql
CREATE TABLE pageviews_per_region (
  user_region STRING,
  pv BIGINT,
  uv BIGINT,
  PRIMARY KEY (user_region) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'pageviews_per_region',
  'properties.bootstrap.servers' = '...',
  'key.format' = 'avro',
  'value.format' = 'avro'
);

CREATE TABLE pageviews (
  user_id BIGINT,
  page_id BIGINT,
  viewtime TIMESTAMP,
  user_region STRING,
  WATERMARK FOR viewtime AS viewtime - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'pageviews',
  'properties.bootstrap.servers' = '...',
  'format' = 'json'
);

-- 计算 pv、uv 并插入到 upsert-kafka sink
INSERT INTO pageviews_per_region
SELECT
  user_region,
  COUNT(*),
  COUNT(DISTINCT user_id)
FROM pageviews
GROUP BY user_region;

```

<span class="label label-danger">注意</span> 确保在 DDL 中定义主键。

Available Metadata
------------------

See the [regular Kafka connector]({{< ref "docs/connectors/table/kafka" >}}#available-metadata) for a list
of all available metadata fields.

连接器参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
      <th class="text-left" style="width: 25%">参数</th>
      <th class="text-center" style="width: 10%">是否必选</th>
      <th class="text-center" style="width: 10%">默认值</th>
      <th class="text-center" style="width: 10%">数据类型</th>
      <th class="text-center" style="width: 50%">描述</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器，Upsert Kafka 连接器使用：<code>'upsert-kafka'</code>。</td>
    </tr>
    <tr>
      <td><h5>topic</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>当表用作 source 时读取数据的 topic 名，或当表用作 sink 时写入的 topic 名。它还支持通过分号分隔的 topic 列表，如 <code>'topic-1;topic-2'</code> 来作为 source 的 topic 列表。注意，“topic-pattern”和“topic”只能指定其中一个。对于 sink 来说，topic 名是写入数据的 topic。它还支持 sink 的 topic 列表。提供的 topic 列表被视为 `topic` 元数据列的有效值的允许列表。如果提供了列表，对于 sink 表，“topic”元数据列是可写的并且必须指定。</td>
    </tr>
    <tr>
      <td><h5>properties.bootstrap.servers</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>以逗号分隔的 Kafka brokers 列表。</td>
    </tr>
    <tr>
      <td><h5>properties.*</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
         该选项可以传递任意的 Kafka 参数。选项的后缀名必须匹配定义在 <a href="https://kafka.apache.org/documentation/#configuration">Kafka 参数文档</a>中的参数名。
         Flink 会自动移除 选项名中的 "properties." 前缀，并将转换后的键名以及值传入 KafkaClient。 例如，你可以通过 <code>'properties.allow.auto.create.topics' = 'false'</code>
         来禁止自动创建 topic。 但是，某些选项，例如<code>'auto.offset.reset'</code> 是不允许通过该方式传递参数，因为 Flink 会重写这些参数的值。
      </td>
    </tr>
    <tr>
      <td><h5>key.format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于对 Kafka 消息中 key 部分序列化和反序列化的格式。key 字段由 PRIMARY KEY 语法指定。支持的格式包括 <code>'csv'</code>、<code>'json'</code>、<code>'avro'</code>。请参考<a href="{{< ref "docs/connectors/table/formats/overview" >}}">格式</a>页面以获取更多详细信息和格式参数。
      </td>
    </tr>
    <tr>
      <td><h5>key.fields-prefix</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Defines a custom prefix for all fields of the key format to avoid name clashes with fields
        of the value format. By default, the prefix is empty. If a custom prefix is defined, both the
        table schema and <code>'key.fields'</code> will work with prefixed names. When constructing the
        data type of the key format, the prefix will be removed and the non-prefixed names will be used
        within the key format. Please note that this option requires that <code>'value.fields-include'</code>
        must be set to <code>'EXCEPT_KEY'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>value.format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于对 Kafka 消息中 value 部分序列化和反序列化的格式。支持的格式包括 <code>'csv'</code>、<code>'json'</code>、<code>'avro'</code>。请参考<a href="{{< ref "docs/connectors/table/formats/overview" >}}">格式</a>页面以获取更多详细信息和格式参数。
      </td>
    </tr>
    <tr>
       <td><h5>value.fields-include</h5></td>
       <td>必选</td>
       <td style="word-wrap: break-word;"><code>'ALL'</code></td>
       <td>String</td>
       <td>控制哪些字段应该出现在 value 中。可取值：
       <ul>
         <li><code>ALL</code>：消息的 value 部分将包含 schema 中所有的字段，包括定义为主键的字段。</li>
         <li><code>EXCEPT_KEY</code>：记录的 value 部分包含 schema 的所有字段，定义为主键的字段除外。</li>
       </ul>
       </td>
    </tr>
    <tr>
      <td><h5>scan.parallelism</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>定义 upsert-kafka source 算子的并行度。默认情况下会使用全局默认并行度。</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>定义 upsert-kafka sink 算子的并行度。默认情况下，由框架确定并行度，与上游链接算子的并行度保持一致。</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>缓存刷新前，最多能缓存多少条记录。当 sink 收到很多同 key 上的更新时，缓存将保留同 key 的最后一条记录，因此 sink 缓存能帮助减少发往 Kafka topic 的数据量，以及避免发送潜在的 tombstone 消息。
      可以通过设置为 '0' 来禁用它。默认，该选项是未开启的。注意，如果要开启 sink 缓存，需要同时设置 <code>'sink.buffer-flush.max-rows'</code>
      和 <code>'sink.buffer-flush.interval'</code> 两个选项为大于零的值。</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Duration</td>
      <td>缓存刷新的间隔时间，超过该时间后异步线程将刷新缓存数据。当 sink 收到很多同 key 上的更新时，缓存将保留同 key 的最后一条记录，因此 sink 缓存能帮助减少发往 Kafka topic 的数据量，以及避免发送潜在的 tombstone 消息。
        可以通过设置为 '0' 来禁用它。默认，该选项是未开启的。注意，如果要开启 sink 缓存，需要同时设置 <code>'sink.buffer-flush.max-rows'</code>
        和 <code>'sink.buffer-flush.interval'</code> 两个选项为大于零的值。</td>
    </tbody>
</table>

特性
----------------

### Key and Value Formats

See the [regular Kafka connector]({{< ref "docs/connectors/datastream/kafka" >}}#key-and-value-formats) for more
explanation around key and value formats. However, note that this connector requires both a key and
value format where the key fields are derived from the `PRIMARY KEY` constraint.

The following example shows how to specify and configure key and value formats. The format options are
prefixed with either the `'key'` or `'value'` plus format identifier.

```sql
CREATE TABLE KafkaTable (
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  ...

  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',

  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'EXCEPT_KEY'
)
```


### 主键约束

Upsert Kafka 始终以 upsert 方式工作，并且需要在 DDL 中定义主键。在具有相同主键值的消息按序存储在同一个分区的前提下，在 changelog source 定义主键意味着 在物化后的 changelog 上主键具有唯一性。定义的主键将决定哪些字段出现在 Kafka 消息的 key 中。

### 一致性保证

默认情况下，如果[启用 checkpoint]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}#enabling-and-configuring-checkpointing)，Upsert Kafka sink 会保证至少一次将数据插入 Kafka topic。

这意味着，Flink 可以将具有相同 key 的重复记录写入 Kafka topic。但由于该连接器以 upsert 的模式工作，该连接器作为 source 读入时，可以确保具有相同主键值下仅最后一条消息会生效。因此，upsert-kafka 连接器可以像 [HBase sink]({{< ref "docs/connectors/table/hbase" >}}) 一样实现幂等写入。

### 为每个分区生成相应的 watermark

Flink 支持根据 Upsert Kafka 的 每个分区的数据特性发送相应的 watermark。当使用这个特性的时候，watermark 是在 Kafka consumer 内部生成的。 合并每个分区
生成的 watermark 的方式和 stream shuffle 的方式是一致的。 数据源产生的 watermark 是取决于该 consumer 负责的所有分区中当前最小的 watermark。如果该
consumer 负责的部分分区是 idle 的，那么整体的 watermark 并不会前进。在这种情况下，可以通过设置合适的 [table.exec.source.idle-timeout]({{< ref "docs/dev/table/config" >}}#table-exec-source-idle-timeout)
来缓解这个问题。

如想获得更多细节，请查阅 [Kafka watermark strategies]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#watermark-strategies-and-the-kafka-connector).

数据类型映射
----------------

Upsert Kafka 用字节存储消息的 key 和 value，因此没有 schema 或数据类型。消息按格式进行序列化和反序列化，例如：csv、json、avro。因此数据类型映射表由指定的格式确定。请参考[格式]({{< ref "docs/connectors/table/formats/overview" >}})页面以获取更多详细信息。

{{< top >}}
