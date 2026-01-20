---
title: Dynamic Kafka
weight: 5
type: docs
aliases:
  - /dev/table/connectors/dynamic-kafka.html
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

# Dynamic Kafka SQL 连接器

{{< label "Scan Source: Unbounded" >}}

Dynamic Kafka 连接器允许从可跨集群变更的 Kafka 主题读取数据，无需重启作业。
流的解析由 Kafka 元数据服务完成，尤其适用于集群迁移和动态主题/集群变更场景。

依赖
----

{{< sql_connector_download_table "kafka" >}}

Kafka 连接器不包含在二进制发行版中。
请参考 [这里]({{< ref "docs/dev/configuration/overview" >}}) 了解如何在集群执行时引入。

如何创建 Dynamic Kafka 表
------------------------

下面示例使用内置的 `single-cluster` 元数据服务创建 Dynamic Kafka 表。
在该模式下，stream id 会被解释为单集群中的 Kafka 主题。

```sql
CREATE TABLE DynamicKafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'dynamic-kafka',
  'stream-ids' = 'user_behavior;user_behavior_v2',
  'metadata-service' = 'single-cluster',
  'metadata-service.cluster-id' = 'cluster-0',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
)
```

该连接器也支持自定义元数据服务，通过 `metadata-service` 指定。服务类需实现
`KafkaMetadataService`，并且需要提供无参构造函数或接收 `Properties` 参数的构造函数。
连接器会将所有 `properties.*` 选项作为 Kafka 属性传入构造函数。

可用元数据
--------

Dynamic Kafka 连接器暴露的元数据列与 Kafka 连接器一致。
请参考 [Kafka SQL 连接器]({{< ref "docs/connectors/table/kafka" >}}#available-metadata)。

连接器参数
---------

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">参数</th>
      <th class="text-center" style="width: 8%">必需</th>
      <th class="text-center" style="width: 8%">转发</th>
      <th class="text-center" style="width: 7%">默认值</th>
      <th class="text-center" style="width: 10%">类型</th>
      <th class="text-center" style="width: 42%">说明</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必需</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>指定要使用的连接器，Dynamic Kafka 使用 <code>'dynamic-kafka'</code>。</td>
    </tr>
    <tr>
      <td><h5>stream-ids</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>以分号分隔的 stream id 列表。<code>stream-ids</code> 与 <code>stream-pattern</code> 只能设置其一。</td>
    </tr>
    <tr>
      <td><h5>stream-pattern</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>stream id 的正则表达式。<code>stream-ids</code> 与 <code>stream-pattern</code> 只能设置其一。</td>
    </tr>
    <tr>
      <td><h5>metadata-service</h5></td>
      <td>必需</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>元数据服务标识。可使用 <code>'single-cluster'</code> 或实现 <code>KafkaMetadataService</code> 的全限定类名。</td>
    </tr>
    <tr>
      <td><h5>metadata-service.cluster-id</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td><code>single-cluster</code> 元数据服务所需的集群 ID。</td>
    </tr>
    <tr>
      <td><h5>stream-metadata-discovery-interval-ms</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">-1</td>
      <td>Long</td>
      <td>发现 stream 元数据变更的周期（毫秒）。非正值表示关闭发现。</td>
    </tr>
    <tr>
      <td><h5>stream-metadata-discovery-failure-threshold</h5></td>
      <td>可选</td>
      <td>否</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>连续发现失败次数阈值，超过将触发作业失败。</td>
    </tr>
    </tbody>
</table>

Dynamic Kafka 连接器同样支持 Kafka 连接器的格式参数与 Kafka 客户端参数。
完整列表请参考 [Kafka SQL 连接器]({{< ref "docs/connectors/table/kafka" >}}#connector-options)。
