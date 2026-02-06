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

# Dynamic Kafka SQL Connector

{{< label "Scan Source: Unbounded" >}}

The Dynamic Kafka connector allows for reading data from Kafka topics that can move across
clusters without restarting the job. Streams are resolved via a Kafka metadata service. This is
especially useful for cluster migrations and dynamic topic/cluster changes.

Dependencies
------------

{{< sql_connector_download_table "kafka" >}}

The Kafka connector is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

How to create a Dynamic Kafka table
-----------------------------------

The example below shows how to create a Dynamic Kafka table using the built-in
`single-cluster` metadata service. With this service, stream ids are interpreted as topics
in a single Kafka cluster.

```sql
CREATE TABLE DynamicKafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `kafka_cluster` STRING METADATA FROM 'kafka_cluster',
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

The connector also supports a custom metadata service via the `metadata-service` option. The
service class must implement `KafkaMetadataService` and should either have a public no-arg
constructor or a constructor that accepts `Properties`. The connector will pass the Kafka
properties (all `properties.*` options) into the constructor when available.

Available Metadata
------------------

The Dynamic Kafka connector exposes all metadata columns from the Kafka connector and adds one
dynamic-source-specific metadata column:

* `kafka_cluster` (`STRING NOT NULL`, read-only): cluster id resolved by the metadata service for
  the record.

See [Kafka SQL Connector]({{< ref "docs/connectors/table/kafka" >}}#available-metadata) for the
shared metadata columns.

Example:

```sql
CREATE TABLE DynamicKafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `kafka_cluster` STRING METADATA FROM 'kafka_cluster' VIRTUAL
) WITH (
  'connector' = 'dynamic-kafka',
  'stream-ids' = 'user_behavior;user_behavior_v2',
  'metadata-service' = 'single-cluster',
  'metadata-service.cluster-id' = 'cluster-0',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);
```

Connector Options
----------------

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Option</th>
      <th class="text-center" style="width: 8%">Required</th>
      <th class="text-center" style="width: 8%">Forwarded</th>
      <th class="text-center" style="width: 7%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 42%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, for Dynamic Kafka use <code>'dynamic-kafka'</code>.</td>
    </tr>
    <tr>
      <td><h5>stream-ids</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Semicolon-separated stream ids to subscribe. Only one of <code>stream-ids</code> and <code>stream-pattern</code> can be set.</td>
    </tr>
    <tr>
      <td><h5>stream-pattern</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Regex pattern for stream ids to subscribe. Only one of <code>stream-ids</code> and <code>stream-pattern</code> can be set.</td>
    </tr>
    <tr>
      <td><h5>metadata-service</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Metadata service identifier. Use <code>'single-cluster'</code> or a fully qualified class name implementing <code>KafkaMetadataService</code>.</td>
    </tr>
    <tr>
      <td><h5>metadata-service.cluster-id</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Cluster id required by <code>single-cluster</code> metadata service.</td>
    </tr>
    <tr>
      <td><h5>stream-metadata-discovery-interval-ms</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">-1</td>
      <td>Long</td>
      <td>Interval in milliseconds for discovering stream metadata changes. A non-positive value disables discovery.</td>
    </tr>
    <tr>
      <td><h5>stream-metadata-discovery-failure-threshold</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>Number of consecutive discovery failures before failing the job.</td>
    </tr>
    </tbody>
</table>

The connector also supports the same format options and Kafka client properties as the Kafka
connector. See [Kafka SQL Connector]({{< ref "docs/connectors/table/kafka" >}}#connector-options)
for the full list of format options and Kafka properties (all <code>properties.*</code> options).
