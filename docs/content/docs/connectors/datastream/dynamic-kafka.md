---
title: Dynamic Kafka
weight: 3
type: docs
aliases:
  - /dev/connectors/dynamic-kafka.html
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

# Dynamic Kafka Source _`Experimental`_

Flink provides an [Apache Kafka](https://kafka.apache.org) connector for reading data from Kafka topics from one or more Kafka clusters. 
The Dynamic Kafka connector discovers the clusters and topics using a Kafka metadata service and can achieve reading in a dynamic fashion, facilitating changes in 
topics and/or clusters, without requiring a job restart. This is especially useful when you need to read a new Kafka cluster/topic and/or stop reading 
an existing Kafka cluster/topic (cluster migration/failover/other infrastructure changes) and when you need direct integration with Hybrid Source. The solution 
makes these operations automated so that they are transparent to Kafka consumers.

## Dependency

For details on Kafka compatibility, please refer to the official [Kafka documentation](https://kafka.apache.org/protocol.html#protocol_compatibility).

{{< connector_artifact flink-connector-kafka kafka >}}

Flink's streaming connectors are not part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

## Dynamic Kafka Source
{{< hint info >}}
This part describes the Dynamic Kafka Source based on the new
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.
{{< /hint >}}

### Usage

Dynamic Kafka Source provides a builder class to initialize the DynamicKafkaSource. The code snippet 
below shows how to build a DynamicKafkaSource to consume messages from the earliest offset of the 
stream "input-stream" and deserialize only the value of the 
ConsumerRecord as a string, using "MyKafkaMetadataService" to resolve the cluster(s) and topic(s)
corresponding to "input-stream".

{{< tabs "DynamicKafkaSource" >}}
{{< tab "Java" >}}
```java

DynamicKafkaSource<String> source = DynamicKafkaSource.<String>builder()
    .setKafkaMetadataService(new MyKafkaMetadataService())
    .setStreamIds(Collections.singleton("input-stream"))
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
    .setProperties(properties)
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Dynamic Kafka Source");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
metadata_service = SingleClusterTopicMetadataService(
    "cluster-a",
    {"bootstrap.servers": "localhost:9092"})

source = DynamicKafkaSource.builder() \
    .set_kafka_metadata_service(metadata_service) \
    .set_stream_ids({"input-stream"}) \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_properties(properties) \
    .build()

env.from_source(source, WatermarkStrategy.no_watermarks(), "Dynamic Kafka Source")
```
{{< /tab >}}
{{< /tabs >}}
The following properties are **required** for building a DynamicKafkaSource:

The Kafka metadata service, configured by setKafkaMetadataService(KafkaMetadataService)
The stream ids to subscribe, see the following Kafka stream subscription section for more details.
Deserializer to parse Kafka messages, see the [Kafka Source Documentation]({{< ref "docs/connectors/datastream/kafka" >}}#deserializer) for more details.

### Offsets Initialization

You can configure starting and stopping offsets globally via the builder. Starting offsets apply to
both bounded and unbounded sources, while stopping offsets only take effect when the source runs in
bounded mode. Cluster metadata may optionally include per-cluster starting or stopping offsets
initializers; if present, they override the global defaults for that cluster.

Example: override offsets for specific clusters via metadata.

{{< tabs "DynamicKafkaSourceOffsets" >}}
{{< tab "Java" >}}
```java
Properties cluster0Props = new Properties();
cluster0Props.setProperty(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "cluster0:9092");
Properties cluster1Props = new Properties();
cluster1Props.setProperty(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "cluster1:9092");

KafkaStream stream =
    new KafkaStream(
        "input-stream",
        Map.of(
            "cluster0",
            new ClusterMetadata(
                Set.of("topic-a"),
                cluster0Props,
                OffsetsInitializer.earliest(),
                OffsetsInitializer.latest()),
            "cluster1",
            new ClusterMetadata(
                Set.of("topic-b"),
                cluster1Props,
                OffsetsInitializer.latest(),
                null)));

DynamicKafkaSource<String> source =
    DynamicKafkaSource.<String>builder()
        .setStreamIds(Set.of(stream.getStreamId()))
        .setKafkaMetadataService(new MockKafkaMetadataService(Set.of(stream)))
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
        // Overridden by per-cluster starting offsets in metadata when present.
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .build();
```
{{< /tab >}}
{{< /tabs >}}

### Kafka Stream Subscription
The Dynamic Kafka Source provides 2 ways of subscribing to Kafka stream(s).
* A set of Kafka stream ids. For example:
  {{< tabs "DynamicKafkaSource#setStreamIds" >}}
  {{< tab "Java" >}}
  ```java
  DynamicKafkaSource.builder().setStreamIds(Set.of("stream-a", "stream-b"));
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  DynamicKafkaSource.builder().set_stream_ids({"stream-a", "stream-b"})
  ```
  {{< /tab >}}
  {{< /tabs >}}
* A regex pattern that subscribes to all Kafka stream ids that match the provided regex. For example:
  {{< tabs "DynamicKafkaSource#setStreamPattern" >}}
  {{< tab "Java" >}}
  ```java
  DynamicKafkaSource.builder().setStreamPattern(Pattern.of("stream.*"));
  ```
  {{< /tab >}}
  {{< tab "Python" >}}
  ```python
  DynamicKafkaSource.builder().set_stream_pattern("stream.*")
  ```
  {{< /tab >}}
  {{< /tabs >}}

### Kafka Metadata Service

An interface is provided to resolve the logical Kafka stream(s) into the corresponding physical 
topic(s) and cluster(s). Typically, these implementations are based on services that align well
with internal Kafka infrastructure--if that is not available, an in-memory implementation 
would also work. An example of in-memory implementation can be found in our tests.

This source achieves its dynamic characteristic by periodically polling this Kafka metadata service
for any changes to the Kafka stream(s) and reconciling the reader tasks to subscribe to the new 
Kafka metadata returned by the service. For example, in the case of a Kafka migration, the source would 
swap from one cluster to the new cluster when the service makes that change in the Kafka stream metadata.

Cluster metadata can optionally carry per-cluster starting and stopping offsets initializers. These
override the global builder configuration for the affected cluster.

Dynamic Kafka Source persists the full per-cluster Kafka `Properties` in enumerator state during
checkpointing. This means custom connection and security settings (for example SASL/SSL-related
properties), not only `bootstrap.servers`, are preserved after restore/failover.

### Additional Properties
There are configuration options in DynamicKafkaSourceOptions that can be configured in the properties through the builder:
<table class="table table-bordered">
    <thead>
      <tr>
      <th class="text-left" style="width: 25%">Option</th>
      <th class="text-center" style="width: 8%">Required</th>
      <th class="text-center" style="width: 7%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 50%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>stream-metadata-discovery-interval-ms</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">-1</td>
      <td>Long</td>
      <td>The interval in milliseconds for the source to discover the changes in stream metadata. A non-positive value disables the stream metadata discovery.</td>
    </tr>
    <tr>
      <td><h5>stream-metadata-discovery-failure-threshold</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">1</td>
      <td>Integer</td>
      <td>The number of consecutive failures before letting the exception from Kafka metadata service discovery trigger jobmanager failure and global failover. The default is one to at least catch startup failures.</td>
    </tr>
    </tbody>
</table>


In addition to this list, see the [regular Kafka connector]({{< ref "docs/connectors/datastream/kafka" >}}#additional-properties) for
a list of applicable properties.

### Metrics

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">Scope</th>
      <th class="text-left" style="width: 18%">Metrics</th>
      <th class="text-left" style="width: 18%">User Variables</th>
      <th class="text-left" style="width: 39%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <th rowspan="8">Operator</th>
        <td>currentEmitEventTimeLag</td>
        <td>n/a</td>
        <td>The time span from the record event timestamp to the time the record is emitted by the source connector¹: <code>currentEmitEventTimeLag = EmitTime - EventTime.</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>watermarkLag</td>
        <td>n/a</td>
        <td>The time span that the watermark lags behind the wall clock time: <code>watermarkLag = CurrentTime - Watermark</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>sourceIdleTime</td>
        <td>n/a</td>
        <td>The time span that the source has not processed any record: <code>sourceIdleTime = CurrentTime - LastRecordProcessTime</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>pendingRecords</td>
        <td>n/a</td>
        <td>The number of records that have not been fetched by the source. e.g. the available records after the consumer offset in a Kafka partition.</td>
        <td>Gauge</td>
    </tr>
    <tr>
      <td>kafkaClustersCount</td>
      <td>n/a</td>
      <td>The total number of Kafka clusters read by this reader.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

In addition to this list, see the [regular Kafka connector]({{< ref "docs/connectors/datastream/kafka" >}}#monitoring) for
the KafkaSourceReader metrics that are also reported.

### Additional Details

For additional details on deserialization, event time and watermarks, idleness, consumer offset 
committing, security, and more, you can refer to the [Kafka Source documentation]({{< ref "docs/connectors/datastream/kafka" >}}#kafka-source). This is possible because the 
Dynamic Kafka Source leverages components of the Kafka Source, and the implementation will be 
discussed in the next section.

### Behind the Scene
{{< hint info >}}
If you are interested in how Kafka source works under the design of new data source API, you may
want to read this part as a reference. For details about the new data source API,
[documentation of data source]({{< ref "docs/dev/datastream/sources.md" >}}) and
<a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface">FLIP-27</a>
provide more descriptive discussions.
{{< /hint >}}


Under the abstraction of the new data source API, Dynamic Kafka Source consists of the following components:
#### Source Split
A source split in Dynamic Kafka Source represents a partition of a Kafka topic, with cluster information. It
consists of:
* A Kafka cluster id that can be resolved by the Kafka metadata service.
* A Kafka Source Split (TopicPartition, starting offset, stopping offset).

You can check the class `DynamicKafkaSourceSplit` for more details.

#### Split Enumerator

This enumerator is responsible for discovering and assigning splits from one or more clusters. At startup, the
enumerator will discover metadata belonging to the Kafka stream ids. Using the metadata, it can 
initialize KafkaSourceEnumerators to handle the functions of assigning splits to the readers. In addition,
source events will be sent to the source reader to reconcile the metadata. This enumerator has the ability to poll the 
KafkaMetadataService, periodically for stream discovery. In addition, restarting enumerators when metadata changes involve 
clearing outdated metrics since clusters may be removed and so should their metrics.

#### Source Reader

This reader is responsible for reading from one or more clusters and using the KafkaSourceReader to fetch 
records from topics and clusters based on the metadata. When new metadata is discovered by the enumerator,
the reader will reconcile metadata changes to possibly restart the KafkaSourceReader to read from the new 
set of topics and clusters.

#### Kafka Metadata Service

This interface represents the source of truth for the current metadata for the configured Kafka stream ids.
Metadata that is removed in between polls is considered non-active (e.g. removing a cluster from the 
return value, means that a cluster is non-active and should not be read from). The cluster metadata 
contains an immutable Kafka cluster id, the set of topics, and properties needed to connect to the
Kafka cluster.

#### FLIP 246

To understand more behind the scenes, please read [FLIP-246](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=217389320)
for more details and discussion.

{{< top >}}
