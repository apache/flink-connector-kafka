---
title: Kafka
weight: 3
type: docs
aliases:
  - /zh/dev/connectors/dynamic-kafka.html
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

Flink provides an [Apache Kafka](https://kafka.apache.org) connector for reading data from and 
writing data to Kafka topics from one or more Kafka clusters. This connector achieves this in a dynamic 
fashion, without requiring a job restart, using a Kafka metadata service to facilitate changes in 
topics and/or clusters. This is especially useful in transparent Kafka cluster addition/removal without 
Flink job restart, transparent Kafka topic addition/removal without Flink job restart, and direct integration
with Hybrid Source.

## Dependency

For details on Kafka compatibility, please refer to the official [Kafka documentation](https://kafka.apache.org/protocol.html#protocol_compatibility).

{{< connector_artifact flink-connector-kafka 3.1.0 >}}

Flink's streaming connectors are not part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

## Dynamic Kafka Source
{{< hint info >}}
This part describes the Dynamic Kafka Source based on the new
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.
{{< /hint >}}

## Usage

Dynamic Kafka Source provides a builder class to initialize the DynamicKafkaSource. The code snippet 
below shows how to build a DynamicKafkaSource to consume messages from the earliest offset of the 
stream "input-stream" and deserialize only the value of the 
ConsumerRecord as a string, using "MyKafkaMetadataService" to resolve the cluster(s) and topic(s)
corresponding to "input-stream".

{{< tabs "KafkaSource" >}}
{{< tab "Java" >}}
```java

DynamicKafkaSource<String> source = DynamicKafkaSource.<String>builder()
    .setKafkaMetadataService(new MyKafkaMetadataService())
    .setStreamIds(Collections.singleton("input-stream"))
    .setStartingOffsets(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
    .setDeserializer(new SimpleStringSchema())
    .setProperties(properties)
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
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

### Additional Details

For additional details on deserialization, event time and watermarks, idleness, consumer offset 
committing, security, and more, you can refer to the Kafka Source documentation. This is possible because the 
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

This enumerator is responsible for discovering and assigning splits from 1+ cluster. At startup, the
enumerator will discover metadata belonging to the Kafka stream ids. Using the metadata, it can 
initialize KafkaSourceEnumerators to handle the functions of assigning splits to the readers. In addition,
source events will be sent to the source reader to reconcile the metadata. This enumerator has the ability to poll the 
KafkaMetadataService, periodically for stream discovery. In addition, restarting enumerators when metadata changes involve 
clearing outdated metrics since clusters may be removed and so should their metrics.

#### Source Reader

This reader is responsible for reading from 1+ clusters and using the KafkaSourceReader to fetch 
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
