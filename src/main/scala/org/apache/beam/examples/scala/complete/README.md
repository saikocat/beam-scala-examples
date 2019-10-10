<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with this work
for additional information regarding copyright ownership.  The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License.  You may obtain a copy of
the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
    License for the specific language governing permissions and limitations
    under the License.  -->

# "Complete" Examples

This directory contains end-to-end example pipelines that perform complex data
processing tasks. They include:

* [AutoComplete](AutoComplete.scala) - An example that computes the most popular
  hash tags for every prefix, which can be used for auto-completion.
  Demonstrates how to use the same pipeline in both streaming and batch,
  combiners, and composite transforms.
* [StreamingWordExtract](StreamingWordExtract.scala) - A streaming pipeline
  example that inputs lines of text from a Cloud Pub/Sub topic, splits each line
  into individual words, capitalizes those words, and writes the output to a
  BigQuery table.
* [TfIdf](TfIdf.scala) - An example that computes a basic TF-IDF search table
  for a directory or Cloud Storage prefix. Demonstrates joining data, side
  inputs, and logging.
* [TopWikipediaSessions](TopWikipediaSessions.scala) - An example that reads
  Wikipedia edit data from Cloud Storage and computes the user with the longest
  string of edits separated by no more than an hour within each month.
  Demonstrates using Cloud Dataflow `Windowing` to perform time-based
  aggregations of data.
* [TrafficMaxLaneFlow](TrafficMaxLaneFlow.scala) - A streaming Beam Example
  using BigQuery output in the `traffic sensor` domain. Demonstrates the Cloud
  Dataflow streaming runner, sliding windows, Cloud Pub/Sub topic ingestion, the
  use of the `AvroCoder` to encode a custom class, and custom `Combine`
  transforms.
* [TrafficRoutes](TrafficRoutes.scala) - A streaming Beam Example using BigQuery
  output in the `traffic sensor` domain. Demonstrates the Cloud Dataflow
  streaming runner, `GroupByKey`, keyed state, sliding windows, and Cloud
  Pub/Sub topic ingestion.

See the [documentation](http://beam.apache.org/get-started/quickstart/) and the
[Examples README](../../../../../../../../../README.md) for information about
how to run these examples.
