/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.scala.subprocess

/**
  * In this example batch pipeline we will invoke a simple Echo C++ library within a DoFn The sample
  * makes use of a ExternalLibraryDoFn class which abstracts the setup and processing of the
  * executable, logs and results. For this example we are using commands passed to the library based
  * on ordinal position but for a production system you should use a mechanism like ProtoBuffers with
  * Base64 encoding to pass the parameters to the library To test this example you will need to build
  * the files Echo.cc and EchoAgain.cc in a linux env matching the runner that you are using (using
  * g++ with static option). Once built copy them to the SourcePath defined in SubProcessPipelineOptions
  */
object ExampleEchoPipeline {}
