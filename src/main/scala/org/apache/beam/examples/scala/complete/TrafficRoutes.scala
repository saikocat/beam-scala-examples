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
package org.apache.beam.examples.scala.complete

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashMap

import org.apache.beam.examples.scala.typealias._

/**
  * A Beam Example that runs in both batch and streaming modes with traffic sensor data. You can
  * configure the running mode by setting --streaming to true or false.
  *
  * Concepts: The batch and streaming runners, GroupByKey, sliding windows.
  *
  * This example analyzes traffic sensor data using SlidingWindows. For each window, it calculates
  * the average speed over the window for some small set of predefined 'routes', and looks for
  * 'slowdowns' in those routes. It writes its results to a BigQuery table.
  *
  * The pipeline reads traffic sensor data from --inputFile.
  *
  * The example is configured to use the default BigQuery table from the example common package
  * (there are no defaults for a general Beam pipeline). You can override them by using the
  * --bigQueryDataset, and --bigQueryTable options. If the BigQuery table do not exist,
  * the example will try to create them.
  *
  * The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
  * and then exits.
  */
object TrafficRoutes {

  // Instantiate some small predefined San Diego routes to analyze
  val sdStations: JMap[String, String] = buildStationInfo()
  final val WINDOW_DURATION = 3 // Default sliding window duration in minutes
  final val WINDOW_SLIDE_EVERY = 1 // Default window 'slide every' setting in minutes

  /** Define some small hard-wired San Diego 'routes' to track based on sensor station ID. */
  private def buildStationInfo(): JMap[String, String] = {
    val stations = LinkedHashMap(
      "1108413" -> "SDRoute1", // from freeway 805 S
      "1108699" -> "SDRoute2", // from freeway 78 E
      "1108702" -> "SDRoute2")
    stations.asJava
  }
}
