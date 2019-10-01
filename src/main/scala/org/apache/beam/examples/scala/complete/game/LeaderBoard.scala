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
package org.apache.beam.examples.scala.complete.game

/**
  * This class is the third in a series of four pipelines that tell a story in a 'gaming' domain,
  * following UserScore and HourlyTeamScore. Concepts include: processing unbounded
  * data using fixed windows; use of custom timestamps and event-time processing; generation of
  * early/speculative results; using .accumulatingFiredPanes() to do cumulative processing of late-
  * arriving data.
  *
  * <p>This pipeline processes an unbounded stream of 'game events'. The calculation of the team
  * scores uses fixed windowing based on event time (the time of the game play event), not processing
  * time (the time that an event is processed by the pipeline). The pipeline calculates the sum of
  * scores per team, for each window. By default, the team scores are calculated using one-hour
  * windows.
  *
  * In contrast-- to demo another windowing option-- the user scores are calculated using a global
  * window, which periodically (every ten minutes) emits cumulative user score sums.
  *
  * In contrast to the previous pipelines in the series, which used static, finite input data,
  * here we're using an unbounded data source, which lets us provide speculative results, and allows
  * handling of late data, at much lower latency. We can use the early/speculative results to keep a
  * 'leaderboard' updated in near-realtime. Our handling of late data lets us generate correct
  * results, e.g. for 'team prizes'. We're now outputting window results as they're calculated,
  * giving us much lower latency than with the previous batch examples.
  *
  * Run injector.Injector to generate pubsub data for this pipeline. The Injector
  * documentation provides more detail on how to do this.
  *
  * The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
  * the same topic to which the Injector is publishing.
  */
object LeaderBoard {
  final val FIVE_MINUTES: Duration = Duration.standardMinutes(5)
  final val TEN_MINUTES: Duration = Duration.standardMinutes(10)
}
