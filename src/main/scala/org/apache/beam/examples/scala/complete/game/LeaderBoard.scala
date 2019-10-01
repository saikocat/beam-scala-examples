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

import org.apache.beam.examples.scala.complete.game.utils.WriteToBigQuery.FieldInfo
import org.apache.beam.examples.scala.complete.game.utils.GameConstants
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.KV
import org.joda.time.{Duration, Instant}

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

  /**
    * Create a map of information that describes how to write pipeline output to BigQuery. This map
    * is used to write team score sums and includes event timing information.
    */
  def configureWindowedTableWrite(): Map[String, FieldInfo[KV[String, JInteger]]] =
    Map[String, FieldInfo[KV[String, JInteger]]](
      "team" -> new FieldInfo("STRING", (ctx, _) => ctx.element.getKey),
      "total_score" -> new FieldInfo("INTEGER", (ctx, _) => ctx.element.getValue),
      "window_start" -> new FieldInfo("STRING", (_, boundedWindow) => {
        val window = boundedWindow.asInstanceOf[IntervalWindow]
        GameConstants.DATE_TIME_FORMATTER.print(window.start())
      }),
      "processing_time" -> new FieldInfo(
        "STRING",
        (_, _) => GameConstants.DATE_TIME_FORMATTER.print(Instant.now())),
      "timing" -> new FieldInfo("STRING", (ctx, _) => ctx.pane.getTiming.toString)
    )

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is passed to the WriteToBigQuery constructor to write user score sums.
   */
  def configureBigQueryWrite(): Map[String, FieldInfo[KV[String, JInteger]]] =
    Map[String, FieldInfo[KV[String, JInteger]]](
      "user" -> new FieldInfo("STRING", (ctx, _) => ctx.element.getKey),
      "total_score" -> new FieldInfo("INTEGER", (ctx, _) => ctx.element.getValue)
    )
}
