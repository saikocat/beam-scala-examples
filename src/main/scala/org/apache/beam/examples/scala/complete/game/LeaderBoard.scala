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

import scala.jdk.CollectionConverters._

import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.examples.scala.complete.game.utils.GameConstants
import org.apache.beam.examples.scala.complete.game.utils.WriteToBigQuery
import org.apache.beam.examples.scala.complete.game.utils.WriteToBigQuery.FieldInfo
import org.apache.beam.examples.scala.complete.game.utils.WriteWindowedToBigQuery
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.{ParDo, PTransform}
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.{KV, PCollection}
import org.apache.beam.sdk.{Pipeline, PipelineResult}
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

  import UserScore.{ExtractAndSumScore, GameActionInfo, ParseEventFn}

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true)
    val exampleUtils = new ExampleUtils(options)
    val pipeline: Pipeline = Pipeline.create(options)

    // Read game events from Pub/Sub using custom timestamps, which are extracted from the pubsub
    // data elements, and parse the data.
    val gameEvents: PCollection[GameActionInfo] =
      pipeline
        .apply(
          PubsubIO
            .readStrings()
            .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
            .fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))

    gameEvents
      .apply(
        "CalculateTeamScores",
        new CalculateTeamScores(
          Duration.standardMinutes(options.getTeamWindowDuration.toLong),
          Duration.standardMinutes(options.getAllowedLateness().toLong))
      )
        // Write the results to BigQuery.
      .apply(
        "WriteTeamScoreSums",
        new WriteWindowedToBigQuery(
          options.as(classOf[GcpOptions]).getProject,
          options.getDataset(),
          options.getLeaderBoardTableName() + "_team",
          configureWindowedTableWrite().asJava)
      )
    gameEvents
      .apply(
        "CalculateUserScores",
        new CalculateUserScores(Duration.standardMinutes(options.getAllowedLateness().toLong)))
        // Write the results to BigQuery.
      .apply(
        "WriteUserScoreSums",
        new WriteToBigQuery(
          options.as(classOf[GcpOptions]).getProject,
          options.getDataset(),
          options.getLeaderBoardTableName() + "_user",
          configureGlobalWindowBigQueryWrite().asJava)
      )

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    val result: PipelineResult = pipeline.run()
    exampleUtils.waitToFinish(result)
  }

  /** Options supported by LeaderBoard. */
  trait Options extends ExampleOptions with StreamingOptions {

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    def getDataset(): String
    def setDataset(value: String): Unit

    @Description("Pub/Sub topic to read from")
    @Validation.Required
    def getTopic(): String
    def setTopic(value: String): Unit

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(60)
    def getTeamWindowDuration: JInteger
    def setTeamWindowDuration(value: JInteger): Unit

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(120)
    def getAllowedLateness(): JInteger
    def setAllowedLateness(value: JInteger): Unit

    @Description("Prefix used for the BigQuery table names")
    @Default.String("leaderboard")
    def getLeaderBoardTableName(): String
    def setLeaderBoardTableName(value: String): Unit
  }

  /**
    * Calculates scores for each team within the configured window duration.
    * Extract team/score pairs from the event stream, using hour-long windows by default.
    */
  class CalculateTeamScores(teamWindowDuration: Duration, allowedLateness: Duration)
      extends PTransform[PCollection[GameActionInfo], PCollection[KV[String, JInteger]]] {
    override def expand(infos: PCollection[GameActionInfo]): PCollection[KV[String, JInteger]] =
      infos
        .apply(
          "LeaderboardTeamFixedWindows",
          Window
            .into[GameActionInfo](FixedWindows.of(teamWindowDuration))
              // We will get early (speculative) results as well as cumulative processing of late data.
            .triggering(trigger)
            .withAllowedLateness(allowedLateness)
            .accumulatingFiredPanes()
        )
          // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new ExtractAndSumScore("team"))

    val trigger: AfterWatermarkEarlyAndLate =
      AfterWatermark
        .pastEndOfWindow()
        .withEarlyFirings(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(FIVE_MINUTES))
        .withLateFirings(
          AfterProcessingTime
            .pastFirstElementInPane()
            .plusDelayOf(TEN_MINUTES))
  }

  /**
    * Extract user/score pairs from the event stream using processing time, via global windowing. Get
    * periodic updates on all users' running scores.
    */
  class CalculateUserScores(allowedLateness: Duration)
      extends PTransform[PCollection[GameActionInfo], PCollection[KV[String, JInteger]]] {

    override def expand(input: PCollection[GameActionInfo]): PCollection[KV[String, JInteger]] =
      input
        .apply(
          "LeaderboardUserGlobalWindow",
          Window
            .into[GameActionInfo](new GlobalWindows())
              // Get periodic results every ten minutes.
            .triggering(Repeatedly.forever(
              AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_MINUTES)))
            .accumulatingFiredPanes()
            .withAllowedLateness(allowedLateness)
        )
          // Extract and sum username/score pairs from the event data.
        .apply("ExtractUserScore", new ExtractAndSumScore("user"))
  }

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

  /**
    * Create a map of information that describes how to write pipeline output to BigQuery. This map
    * is used to write user score sums.
    */
  def configureGlobalWindowBigQueryWrite(): Map[String, FieldInfo[KV[String, JInteger]]] =
    Map[String, FieldInfo[KV[String, JInteger]]](
      "processing_time" -> new FieldInfo(
        "STRING",
        (_, _) => GameConstants.DATE_TIME_FORMATTER.print(Instant.now()))
    )
}
