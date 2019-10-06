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

import java.util.TimeZone

import scala.jdk.CollectionConverters._

import org.apache.beam.examples.scala.complete.game.utils.{GameConstants, WriteToText}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{Filter, ParDo, ProcessFunction, WithTimestamps}
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, IntervalWindow, Window}
import org.apache.beam.sdk.values.KV
import org.joda.time.{DateTimeZone, Duration, Instant}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * This class is the second in a series of four pipelines that tell a story in a 'gaming' domain,
  * following UserScore. In addition to the concepts introduced in UserScore, new
  * concepts include: windowing and element timestamps; use of Filter.by().
  *
  * This pipeline processes data collected from gaming events in batch, building on
  * UserScore} but using fixed windows. It calculates the sum of scores per team, for each window,
  * optionally allowing specification of two timestamps before and after which data is filtered out.
  * This allows a model where late data collected after the intended analysis window can be included,
  * and any late-arriving data prior to the beginning of the analysis window can be removed as well.
  * By using windowing and adding element timestamps, we can do finer-grained analysis than with the
  * UserScore pipeline. However, our batch processing is high-latency, in that we don't get
  * results from plays at the beginning of the batch's time period until the batch is processed.
  */
object HourlyTeamScore {

  import UserScore.{ExtractAndSumScore, GameActionInfo, ParseEventFn}

  private val minFmt: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MM-dd-HH-mm")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(GameConstants.TIMEZONE)))

  /** Run a batch pipeline to do windowed analysis of the data. */
  def main(args: Array[String]): Unit = {
    // Begin constructing a pipeline configured by commandline flags.
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])
    val pipeline: Pipeline = Pipeline.create(options)

    val stopMinTimestamp: Instant = new Instant(minFmt.parseMillis(options.getStopMin))
    val startMinTimestamp: Instant = new Instant(minFmt.parseMillis(options.getStartMin))

    // Read 'gaming' events from a text file.
    pipeline
      .apply(TextIO.read().from(options.getInput))
        // Parse the incoming data.
      .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
      // Filter out data before and after the given times so that it is not included
      // in the calculations. As we collect data in batches (say, by day), the batch for the day
      // that we want to analyze could potentially include some late-arriving data from the
      // previous day.
      // If so, we want to weed it out. Similarly, if we include data from the following day
      // (to scoop up late-arriving events from the day we're analyzing), we need to weed out
        // events that fall after the time period we want to analyze.
      .apply(
        "FilterStartTime",
        Filter.by(new ProcessFunction[GameActionInfo, JBoolean] {
          override def apply(gInfo: GameActionInfo): JBoolean =
            gInfo.timestamp > startMinTimestamp.getMillis
        })
      )
      .apply(
        "FilterEndTime",
        Filter.by(new ProcessFunction[GameActionInfo, JBoolean] {
          override def apply(gInfo: GameActionInfo): JBoolean =
            gInfo.timestamp < stopMinTimestamp.getMillis
        })
      )
        // Add an element timestamp based on the event log, and apply fixed windowing.
      .apply(
        "AddEventTimestamps",
        WithTimestamps.of((gInfo: GameActionInfo) => new Instant(gInfo.timestamp)))
      .apply(
        "FixedWindowsTeam",
        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowDuration.toLong))))
        // Extract and sum teamname/score pairs from the event data.
      .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
      .apply(
        "WriteTeamScoreSums",
        new WriteToText(options.getOutput, configureOutput().asJava, true))

    pipeline.run().waitUntilFinish()
    ()
  }

  /** Options supported by HourlyTeamScore. */
  trait Options extends UserScore.Options {

    @Description("Numeric value of fixed window duration, in minutes")
    @Default.Integer(60)
    def getWindowDuration: JInteger
    def setWindowDuration(value: JInteger): Unit

    @Description(
      "String representation of the first minute after which to generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped prior to that minute won't be included in the sums.")
    @Default.String("1970-01-01-00-00")
    def getStartMin: String
    def setStartMin(value: String): Unit

    @Description(
      "String representation of the first minute for which to not generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped after that minute won't be included in the sums.")
    @Default.String("2100-01-01-00-00")
    def getStopMin: String
    def setStopMin(value: String): Unit
  }

  /**
    * Create a map of information that describes how to write pipeline output to text. This map is
    * passed to the WriteToText constructor to write team score sums and includes information
    * about window start time.
    */
  def configureOutput(): Map[String, WriteToText.FieldFn[KV[String, JInteger]]] =
    UserScore
      .configureOutput()
      .concat(
        Map[String, WriteToText.FieldFn[KV[String, JInteger]]](
          "window_start" -> { (_, boundedWindow) =>
            {
              val window = boundedWindow.asInstanceOf[IntervalWindow]
              GameConstants.DATE_TIME_FORMATTER.print(window.start())
            }
          }
        ))
}
