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

import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.examples.scala.complete.game.utils.GameConstants
import org.apache.beam.examples.scala.complete.game.utils.WriteToBigQuery.FieldInfo
import org.apache.beam.examples.scala.complete.game.utils.WriteWindowedToBigQuery
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.metrics.{Counter, Metrics}
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{Combine, DoFn, MapElements, PTransform, ParDo}
import org.apache.beam.sdk.transforms.{Mean, Sum, Values, View}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.Sessions
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.{KV, PCollection, PCollectionView, TypeDescriptors}
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.joda.time.Duration
import org.slf4j.{Logger, LoggerFactory}

/**
  * This class is the fourth in a series of four pipelines that tell a story in a 'gaming' domain,
  * following UserScore, HourlyTeamScore, and LeaderBoard. New concepts:
  * session windows and finding session duration; use of both singleton and non-singleton side
  * inputs.
  *
  * This pipeline builds on the LeaderBoard functionality, and adds some "business
  * intelligence" analysis: abuse detection and usage patterns. The pipeline derives the Mean user
  * score sum for a window, and uses that information to identify likely spammers/robots. (The robots
  * have a higher click rate than the human users). The 'robot' users are then filtered out when
  * calculating the team scores.
  *
  * Additionally, user sessions are tracked: that is, we find bursts of user activity using
  * session windows. Then, the mean session duration information is recorded in the context of
  * subsequent fixed windowing. (This could be used to tell us what games are giving us greater user
  * retention).
  *
  * Run org.apache.beam.examples.complete.game.injector.Injector to generate pubsub data
  * for this pipeline. The Injector documentation provides more detail.
  *
  * The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
  * the same topic to which the Injector is publishing.
  */
object GameStats {
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

    // Read Events from Pub/Sub using custom timestamps
    val rawEvents: PCollection[GameActionInfo] =
      pipeline
        .apply(
          PubsubIO
            .readStrings()
            .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
            .fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))

    // Extract username/score pairs from the event stream
    val userEvents: PCollection[KV[String, Integer]] =
      rawEvents.apply(
        "ExtractUserScore",
        MapElements
          .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
          .via((gInfo: GameActionInfo) => KV.of(gInfo.user, gInfo.score))
      )

    // Calculate the total score per user over fixed windows, and
    // cumulative updates for late data.
    val spammersView: PCollectionView[JMap[String, JInteger]] =
      userEvents
        .apply(
          "FixedWindowsUser",
          Window.into(
            FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration.toLong))))

        // Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
          // These might be robots/spammers.
        .apply("CalculateSpammyUsers", new CalculateSpammyUsers())
        // Derive a view from the collection of spammer users. It will be used as a side input
          // in calculating the team score sums, below.
        .apply("CreateSpammersView", View.asMap())

    // Calculate the total score per team over fixed windows,
    // and emit cumulative updates for late data. Uses the side input derived above-- the set of
    // suspected robots-- to filter out scores from those users from the sum.
    // Write the results to BigQuery.
    rawEvents
      .apply(
        "WindowIntoFixedWindows",
        Window.into(
          FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration.toLong))))
        // Filter out the detected spammer users, using the side input derived above.
      .apply(
        "FilterOutSpammers",
        ParDo
          .of(new FilterOutSpammers(spammersView))
          .withSideInputs(spammersView))
        // Extract and sum teamname/score pairs from the event data.
      .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
        // Write the result to BigQuery
      .apply(
        "WriteTeamSums",
        new WriteWindowedToBigQuery(
          options.as(classOf[GcpOptions]).getProject,
          options.getDataset(),
          options.getGameStatsTablePrefix + "_team",
          configureWindowedWrite().asJava)
      )

    // Detect user sessions-- that is, a burst of activity separated by a gap from further
    // activity. Find and record the mean session lengths.
    // This information could help the game designers track the changing user engagement
    // as their set of games changes.
    userEvents
      .apply(
        "WindowIntoSessions",
        Window
          .into[KV[String, JInteger]](
            Sessions.withGapDuration(Duration.standardMinutes(options.getSessionGap.toLong)))
          .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
      )
      // For this use, we care only about the existence of the session, not any particular
        // information aggregated over it, so the following is an efficient way to do that.
      .apply(Combine.perKey((_: JIterable[JInteger]) => 0))
        // Get the duration per session.
      .apply("UserSessionActivity", ParDo.of(new UserSessionInfoFn()))
        // Re-window to process groups of session sums according to when the sessions complete.
      .apply(
        "WindowToExtractSessionMean",
        Window.into(
          FixedWindows.of(Duration.standardMinutes(options.getUserActivityWindowDuration.toLong))))
        // Find the mean session duration in each window.
      .apply(Mean.globally[JInteger]().withoutDefaults())
        // Write this info to a BigQuery table.
      .apply(
        "WriteAvgSessionLength",
        new WriteWindowedToBigQuery(
          options.as(classOf[GcpOptions]).getProject,
          options.getDataset(),
          options.getGameStatsTablePrefix + "_sessions",
          configureSessionWindowWrite().asJava)
      )

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    val result: PipelineResult = pipeline.run()
    exampleUtils.waitToFinish(result)
  }

  /** Filter out the detected spammer users, using the side input derived above.*/
  class FilterOutSpammers(spammersView: PCollectionView[JMap[String, JInteger]])
      extends DoFn[GameActionInfo, GameActionInfo] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      // If the user is not in the spammers Map, output the data element.
      if (Option(ctx.sideInput(spammersView).get(ctx.element.user.trim)).nonEmpty) {
        ctx.output(ctx.element)
      }
  }

  /** Options supported by GameStats. */
  trait Options extends LeaderBoard.Options {
    @Description("Numeric value of fixed window duration for user analysis, in minutes")
    @Default.Integer(60)
    def getFixedWindowDuration: JInteger
    def setFixedWindowDuration(value: JInteger): Unit

    @Description("Numeric value of gap between user sessions, in minutes")
    @Default.Integer(5)
    def getSessionGap: JInteger
    def setSessionGap(value: JInteger): Unit

    @Description(
      "Numeric value of fixed window for finding mean of user session duration, in minutes")
    @Default.Integer(30)
    def getUserActivityWindowDuration: JInteger
    def setUserActivityWindowDuration(value: JInteger): Unit

    @Description("Prefix used for the BigQuery table names")
    @Default.String("game_stats")
    def getGameStatsTablePrefix: String
    def setGameStatsTablePrefix(value: String): Unit
  }

  /**
    * Filter out all users but those with a high clickrate, which we will consider as 'spammy' users.
    * We do this by finding the mean total score per user, then using that information as a side
    * input to filter out all but those user scores that are larger than (mean *
    * SCORE_WEIGHT).
    */
  class CalculateSpammyUsers
      extends PTransform[PCollection[KV[String, JInteger]], PCollection[KV[String, JInteger]]] {
    import CalculateSpammyUsers.{FilterSpammyUsers, SCORE_WEIGHT}

    override def expand(
        userScores: PCollection[KV[String, Integer]]): PCollection[KV[String, JInteger]] = {
      // Get the sum of scores for each user.
      val sumScores: PCollection[KV[String, JInteger]] =
        userScores.apply("UserSum", Sum.integersPerKey())

      // Extract the score from each element, and use it to find the global mean.
      val globalMeanScore: PCollectionView[JDouble] =
        sumScores.apply(Values.create()).apply(Mean.globally[JInteger]().asSingletonView())

      // Filter the user sums using the global mean.
      val filtered: PCollection[KV[String, JInteger]] =
        sumScores.apply(
          "ProcessAndFilter",
          ParDo
            // use the derived mean total score as a side input
            .of(new FilterSpammyUsers(SCORE_WEIGHT, globalMeanScore))
            .withSideInputs(globalMeanScore)
        )
      filtered
    }
  }

  /** Companion object */
  object CalculateSpammyUsers {
    private val LOG: Logger = LoggerFactory.getLogger(classOf[CalculateSpammyUsers])
    private final val SCORE_WEIGHT: Double = 2.5

    class FilterSpammyUsers(scoreWeight: Double, globalMeanScore: PCollectionView[JDouble])
        extends DoFn[KV[String, JInteger], KV[String, JInteger]] {
      private final val numSpammerUsers: Counter = Metrics.counter("main", "SpammerUsers")

      @ProcessElement
      def processElement(ctx: ProcessContext): Unit = {
        val score: JInteger = ctx.element.getValue
        val gmc: JDouble = ctx.sideInput(globalMeanScore)
        if (score > (gmc * scoreWeight)) {
          LOG.info(s"user ${ctx.element.getKey} spammer score $score with mean $gmc")
          numSpammerUsers.inc()
          ctx.output(ctx.element)
        }
      }
    }
  }

  /** Calculate and output an element's session duration. */
  class UserSessionInfoFn extends DoFn[KV[String, JInteger], JInteger] {
    @ProcessElement
    def processElement(ctx: ProcessContext, boundedWindow: BoundedWindow): Unit = {
      val window = boundedWindow.asInstanceOf[IntervalWindow]
      val duration = new Duration(window.start, window.end)
        .toPeriod()
        .toStandardMinutes()
        .getMinutes
      ctx.output(duration)
    }
  }

  /**
    * Create a map of information that describes how to write pipeline output to BigQuery. This map
    * is used to write information about team score sums.
    */
  def configureWindowedWrite(): Map[String, FieldInfo[KV[String, JInteger]]] =
    LeaderBoard.configureWindowedTableWrite().removed("timing")

  /**
    * Create a map of information that describes how to write pipeline output to BigQuery. This map
    * is used to write information about mean user session time.
    */
  def configureSessionWindowWrite(): Map[String, FieldInfo[JDouble]] =
    Map[String, FieldInfo[JDouble]](
      "window_start" -> new FieldInfo("STRING", (_, boundedWindow) => {
        val window = boundedWindow.asInstanceOf[IntervalWindow]
        GameConstants.DATE_TIME_FORMATTER.print(window.start())
      }),
      "mean_duration" -> new FieldInfo("FLOAT", (ctx, _) => ctx.element)
    )
}
