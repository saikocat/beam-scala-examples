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
import scala.collection.mutable
import scala.util.Try

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.{DoFn, GroupByKey, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PBegin, PCollection}
import org.joda.time.Instant
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

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

  /** This class holds information about a station reading's average speed. */
  @DefaultCoder(classOf[AvroCoder[StationSpeed]])
  case class StationSpeed(stationId: String, avgSpeed: JDouble, timestamp: JLong)
      extends Ordered[StationSpeed] {
    def this() {
      this("", 0.0, 0L)
    }

    override def compare(that: StationSpeed): Int = timestamp.compareTo(that.timestamp)

    override def equals(that: Any): Boolean =
      that match {
        case that: StationSpeed =>
          that.isInstanceOf[StationSpeed] && timestamp == that.timestamp
        case _ => false
      }

    override def hashCode: Int = timestamp.hashCode
  }

  /** This class holds information about a route's speed/slowdown. */
  @DefaultCoder(classOf[AvroCoder[RouteInfo]])
  case class RouteInfo(route: String, avgSpeed: JDouble, slowdownEvent: JBoolean) {
    def this() {
      this("", 0.0, false)
    }
  }

  /** Extract the timestamp field from the input string, and use it as the element timestamp. */
  class ExtractTimestamps extends DoFn[String, String] {
    private final val dateTimeFormat: DateTimeFormatter =
      DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val items = ctx.element.split(",")

      for {
        timestamp <- Try(items(0))
        timestampMs <- Try(dateTimeFormat.parseMillis(timestamp))
        parsedTimestamp = new Instant(timestampMs)
        if items.length > 0
      } ctx.outputWithTimestamp(ctx.element, parsedTimestamp)
    }
  }

  /**
    * Filter out readings for the stations along predefined 'routes', and output (station, speed
    * info) keyed on route.
    */
  class ExtractStationSpeedFn extends DoFn[String, KV[String, StationSpeed]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val items = ctx.element.split(",")

      for {
        stationType <- Try(items(4))
        avgSpeed <- Try(items(9).toDouble)
        stationId <- Try(items(1))
        if "ML" == stationType && sdStations.containsKey(stationId)
      } {
        val stationSpeed = StationSpeed(stationId, avgSpeed, ctx.timestamp.getMillis)
        // The tuple key is the 'route' name stored in the 'sdStations' hash.
        val outputValue: KV[String, StationSpeed] = KV.of(sdStations.get(stationId), stationSpeed)
        ctx.output(outputValue)
      }
    }
  }

  /**
    * For a given route, track average speed for the window. Calculate whether traffic is currently
    * slowing down, via a predefined threshold. If a supermajority of speeds in this sliding window
    * are less than the previous reading we call this a 'slowdown'. Note: these calculations are for
    * example purposes only, and are unrealistic and oversimplified.
    */
  class GatherStats extends DoFn[KV[String, JIterable[StationSpeed]], KV[String, RouteInfo]] {
    case class StationSpeedStats(
        route: String,
        speedSum: Double = 0.0,
        speedCount: Int = 0,
        speedups: Int = 0,
        slowdowns: Int = 0) {
      def speedAvg: Double = speedSum / speedCount
      def slowdownEvent: Boolean = slowdowns >= 2 * speedups
      def routeInfo: RouteInfo = new RouteInfo(route, speedAvg, slowdownEvent)
    }

    @ProcessElement
    def processElement(ctx: ProcessContext) = {
      val route: String = ctx.element.getKey
      val prevSpeeds = new mutable.HashMap[String, Double]
      val infoList: Seq[StationSpeed] =
        ctx.element.getValue.asScala.toSeq.filter(item => Option(item.avgSpeed).nonEmpty).sorted

      val stats = infoList.foldLeft(StationSpeedStats(route = route))(
        (acc: StationSpeedStats, item: StationSpeed) => {
          val speed = item.avgSpeed
          val (speedups, slowdowns) = prevSpeeds.get(item.stationId) match {
            case Some(lastSpeed) => if (lastSpeed < speed) (1, 0) else (0, 1)
            case None => {
              prevSpeeds(item.stationId) = speed
              (0, 0)
            }
          }

          acc.copy(
            speedSum = acc.speedSum + speed.toDouble,
            speedCount = acc.speedCount + 1,
            speedups = acc.speedups + speedups,
            slowdowns = acc.slowdowns + slowdowns
          )
        })

      stats.speedCount match {
        case 0 => ()
        case _ => ctx.output(KV.of(stats.route, stats.routeInfo))
      }
    }
  }

  /** Format the results of the slowdown calculations to a TableRow, to save to BigQuery. */
  class FormatStatsFn extends DoFn[KV[String, RouteInfo], TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val routeInfo: RouteInfo = ctx.element.getValue
      val row: TableRow =
        new TableRow()
          .set("avg_speed", routeInfo.avgSpeed)
          .set("slowdown_event", routeInfo.slowdownEvent)
          .set("route", ctx.element.getKey)
          .set("window_timestamp", ctx.timestamp.toString)
      ctx.output(row)
    }
  }

  /** Defines the BigQuery schema used for the output. */
  object FormatStatsFn {
    def getSchema(): TableSchema = {
      val fields: List[TableFieldSchema] = List(
        new TableFieldSchema().setName("route").setType("STRING"),
        new TableFieldSchema().setName("avg_speed").setType("FLOAT"),
        new TableFieldSchema().setName("slowdown_event").setType("BOOLEAN"),
        new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP")
      )
      new TableSchema().setFields(fields.asJava)
    }
  }

  /**
    * This PTransform extracts speed info from traffic station readings. It groups the readings by
    * 'route' and analyzes traffic slowdown for that route. Lastly, it formats the results for
    * BigQuery.
    */
  class TrackSpeed
      extends PTransform[PCollection[KV[String, StationSpeed]], PCollection[TableRow]] {
    override def expand(
        stationSpeed: PCollection[KV[String, StationSpeed]]): PCollection[TableRow] = {
      // Apply a GroupByKey transform to collect a list of all station
      // readings for a given route.
      val timeGroup: PCollection[KV[String, JIterable[StationSpeed]]] =
        stationSpeed.apply(GroupByKey.create())

      // Analyze 'slowdown' over the route readings.
      val stats: PCollection[KV[String, RouteInfo]] = timeGroup.apply(ParDo.of(new GatherStats()))

      // Format the results for writing to BigQuery
      val results: PCollection[TableRow] = stats.apply(ParDo.of(new FormatStatsFn()))
      results
    }
  }

  class ReadFileAndExtractTimestamps(inputFile: String)
      extends PTransform[PBegin, PCollection[String]] {
    override def expand(begin: PBegin): PCollection[String] =
      begin.apply(TextIO.read().from(inputFile)).apply(ParDo.of(new ExtractTimestamps()))
  }

  /** Define some small hard-wired San Diego 'routes' to track based on sensor station ID. */
  private def buildStationInfo(): JMap[String, String] = {
    val stations = mutable.LinkedHashMap(
      "1108413" -> "SDRoute1", // from freeway 805 S
      "1108699" -> "SDRoute2", // from freeway 78 E
      "1108702" -> "SDRoute2")
    stations.asJava
  }
}
