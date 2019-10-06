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

import java.io.IOException

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.api.services.bigquery.model.TableReference
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.examples.common.{ExampleBigQueryTableOptions, ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{Combine, DoFn, PTransform, ParDo, SerializableFunction}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{SlidingWindows, Window}
import org.apache.beam.sdk.values.{KV, PBegin, PCollection}
import org.joda.time.{Duration, Instant}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * A Beam Example that runs in both batch and streaming modes with traffic sensor data. You can
  * configure the running mode by setting --streaming to true or false.
  *
  * Concepts: The batch and streaming runners, sliding windows, use of the AvroCoder to encode a
  * custom class, and custom Combine transforms.
  *
  * This example analyzes traffic sensor data using SlidingWindows. For each window, it finds the
  * lane that had the highest flow recorded, for each sensor station. It writes those max values
  * along with auxiliary info to a BigQuery table.
  *
  * The pipeline reads traffic sensor data from  --inputFile.
  *
  * The example is configured to use the default BigQuery table from the example common package
  * (there are no defaults for a general Beam pipeline). You can override them by using the
  * --bigQueryDataset, and --bigQueryTable options. If the BigQuery table do not exist,
  * the example will try to create them.
  *
  * The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
  * and then exits.
  */
object TrafficMaxLaneFlow {
  final val WINDOW_DURATION = 60 // Default sliding window duration in minutes
  final val WINDOW_SLIDE_EVERY = 5 // Default window 'slide every' setting in minutes

  /**
    * Sets up and starts streaming pipeline.
    *
    * throws IOException if there is a problem setting up resources
    */
  @throws(classOf[IOException])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[TrafficMaxLaneFlowOptions])
    options.setBigQuerySchema(FormatMaxesFn.getSchema())

    runTrafficMaxLaneFlow(options)
  }

  @throws(classOf[IOException])
  def runTrafficMaxLaneFlow(options: TrafficMaxLaneFlowOptions): Unit = {
    // Using ExampleUtils to set up required resources.
    val exampleUtils = new ExampleUtils(options);
    exampleUtils.setup()

    val pipeline = Pipeline.create(options)
    val tableRef: TableReference = new TableReference()
      .setProjectId(options.getProject)
      .setDatasetId(options.getBigQueryDataset)
      .setTableId(options.getBigQueryTable)

    pipeline
      .apply("ReadLines", new ReadFileAndExtractTimestamps(options.getInputFile))
        // row... => <station route, station speed> ...
      .apply(ParDo.of(new ExtractFlowInfoFn()))
        // map the incoming data stream into sliding windows.
      .apply(
        Window.into(
          SlidingWindows
            .of(Duration.standardMinutes(options.getWindowDuration.toLong))
            .every(Duration.standardMinutes(options.getWindowSlideEvery.toLong))))
      .apply(new MaxLaneFlow())
      .apply(BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatMaxesFn.getSchema()))

    // Run the pipeline.
    val result: PipelineResult = pipeline.run()

    // ExampleUtils will try to cancel the pipeline and the injector before the program exists.
    exampleUtils.waitToFinish(result)
  }

  trait TrafficMaxLaneFlowOptions extends ExampleOptions with ExampleBigQueryTableOptions {
    @Description("Path of the file to read from")
    @Default.String(
      "gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv")
    def getInputFile: String
    def setInputFile(value: String): Unit

    @Description("Numeric value of sliding window duration, in minutes")
    @Default.Integer(WINDOW_DURATION)
    def getWindowDuration: JInteger
    def setWindowDuration(value: JInteger): Unit

    @Description("Numeric value of window 'slide every' setting, in minutes")
    @Default.Integer(WINDOW_SLIDE_EVERY)
    def getWindowSlideEvery: JInteger
    def setWindowSlideEvery(value: JInteger): Unit
  }

  /**
    * This class holds information about each lane in a station reading, along with some general
    * information from the reading.
    */
  @DefaultCoder(classOf[AvroCoder[LaneInfo]])
  case class LaneInfo(
      stationId: String,
      lane: String,
      direction: String,
      freeway: String,
      recordedTimestamp: String,
      laneFlow: JInteger,
      totalFlow: JInteger,
      laneAO: JDouble,
      laneAS: JDouble) {
    def this() {
      this("", "", "", "", "", 0, 0, 0.0, 0.0)
    }
  }

  /** Extract the timestamp field from the input string, and use it as the element timestamp. */
  class ExtractTimestamps extends DoFn[String, String] {
    import ExtractTimestamps.dateTimeFormat

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val items = ctx.element.split(",", -1)

      for {
        timestamp <- Option(items(0))
        timestampMs <- Try(dateTimeFormat.parseMillis(timestamp))
        parsedTimestamp = new Instant(timestampMs)
        if items.length > 0
      } ctx.outputWithTimestamp(ctx.element, parsedTimestamp)
    }
  }

  /** companion object - dateTimeFormat is not serializable if non static */
  object ExtractTimestamps {
    private final val dateTimeFormat: DateTimeFormatter =
      DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
  }

  /**
    * Extract flow information for each of the 8 lanes in a reading, and output as separate tuples.
    * This will let us determine which lane has the max flow for that station over the span of the
    * window, and output not only the max flow from that calculation, but other associated
    * information. The number of lanes for which data is present depends upon which freeway the data
    * point comes from.
    *
    * Timestamp,Station ID,Freeway,Direction of Travel,Station Type,Samples,% Observed,Total Flow,Average Occupancy,Average Speed,
    * Lane 1 Samples,Lane 1 Flow,Lane 1 Average Occupancy,Lane 1 Average Speed,Lane 1 Observed,
    * Lane 2 Samples,Lane 2 Flow,Lane 2 Average Occupancy,Lane 2 Average Speed,Lane 2 Observed,
    * Lane 3 Samples,Lane 3 Flow,Lane 3 Average Occupancy,Lane 3 Average Speed,Lane 3 Observed,
    * Lane 4 Samples,Lane 4 Flow,Lane 4 Average Occupancy,Lane 4 Average Speed,Lane 4 Observed,
    * Lane 5 Samples,Lane 5 Flow,Lane 5 Average Occupancy,Lane 5 Average Speed,Lane 5 Observed,
    * Lane 6 Samples,Lane 6 Flow,Lane 6 Average Occupancy,Lane 6 Average Speed,Lane 6 Observed,
    * Lane 7 Samples,Lane 7 Flow,Lane 7 Average Occupancy,Lane 7 Average Speed,Lane 7 Observed,
    * Lane 8 Samples,Lane 8 Flow,Lane 8 Average Occupancy,Lane 8 Average Speed,Lane 8 Observed
    * 01/01/2010 00:00:00,1100310,5,N,FR,18,0,-1,,,9,,,,0,9,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,,0,,,,,0
    * 01/01/2010 00:00:00,1108148,15,S,ML,54,100,63,.0092,70.9,9,3,.002,76.5,1,9,20,.0167,74,1,9,18,.0163,72,1,9,9,.0077,69.2,1,9,7,.0068,64.5,1,9,6,.0056,64.5,1,,,,,0,,,,,0
    */
  class ExtractFlowInfoFn extends DoFn[String, KV[String, LaneInfo]] {
    private val INVALID_INPUT_LENGTH = 48
    private val NUM_OF_LANES = 8

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val items = ctx.element.split(",", -1);
      items.length match {
        case length if length < INVALID_INPUT_LENGTH => ()
        case _ => {
          // extract the sensor information for the lanes from the input string fields.
          extractLaneInfo(items).foreach(laneInfo =>
            ctx.output(KV.of(laneInfo.stationId, laneInfo)))
        }
      }
    }

    def extractLaneInfo(items: Array[String]): Iterator[LaneInfo] = {
      val timestamp = items(0)
      val stationId = items(1)
      val freeway = items(2)
      val direction = items(3)
      val totalFlow = Try(items(7).toInt).toOption.getOrElse(-1)
      for {
        i <- (1 to NUM_OF_LANES).iterator
        laneFlow = Try(items(6 + 5 * i).toInt).toOption
        laneAvgOccupancy = Try(items(7 + 5 * i).toDouble).toOption
        laneAvgSpeed = Try(items(8 + 5 * i).toDouble).toOption
        if laneFlow.nonEmpty && laneAvgOccupancy.nonEmpty && laneAvgSpeed.nonEmpty
      } yield {
        new LaneInfo(
          stationId,
          s"lane $i",
          direction,
          freeway,
          timestamp,
          laneFlow.get,
          totalFlow,
          laneAvgOccupancy.get,
          laneAvgSpeed.get)
      }
    }
  }

  /**
    * A custom 'combine function' used with the Combine.perKey transform. Used to find the max lane
    * flow over all the data points in the Window. Extracts the lane flow from the input string and
    * determines whether it's the max seen so far. We're using a custom combiner instead of the Max
    * transform because we want to retain the additional information we've associated with the flow
    * value.
    */
  class MaxFlow extends SerializableFunction[JIterable[LaneInfo], LaneInfo] {
    override def apply(input: JIterable[LaneInfo]): LaneInfo =
      input.asScala.reduce((thiz, that) => if (that.laneFlow >= thiz.laneFlow) that else thiz)
  }

  /**
    * Format the results of the Max Lane flow calculation to a TableRow, to save to BigQuery. Add the
    * timestamp from the window context.
    */
  class FormatMaxesFn extends DoFn[KV[String, LaneInfo], TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val laneInfo: LaneInfo = ctx.element.getValue
      val row: TableRow =
        new TableRow()
          .set("station_id", ctx.element.getKey)
          .set("direction", laneInfo.direction)
          .set("freeway", laneInfo.freeway)
          .set("lane_max_flow", laneInfo.laneFlow)
          .set("lane", laneInfo.lane)
          .set("avg_occ", laneInfo.laneAO)
          .set("avg_speed", laneInfo.laneAS)
          .set("total_flow", laneInfo.totalFlow)
          .set("recorded_timestamp", laneInfo.recordedTimestamp)
          .set("window_timestamp", ctx.timestamp.toString)
      ctx.output(row)
    }
  }

  object FormatMaxesFn {

    /** Defines the BigQuery schema used for the output. */
    def getSchema(): TableSchema = {
      val fields: List[TableFieldSchema] = List(
        new TableFieldSchema().setName("station_id").setType("STRING"),
        new TableFieldSchema().setName("direction").setType("STRING"),
        new TableFieldSchema().setName("freeway").setType("STRING"),
        new TableFieldSchema().setName("lane_max_flow").setType("INTEGER"),
        new TableFieldSchema().setName("lane").setType("STRING"),
        new TableFieldSchema().setName("avg_occ").setType("FLOAT"),
        new TableFieldSchema().setName("avg_speed").setType("FLOAT"),
        new TableFieldSchema().setName("total_flow").setType("INTEGER"),
        new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"),
        new TableFieldSchema().setName("recorded_timestamp").setType("STRING")
      )
      val schema: TableSchema = new TableSchema().setFields(fields.asJava)
      schema
    }
  }

  /**
    * This PTransform extracts lane info, calculates the max lane flow found for a given station (for
    * the current Window) using a custom 'combiner', and formats the results for BigQuery.
    */
  class MaxLaneFlow extends PTransform[PCollection[KV[String, LaneInfo]], PCollection[TableRow]] {
    override def expand(flowInfo: PCollection[KV[String, LaneInfo]]): PCollection[TableRow] = {
      // stationId, LaneInfo => stationId + max lane flow info
      val flowMaxes: PCollection[KV[String, LaneInfo]] =
        flowInfo.apply(Combine.perKey(new MaxFlow()))
      // <stationId, max lane flow info>... => row...
      val results: PCollection[TableRow] = flowMaxes.apply(ParDo.of(new FormatMaxesFn()))
      results
    }
  }

  class ReadFileAndExtractTimestamps(inputFile: String)
      extends PTransform[PBegin, PCollection[String]] {
    override def expand(begin: PBegin): PCollection[String] =
      begin
        .apply(TextIO.read().from(inputFile))
        .apply(ParDo.of(new ExtractTimestamps()))
  }
}
