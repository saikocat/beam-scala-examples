package org.apache.beam.examples.scala.complete

import scala.util.Try

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.joda.time.Instant
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
      this("", "", "", "", "", -1, -1, -1.0, -1.0)
    }
  }

  /** Extract the timestamp field from the input string, and use it as the element timestamp. */
  class ExtractTimestamps extends DoFn[String, String] {
    private final val dateTimeFormat: DateTimeFormatter =
      DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")

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
}
