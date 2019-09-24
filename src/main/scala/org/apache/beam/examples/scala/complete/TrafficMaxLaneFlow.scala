package org.apache.beam.examples.scala.complete

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.transforms.{DoFn, SerializableFunction}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.KV
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
}
