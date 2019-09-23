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

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.gcp.util.Transport
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{
  Count,
  DoFn,
  MapElements,
  PTransform,
  ParDo,
  SerializableComparator,
  SimpleFunction,
  Top
}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{
  BoundedWindow,
  CalendarWindows,
  IntervalWindow,
  Sessions,
  Window
}
import org.apache.beam.sdk.values.{KV, PCollection}
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain
import org.joda.time.{Duration, Instant}

/**
  * An example that reads Wikipedia edit data from Cloud Storage and computes the user with the
  * longest string of edits separated by no more than an hour within each month.
  *
  * Concepts: Using Windowing to perform time-based aggregations of data.
  */
object TopWikipediaSessions {
  private final val EXPORTED_WIKI_TABLE = "gs://apache-beam-samples/wikipedia_edits/*.json"
  private final val SAMPLING_THRESHOLD = 0.1

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    run(options)
  }

  def run(options: Options): Unit = {
    val pipeline = Pipeline.create(options)

    pipeline
      .apply(TextIO.read().from(options.getWikiInput))
      .apply(MapElements.via(new ParseTableRowJson()))
      .apply(new ComputeTopSessions(options.getSamplingThreshold))
      .apply("Write", TextIO.write().to(options.getOutput))

    pipeline.run().waitUntilFinish()
    ()
  }

  trait Options extends PipelineOptions {
    @Description("Input specified as a GCS path containing a BigQuery table exported as json")
    @Default.String(EXPORTED_WIKI_TABLE)
    def getWikiInput: String
    def setWikiInput(value: String): Unit

    @Description("Sampling threshold for number of users")
    @Default.Double(SAMPLING_THRESHOLD)
    def getSamplingThreshold: JDouble
    def setSamplingThreshold(value: JDouble): Unit

    @Description("File to output results to")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String): Unit
  }

  class ParseTableRowJson extends SimpleFunction[String, TableRow] {
    override def apply(input: String): TableRow =
      // whew... do hope they have an example that handles failure with tags
      // instead of a rude runtime exception
      try {
        Transport.getJsonFactory.fromString(input, classOf[TableRow])
      } catch {
        case e: java.io.IOException =>
          throw new RuntimeException("Failed parsing table row json", e)
      }
  }

  /** Extracts user and timestamp from a TableRow representing a Wikipedia edit. */
  class ExtractUserAndTimestamp extends DoFn[TableRow, String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val timestamp: Int = row.get("timestamp") match {
        case t: java.math.BigDecimal => t.intValue()
        case t: JInteger => t.intValue()
      }
      val userName = row.get("contributor_username").asInstanceOf[String]
      if (Option(userName).nonEmpty) {
        // Sets the implicit timestamp field to be used in windowing.
        ctx.outputWithTimestamp(userName, new Instant(timestamp * 1000L))
      }
    }
  }

  /**
    * Computes the number of edits in each user session. A session is defined as a string of edits
    * where each is separated from the next by less than an hour.
    */
  class ComputeSessions extends PTransform[PCollection[String], PCollection[KV[String, JLong]]] {
    override def expand(actions: PCollection[String]): PCollection[KV[String, JLong]] =
      actions
        .apply(Window.into(Sessions.withGapDuration(Duration.standardHours(1))))
        .apply(Count.perElement())
  }

  /** Computes the longest session ending in each month. */
  class TopPerMonth
      extends PTransform[PCollection[KV[String, JLong]], PCollection[JList[KV[String, JLong]]]] {
    override def expand(
        sessions: PCollection[KV[String, JLong]]): PCollection[JList[KV[String, JLong]]] = {
      val comparator = new SerializableComparator[KV[String, JLong]] {
        override def compare(thiz: KV[String, JLong], that: KV[String, JLong]): Int =
          ComparisonChain
            .start()
            .compare(thiz.getValue, that.getValue)
            .compare(thiz.getKey, that.getKey)
            .result()
      }

      sessions
        .apply(Window.into(CalendarWindows.months(1)))
        .apply(
          Top
            .of[KV[String, JLong], SerializableComparator[KV[String, JLong]]](1, comparator)
            .withoutDefaults())
    }
  }

  class SessionsToStringsDoFn extends DoFn[KV[String, JLong], KV[String, JLong]] {
    @ProcessElement
    def processElement(ctx: ProcessContext, window: BoundedWindow): Unit =
      ctx.output(KV.of(s"${ctx.element.getKey} : $window", ctx.element.getValue))
  }

  class FormatOutputDoFn extends DoFn[JList[KV[String, JLong]], String] {
    @ProcessElement
    def processElement(ctx: ProcessContext, window: BoundedWindow): Unit =
      for (item <- ctx.element.asScala) {
        val session = item.getKey
        val count = item.getValue
        val instantTs = window.asInstanceOf[IntervalWindow].start()
        ctx.output(s"$session : $count : $instantTs")
      }
  }

  class ComputeTopSessions(samplingThreshold: Double)
      extends PTransform[PCollection[TableRow], PCollection[String]] {
    override def expand(input: PCollection[TableRow]): PCollection[String] =
      input
        .apply(ParDo.of(new ExtractUserAndTimestamp()))
        .apply("SampleUsers", ParDo.of(new SampleUsersFn(samplingThreshold)))
        .apply(new ComputeSessions())
        .apply("SessionsToStrings", ParDo.of(new SessionsToStringsDoFn()))
        .apply(new TopPerMonth())
        .apply("FormatOutput", ParDo.of(new FormatOutputDoFn()))
  }

  class SampleUsersFn(samplingThreshold: Double) extends DoFn[String, String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      if (Math.abs(ctx.element.hashCode.toLong)
          <= Integer.MAX_VALUE * samplingThreshold) {
        ctx.output(ctx.element)
      }
  }
}
