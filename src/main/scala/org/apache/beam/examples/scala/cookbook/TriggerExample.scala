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
package org.apache.beam.examples.scala.cookbook

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.Random

import com.google.api.services.bigquery.model.{
  TableFieldSchema,
  TableReference,
  TableRow,
  TableSchema
}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.transforms.{DoFn, GroupByKey, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.{KV, PCollection}
import org.joda.time.{Duration, Instant}

object TriggerExample {
  // Numeric value of fixed window duration, in minutes
  final val WINDOW_DURATION = 30
  // Constants used in triggers.
  // Speeding up ONE_MINUTE or FIVE_MINUTES helps you get an early approximation of results.
  // ONE_MINUTE is used only with processing time before the end of the window
  final val ONE_MINUTE: Duration = Duration.standardMinutes(1)
  // FIVE_MINUTES is used only with processing time after the end of the window
  final val FIVE_MINUTES: Duration = Duration.standardMinutes(5)
  // ONE_DAY is used to specify the amount of lateness allowed for the data elements.
  final val ONE_DAY: Duration = Duration.standardDays(1)

  /**
    * Calculate total flow and number of records for each freeway and format the results to TableRow
    * objects, to save to BigQuery.
    */
  class TotalFlow(triggerType: String)
      extends PTransform[PCollection[KV[String, JInteger]], PCollection[TableRow]] {
    override def expand(flowInfo: PCollection[KV[String, JInteger]]): PCollection[TableRow] = {
      val flowPerFreeway: PCollection[KV[String, JIterable[JInteger]]] =
        flowInfo.apply(GroupByKey.create())
      val results: PCollection[KV[String, String]] =
        flowPerFreeway.apply(ParDo.of(new SumCountRecordFn()))
      val output: PCollection[TableRow] =
        results.apply(ParDo.of(new FormatTotalFlowFn(triggerType)))
      output
    }
  }

  // helper fn instead of anon fn to make linter happy
  class SumCountRecordFn extends DoFn[KV[String, JIterable[Integer]], KV[String, String]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val flows: JIterable[Integer] = ctx.element.getValue
      val (sum, numberOfRecords) = flows.asScala.foldLeft((0, 0L)) { (acc, value) =>
        (acc._1 + value, acc._2 + 1)
      }
      ctx.output(KV.of(ctx.element.getKey, s"$sum, $numberOfRecords"))
    }
  }

  /**
    * Format the results of the Total flow calculation to a TableRow, to save to BigQuery. Adds the
    * triggerType, pane information, processing time and the window timestamp.
    */
  class FormatTotalFlowFn(triggerType: String) extends DoFn[KV[String, String], TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext, window: BoundedWindow): Unit = {
      val values = ctx.element.getValue.split(",", -1)
      // Exception fest if unable to parse int or long
      val row: TableRow =
        new TableRow()
          .set("trigger_type", triggerType)
          .set("freeway", ctx.element.getKey)
          .set("total_flow", Integer.parseInt(values(0)))
          .set("number_of_records", Long2long(values(1).toLong))
          .set("window", window.toString)
          .set("isFirst", ctx.pane.isFirst)
          .set("isLast", ctx.pane.isLast)
          .set("timing", ctx.pane.getTiming.toString)
          .set("event_time", ctx.timestamp.toString)
          .set("processing_time", Instant.now().toString)
      ctx.output(row)
    }
  }

  /**
    * Extract the freeway and total flow in a reading. Freeway is used as key since we are
    * calculating the total flow for each freeway.
    */
  class ExtractFlowInfoFn extends DoFn[String, KV[String, JInteger]] {
    private final val VALID_NUM_FIELDS = 50

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val laneInfo = ctx.element.split(",", -1)

      for {
        totalFlow <- scala.util.Try(laneInfo(7).toInt)
        freeway = laneInfo(2)
        // Skip the invalid input.
        if laneInfo.length >= VALID_NUM_FIELDS
        // Header row
        if laneInfo(0) != "timestamp"
        // Ignore the records with total flow 0 to easily understand the working of triggers.
        // Skip the records with total flow -1 since they are invalid input.
        if totalFlow > 0
      } ctx.output(KV.of(freeway, totalFlow))
    }
  }

  /** Add current time to each record. Also insert a delay at random to demo the triggers. */
  class InsertDelaysFn extends DoFn[String, String] {
    private final val THRESHOLD = 0.001
    // MIN_DELAY and MAX_DELAY in minutes.
    private final val MIN_DELAY = 1
    private final val MAX_DELAY = 100

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val random = new Random()
      val now: Instant = Instant.now()
      val timestamp: Instant = (random.nextDouble < THRESHOLD) match {
        case false => now
        case true => {
          val range = MAX_DELAY - MIN_DELAY
          val delayInMinutes = random.nextInt(range) + MIN_DELAY
          val delayInMillis = TimeUnit.MINUTES.toMillis(delayInMinutes.toLong)
          new Instant(now.getMillis - delayInMillis)
        }
      }
      ctx.outputWithTimestamp(ctx.element, timestamp)
    }
  }

  /** Sets the table reference. */
  def getTableReference(project: String, dataset: String, table: String): TableReference =
    new TableReference()
      .setProjectId(project)
      .setDatasetId(dataset)
      .setTableId(table)

  /** Defines the BigQuery schema used for the output. */
  def getSchema(): TableSchema = {
    val fields: List[TableFieldSchema] = List(
      new TableFieldSchema().setName("trigger_type").setType("STRING"),
      new TableFieldSchema().setName("freeway").setType("STRING"),
      new TableFieldSchema().setName("total_flow").setType("INTEGER"),
      new TableFieldSchema().setName("number_of_records").setType("INTEGER"),
      new TableFieldSchema().setName("window").setType("STRING"),
      new TableFieldSchema().setName("isFirst").setType("BOOLEAN"),
      new TableFieldSchema().setName("isLast").setType("BOOLEAN"),
      new TableFieldSchema().setName("timing").setType("STRING"),
      new TableFieldSchema().setName("event_time").setType("TIMESTAMP"),
      new TableFieldSchema().setName("processing_time").setType("TIMESTAMP")
    )
    new TableSchema().setFields(fields.asJava)
  }
}
