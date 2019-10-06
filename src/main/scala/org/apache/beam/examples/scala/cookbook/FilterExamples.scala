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

import scala.jdk.CollectionConverters._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{DoFn, Mean, PTransform, ParDo, View}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{PCollection, PCollectionView}
import org.slf4j.LoggerFactory

object FilterExamples {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private final val WEATHER_SAMPLES_TABLE =
    "clouddataflow-readonly:samples.weather_stations"
  final val LOG = LoggerFactory.getLogger(getClass.getName)
  final val MONTH_TO_FILTER = 7

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline = Pipeline.create(options)
    val schema: TableSchema = buildWeatherSchemaProjection()

    pipeline
      .apply(BigQueryIO.readTableRows().from(options.getInput))
      .apply(ParDo.of(new ProjectionFn()))
      .apply(new BelowGlobalMean(options.getMonthFilter))
      .apply(
        BigQueryIO
          .writeTableRows()
          .to(options.getOutput)
          .withSchema(schema)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

    pipeline.run().waitUntilFinish()
    ()
  }

  trait Options extends PipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    def getInput: String
    def setInput(value: String): Unit

    @Description(
      "Table to write to, specified as <project_id>:<dataset_id>.<table_id>. " +
        "The dataset_id must already exist")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String): Unit

    @Description("Numeric value of month to filter on")
    @Default.Integer(MONTH_TO_FILTER)
    def getMonthFilter: JInteger
    def setMonthFilter(value: JInteger): Unit
  }

  /**
    * Examines each row in the input table. Outputs only the subset of the cells this example is
    * interested in-- the mean_temp and year, month, and day-- as a bigquery table row.
    */
  class ProjectionFn extends DoFn[TableRow, TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      // Grab year, month, day, mean_temp from the row
      val year = Integer.parseInt(row.get("year").toString)
      val month = Integer.parseInt(row.get("month").toString)
      val day = Integer.parseInt(row.get("day").toString)
      val meanTemp = row.get("mean_temp").toString.toDouble
      // Prepares the data for writing to BigQuery by building a TableRow object
      val outRow: TableRow = new TableRow()
        .set("year", year)
        .set("month", month)
        .set("day", day)
        .set("mean_temp", meanTemp)
      ctx.output(outRow)
    }
  }

  /**
    * Implements 'filter' functionality.
    *
    * Examines each row in the input table. Outputs only rows from the month monthFilter, which is
    * passed in as a parameter during construction of this DoFn.
    */
  class FilterSingleMonthDataFn(monthFilter: JInteger) extends DoFn[TableRow, TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val month = Integer.parseInt(row.get("month").toString)
      if (month.equals(this.monthFilter)) {
        ctx.output(row)
      }
    }
  }

  /**
    * Examines each row (weather reading) in the input table. Output the temperature reading for that
    * row ('mean_temp').
    */
  class ExtractTempFn extends DoFn[TableRow, JDouble] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val meanTemp: JDouble = row.get("mean_temp").toString.toDouble
      ctx.output(meanTemp)
    }
  }

  /**
    * Finds the global mean of the mean_temp for each day/record, and outputs only data that has a
    * mean temp larger than this global mean.
    */
  class BelowGlobalMean(monthFilter: JInteger)
      extends PTransform[PCollection[TableRow], PCollection[TableRow]] {
    override def expand(rows: PCollection[TableRow]): PCollection[TableRow] = {

      // Extract the mean_temp from each row.
      val meanTemps: PCollection[JDouble] = rows.apply(ParDo.of(new ExtractTempFn()))

      // Find the global mean, of all the mean_temp readings in the weather data,
      // and prepare this singleton PCollectionView for use as a side input.
      val globalMeanTemp: PCollectionView[JDouble] =
        meanTemps.apply(Mean.globally()).apply(View.asSingleton())

      // Rows filtered to remove all but a single month
      val monthFilteredRows: PCollection[TableRow] =
        rows.apply(ParDo.of(new FilterSingleMonthDataFn(this.monthFilter)))

      // Then, use the global mean as a side input, to further filter the weather data.
      // By using a side input to pass in the filtering criteria, we can use a value
      // that is computed earlier in pipeline execution.
      // We'll only output readings with temperatures below this mean.
      val filteredRows: PCollection[TableRow] = monthFilteredRows.apply(
        "ParseAndFilter",
        ParDo
          .of(new ParseAndFilter(globalMeanTemp))
          .withSideInputs(globalMeanTemp)
      )

      filteredRows
    }
  }

  // Added this class else scalac complained about unused method in anon
  class ParseAndFilter(globalMeanTemp: PCollectionView[JDouble]) extends DoFn[TableRow, TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val meanTemp: JDouble = ctx.element.get("mean_temp").toString.toDouble
      val gTemp: JDouble = ctx.sideInput(globalMeanTemp)
      if (meanTemp < gTemp) {
        ctx.output(ctx.element)
      }
    }
  }

  /** Helper method to build the table schema for the output table. */
  private def buildWeatherSchemaProjection(): TableSchema =
    new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("year").setType("INTEGER"),
        new TableFieldSchema().setName("month").setType("INTEGER"),
        new TableFieldSchema().setName("day").setType("INTEGER"),
        new TableFieldSchema().setName("mean_temp").setType("FLOAT")
      ).asJava
    )
}
