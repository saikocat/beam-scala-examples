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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead
import org.apache.beam.sdk.options.{
  Default,
  Description,
  PipelineOptions,
  PipelineOptionsFactory,
  Validation
}
import org.apache.beam.sdk.transforms.{Count, DoFn, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PCollection}

object BigQueryTornadoes {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private final val WEATHER_SAMPLES_TABLE =
    "clouddataflow-readonly:samples.weather_stations"

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().as(classOf[Options])

    runBigQueryTornadoes(options)
  }

  def runBigQueryTornadoes(options: Options): Unit = {
    val pipeline = Pipeline.create(options)

    // Build the table schema for the output table.
    val fields: List[TableFieldSchema] = List(
      new TableFieldSchema().setName("month").setType("INTEGER"),
      new TableFieldSchema().setName("tornado_count").setType("INTEGER"))
    val schema: TableSchema = new TableSchema().setFields(fields.asJava)

    val rowsFromBigQuery: PCollection[TableRow] = options.getReadMethod match {
      case TypedRead.Method.DIRECT_READ => {
        pipeline.apply(
          BigQueryIO
            .readTableRows()
            .from(options.getInput)
            .withMethod(TypedRead.Method.DIRECT_READ)
            .withSelectedFields(List("month", "tornado").asJava))
      }
      case _ => {
        pipeline.apply(
          BigQueryIO
            .readTableRows()
            .from(options.getInput)
            .withMethod(options.getReadMethod))
      }
    }

    rowsFromBigQuery
      .apply(new CountTornadoes())
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

  // Monkey patching this method since current latest release v2.15.0 doesn't
  // have this method
  implicit class TypeReadWihSelectedFieldsCompat(tr: TypedRead[TableRow]) {
    import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions

    def withSelectedFields(fields: JList[String]): BigQueryIO.TypedRead[TableRow] = {
      val tableReadOptions: TableReadOptions =
        TableReadOptions
          .newBuilder()
          .addAllSelectedFields(fields)
          .build()
      tr.withReadOptions(tableReadOptions)
    }
  }

  /**
    * Options supported by [BigQueryTornadoes].
    *
    * Inherits standard configuration options.
    */
  trait Options extends PipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    def getInput: String
    def setInput(value: String): Unit

    @Description("Mode to use when reading from BigQuery")
    @Default.Enum("EXPORT")
    def getReadMethod(): TypedRead.Method
    def setReadMethod(value: TypedRead.Method): Unit

    @Description(
      "BigQuery table to write to, specified as <project_id>:<dataset_id>.<table_id>. " +
        "The dataset must already exist.")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String): Unit
  }

  /**
    * Examines each row in the input table. If a tornado was recorded in that sample, the month in
    * which it occurred is output.
    */
  class ExtractTornadoesFn extends DoFn[TableRow, JInteger] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      if (row.get("tornado").asInstanceOf[Boolean]) {
        ctx.output(Integer.parseInt(row.get("month").asInstanceOf[String]))
      }
    }
  }

  /**
    * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
    * representation of month and the number of tornadoes that occurred in each month.
    */
  class FormatCountsFn extends DoFn[KV[Integer, JLong], TableRow]() {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row = new TableRow()
        .set("month", ctx.element.getKey)
        .set("tornado_count", ctx.element.getValue)
      ctx.output(row)
    }
  }

  /**
    * Takes rows from a table and generates a table of counts.
    *
    * The input schema is described by https://developers.google.com/bigquery/docs/dataset-gsod .
    * The output contains the total number of tornadoes found in each month in the following schema:
    *
    *  * month: integer
    *  * tornado_count: integer
    */
  class CountTornadoes extends PTransform[PCollection[TableRow], PCollection[TableRow]] {
    override def expand(rows: PCollection[TableRow]): PCollection[TableRow] = {
      // row... => month...
      val tornadoes: PCollection[Integer] = rows.apply(ParDo.of(new ExtractTornadoesFn()))
      // month... => <month,count>...
      val tornadoCounts: PCollection[KV[Integer, JLong]] = tornadoes.apply(Count.perElement())
      // <month,count>... => row...
      tornadoCounts.apply(ParDo.of(new FormatCountsFn()))
    }
  }

}
