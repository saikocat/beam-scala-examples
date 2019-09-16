package org.apache.beam.examples.scala.cookbook

import scala.collection.JavaConverters._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{DoFn, Max, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PCollection}

object MaxPerKeyExamples {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private final val WEATHER_SAMPLES_TABLE =
    "clouddataflow-readonly:samples.weather_stations"

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    // Build the table schema for the output table.
    val schema: TableSchema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("month").setType("INTEGER"),
        new TableFieldSchema().setName("max_mean_temp").setType("FLOAT")
      ).asJava
    )

    val pipeline = Pipeline.create(options)
    pipeline
      .apply(BigQueryIO.readTableRows().from(options.getInput))
      .apply(new MaxMeanTemp())
      .apply(
        BigQueryIO
          .writeTableRows()
          .to(options.getOutput)
          .withSchema(schema)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    pipeline.run().waitUntilFinish()
    ()
  }

  trait Options extends PipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    def getInput: String
    def setInput(value: String): Unit

    @Description("Table to write to, specified as <project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String): Unit
  }

  /**
    * Examines each row (weather reading) in the input table. Output the month of the reading, and
    * the mean_temp.
    */
  class ExtractTempFn extends DoFn[TableRow, KV[JInteger, JDouble]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val month: JInteger = Integer.parseInt(row.get("month").toString)
      val meanTemp: JDouble = row.get("mean_temp").toString.toDouble
      ctx.output(KV.of(month, meanTemp))
    }
  }

  /** Format the results to a TableRow, to save to BigQuery. */
  class FormatMaxesFn extends DoFn[KV[JInteger, JDouble], TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row = new TableRow()
        .set("month", ctx.element.getKey)
        .set("max_mean_temp", ctx.element.getValue)
      ctx.output(row)
    }
  }

  /**
    * Reads rows from a weather data table, and finds the max mean_temp for each month via the 'Max'
    * statistical combination function.
    */
  class MaxMeanTemp extends PTransform[PCollection[TableRow], PCollection[TableRow]] {
    override def expand(rows: PCollection[TableRow]): PCollection[TableRow] = {
      // row... => <month, mean_temp> ...
      val temps: PCollection[KV[Integer, JDouble]] = rows.apply(ParDo.of(new ExtractTempFn()))
      // month, mean_temp... => <month, max mean temp>...
      val tempMaxes: PCollection[KV[Integer, JDouble]] = temps.apply(Max.doublesPerKey())
      // <month, max>... => row...
      val results: PCollection[TableRow] = tempMaxes.apply(ParDo.of(new FormatMaxesFn()))
      results
    }
  }

}
