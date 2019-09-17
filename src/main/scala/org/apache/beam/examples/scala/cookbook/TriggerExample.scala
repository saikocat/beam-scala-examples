package org.apache.beam.examples.scala.cookbook

import scala.collection.JavaConverters._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableReference, TableSchema}
import org.joda.time.Duration

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
