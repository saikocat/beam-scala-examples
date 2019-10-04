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
package org.apache.beam.examples.scala.complete.game.utils

import java.io.Serializable

import scala.collection.JavaConverters._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.google.api.services.bigquery.model.TableReference
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.{PCollection, PDone}

/**
  * Generate, format, and write BigQuery table row information. Use provided information about the
  * field names and types, as well as lambda functions that describe how to generate their values.
  */
class WriteToBigQuery[InputT](
    projectId: String,
    datasetId: String,
    tableName: String,
    fieldInfo: JMap[String, WriteToBigQuery.FieldInfo[InputT]])
    extends PTransform[PCollection[InputT], PDone] {

  import WriteToBigQuery.FieldFn

  /** Convert each key/score pair into a BigQuery TableRow as specified by fieldFn. */
  protected class BuildRowFn extends DoFn[InputT, TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext, window: BoundedWindow): Unit = {
      val row: TableRow = fieldInfo.asScala
        .foldLeft(new TableRow()) {
          case (row, (key, fieldData)) =>
            val fieldFn: FieldFn[InputT] = fieldData.fieldFn
            row.set(key, fieldFn.apply(ctx, window))
        }
      ctx.output(row)
    }
  }

  /** Build the output table schema. */
  protected def getSchema(): TableSchema = {
    val fields: Iterable[TableFieldSchema] = fieldInfo.asScala
      .map {
        case (key, fieldData) => {
          val bqType: String = fieldData.fieldType
          new TableFieldSchema().setName(key).setType(bqType)
        }
      }
    new TableSchema().setFields(fields.toIndexedSeq.asJava)
  }

  override def expand(teamAndScore: PCollection[InputT]): PDone = {
    teamAndScore
      .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
      .apply(
        BigQueryIO
          .writeTableRows()
          .to(getTable(projectId, datasetId, tableName))
          .withSchema(getSchema())
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND))
    PDone.in(teamAndScore.getPipeline())
  }

  /** Utility to construct an output table reference. */
  def getTable(projectId: String, datasetId: String, tableName: String): TableReference =
    new TableReference()
      .setDatasetId(datasetId)
      .setProjectId(projectId)
      .setTableId(tableName)
}

/** companion object */
object WriteToBigQuery {

  /**
    * A Serializable function from a DoFn.ProcessContext and BoundedWindow to
    * the value for that field.
    */
  trait FieldFn[InputT] extends Serializable {
    def apply(context: DoFn[InputT, TableRow]#ProcessContext, window: BoundedWindow): Object
  }

  /** Define a class to hold information about output table field definitions. */
  // The BigQuery 'type' of the field
  // A lambda function to generate the field value
  case class FieldInfo[InputT](fieldType: String, fieldFn: FieldFn[InputT]) extends Serializable
}
