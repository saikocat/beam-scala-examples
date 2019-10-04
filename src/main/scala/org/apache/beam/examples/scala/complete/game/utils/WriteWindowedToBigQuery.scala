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

import scala.collection.JavaConverters._

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.{PCollection, PDone}

/**
  * Generate, format, and write BigQuery table row information. Subclasses WriteToBigQuery to
  * require windowing; so this subclass may be used for writes that require access to the context's
  * window information.
  */
class WriteWindowedToBigQuery[T](
    projectId: String,
    datasetId: String,
    tableName: String,
    fieldInfo: JMap[String, WriteToBigQuery.FieldInfo[T]])
    extends WriteToBigQuery[T](projectId, datasetId, tableName, fieldInfo) {

  /** Convert each key/score pair into a BigQuery TableRow. */
  protected class BuildRowFn extends DoFn[T, TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext, window: BoundedWindow): Unit = {
      val row: TableRow = fieldInfo.asScala
        .foldLeft(new TableRow()) {
          case (row, (key, fieldData)) =>
            val fieldFn = fieldData.fieldFn
            row.set(key, fieldFn.apply(ctx, window))
        }
      ctx.output(row)
    }
  }

  override def expand(teamAndScore: PCollection[T]): PDone = {
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
}
