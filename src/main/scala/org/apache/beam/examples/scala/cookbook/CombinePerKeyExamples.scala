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
import org.apache.beam.sdk.metrics.{Counter, Metrics}
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{Combine, DoFn, PTransform, ParDo, SerializableFunction}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PCollection}

object CombinePerKeyExamples {
  // Use the shakespeare public BigQuery sample
  private final val SHAKESPEARE_TABLE = "publicdata:samples.shakespeare"
  // We'll track words >= this word length across all plays in the table.
  private final val MIN_WORD_LENGTH = 9

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline = Pipeline.create(options)

    // Build the table schema for the output table.
    val fields: List[TableFieldSchema] = List(
      new TableFieldSchema().setName("word").setType("STRING"),
      new TableFieldSchema().setName("all_plays").setType("STRING"))
    val schema: TableSchema = new TableSchema().setFields(fields.asJava)

    pipeline
      .apply(BigQueryIO.readTableRows().from(options.getInput))
      .apply(new PlaysForWord())
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
    @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(SHAKESPEARE_TABLE)
    def getInput: String
    def setInput(value: String): Unit

    @Description(
      "Table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. "
        + "The dataset_id must already exist")
    @Validation.Required
    def getOutput(): String
    def setOutput(value: String): Unit
  }

  /**
    * Examines each row in the input table. If the word is greater than or equal to MIN_WORD_LENGTH,
    * outputs word, play_name.
    */
  class ExtractLargeWordsFn extends DoFn[TableRow, KV[String, String]] {
    private final val smallerWords: Counter =
      Metrics.counter(classOf[ExtractLargeWordsFn], "smallerWords")

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val playName = row.get("corpus").asInstanceOf[String]
      val word = row.get("word").asInstanceOf[String]
      if (word.length >= MIN_WORD_LENGTH) {
        ctx.output(KV.of(word, playName))
      } else {
        // Track how many smaller words we're not including. This information will be
        // visible in the Monitoring UI.
        smallerWords.inc()
      }
    }
  }

  /**
    * Prepares the data for writing to BigQuery by building a TableRow object containing a word with
    * a string listing the plays in which it appeared.
    */
  class FormatShakespeareOutputFn extends DoFn[KV[String, String], TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = new TableRow()
        .set("word", ctx.element.getKey)
        .set("all_plays", ctx.element.getValue)
      ctx.output(row)
    }
  }

  /**
    * Reads the public 'Shakespeare' data, and for each word in the dataset over a given length,
    * generates a string containing the list of play names in which that word appears. It does this
    * via the Combine.perKey transform, with the ConcatWords combine function.
    *
    * Combine.perKey is similar to a GroupByKey followed by a ParDo, but has more restricted
    * semantics that allow it to be executed more efficiently. These records are then formatted as BQ
    * table rows.
    */
  class PlaysForWord extends PTransform[PCollection[TableRow], PCollection[TableRow]] {
    override def expand(rows: PCollection[TableRow]): PCollection[TableRow] = {
      // row... => <word, play_name> ...
      val words: PCollection[KV[String, String]] = rows.apply(ParDo.of(new ExtractLargeWordsFn()))
      // word, play_name => word, all_plays ...
      val wordAllPlays: PCollection[KV[String, String]] =
        words.apply(Combine.perKey(new ConcatWords()))
      // <word, all_plays>... => row...
      wordAllPlays.apply(ParDo.of(new FormatShakespeareOutputFn()))
    }
  }

  /**
    * A 'combine function' used with the Combine.perKey transform. Builds a comma-separated string of
    * all input items. So, it will build a string containing all the different Shakespeare plays in
    * which the given input word has appeared.
    */
  class ConcatWords extends SerializableFunction[JIterable[String], String] {
    override def apply(input: JIterable[String]): String =
      input.asScala.mkString(",")
  }
}
