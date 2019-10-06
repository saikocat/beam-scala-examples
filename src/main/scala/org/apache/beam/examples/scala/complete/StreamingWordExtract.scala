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

import scala.jdk.CollectionConverters._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.apache.beam.examples.common.{ExampleBigQueryTableOptions, ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.{Pipeline, PipelineResult}

object StreamingWordExtract {

  @throws(classOf[java.io.IOException])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[StreamingWordExtractOptions])
    options.setStreaming(true)

    val tableSpec =
      s"${options.getProject}:${options.getBigQueryDataset}.${options.getBigQueryTable}"
    val schema = StringToRowConverterFn.getSchema
    options.setBigQuerySchema(schema)

    val exampleUtils = new ExampleUtils(options)
    exampleUtils.setup()

    val pipeline = Pipeline.create(options)
    pipeline
      .apply("ReadLines", TextIO.read().from(options.getInputFile))
      .apply(ParDo.of(new ExtractWordsFn()))
      .apply(ParDo.of(new UppercaseFn()))
      .apply(ParDo.of(new StringToRowConverterFn()))
      .apply(BigQueryIO.writeTableRows().to(tableSpec).withSchema(schema))

    val result: PipelineResult = pipeline.run()

    // ExampleUtils will try to cancel the pipeline before the program exists.
    exampleUtils.waitToFinish(result)
  }

  trait StreamingWordExtractOptions
      extends ExampleOptions
      with ExampleBigQueryTableOptions
      with StreamingOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    def getInputFile: String
    def setInputFile(value: String): Unit
  }

  /** A DoFn that tokenizes lines of text into individual words. */
  class ExtractWordsFn extends DoFn[String, String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val words = ctx.element.split(ExampleUtils.TOKENIZER_PATTERN, -1)
      for (word <- words if word.nonEmpty) {
        ctx.output(word)
      }
    }
  }

  /** A DoFn that uppercases a word. */
  class UppercaseFn extends DoFn[String, String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      ctx.output(ctx.element.toUpperCase)
  }

  /** Converts strings into BigQuery rows. */
  class StringToRowConverterFn extends DoFn[String, TableRow] {

    /** In this example, put the whole string into single BigQuery field. */
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      ctx.output(new TableRow().set("string_field", ctx.element))
  }

  /** Companion object */
  object StringToRowConverterFn {
    def getSchema(): TableSchema =
      new TableSchema()
        .setFields(List(new TableFieldSchema().setName("string_field").setType("STRING")).asJava)
  }

}
