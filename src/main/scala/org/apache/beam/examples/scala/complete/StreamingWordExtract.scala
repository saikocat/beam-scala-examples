package org.apache.beam.examples.scala.complete

import scala.collection.JavaConverters._

import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

object StreamingWordExtract {

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

    def getSchema(): TableSchema =
      new TableSchema()
        .setFields(List(new TableFieldSchema().setName("string_field").setType("STRING")).asJava)
  }

}
