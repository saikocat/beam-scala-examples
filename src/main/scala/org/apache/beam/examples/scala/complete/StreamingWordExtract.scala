package org.apache.beam.examples.scala.complete

import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

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

}
