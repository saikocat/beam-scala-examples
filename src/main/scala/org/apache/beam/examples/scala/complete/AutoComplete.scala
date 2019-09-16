package org.apache.beam.examples.scala.complete

import scala.util.matching.Regex

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object AutoComplete {
  /** Takes as input a set of strings, and emits each #hashtag found therein. */
  class ExtractHashtagsFn extends DoFn[String, String] {
    val hashtagRegex: Regex = "#\\S+".r

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      for (hashtag <- hashtagRegex.findAllIn(ctx.element)) {
        ctx.output(hashtag.substring(1))
      }
    }
  }
}
