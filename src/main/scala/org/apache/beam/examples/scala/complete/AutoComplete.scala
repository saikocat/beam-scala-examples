package org.apache.beam.examples.scala.complete

import scala.math.Ordered
import scala.util.matching.Regex

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object AutoComplete {

  /** Class used to store tag-count pairs. */
  @DefaultCoder(classOf[AvroCoder[CompletionCandidate]])
  case class CompletionCandidate(count: JLong, value: String) extends Ordered[CompletionCandidate] {
    // Empty constructor required for Avro decoding.
    def this() = this(0L, "")

    def compare(that: CompletionCandidate) =
      Long2long(count).compare(Long2long(that.count)) match {
        case 0 => value.compare(that.value)
        case c => c
      }
  }

  /** Takes as input a set of strings, and emits each #hashtag found therein. */
  class ExtractHashtagsFn extends DoFn[String, String] {
    val hashtagRegex: Regex = "#\\S+".r

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      for (hashtag <- hashtagRegex.findAllIn(ctx.element)) {
        ctx.output(hashtag.substring(1))
      }
  }
}
