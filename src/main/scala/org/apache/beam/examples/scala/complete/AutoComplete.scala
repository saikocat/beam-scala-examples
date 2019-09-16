package org.apache.beam.examples.scala.complete

import scala.math.Ordered
import scala.math.{min, pow}
import scala.util.matching.Regex

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo, SerializableFunction}
import org.apache.beam.sdk.transforms.Top
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PCollection}

object AutoComplete {

  /** Lower latency, but more expensive. */
  class ComputeTopFlat(candidatesPerPrefix: JInteger, minPrefix: JInteger)
      extends PTransform[
        PCollection[CompletionCandidate],
        PCollection[KV[String, JList[CompletionCandidate]]]] {

    override def expand(input: PCollection[CompletionCandidate])
        : PCollection[KV[String, JList[CompletionCandidate]]] =
      input
        // For each completion candidate, map it to all prefixes.
        .apply(ParDo.of(new AllPrefixesFn(minPrefix)))
          // Find and return the top candiates for each prefix.
        .apply(
          Top
            .largestPerKey[String, CompletionCandidate](candidatesPerPrefix)
            .withHotKeyFanout(new HotKeyFanout()))

    class HotKeyFanout extends SerializableFunction[String, JInteger] {
      override def apply(input: String): JInteger =
        pow(4, (5 - input.length).toDouble).toInt
    }
  }

  /** A DoFn that keys each candidate by all its prefixes. */
  class AllPrefixesFn(minPrefix: JInteger, maxPrefix: JInteger = Integer.MAX_VALUE)
      extends DoFn[CompletionCandidate, KV[String, CompletionCandidate]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val word = ctx.element.value
      for (prefixLen <- minPrefix.toInt to min(word.length, maxPrefix.toInt)) {
        ctx.output(KV.of(word.substring(0, prefixLen), ctx.element))
      }
    }
  }

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
