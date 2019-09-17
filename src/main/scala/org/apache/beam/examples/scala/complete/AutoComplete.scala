package org.apache.beam.examples.scala.complete

import scala.collection.JavaConverters._
import scala.math.Ordered
import scala.math.{min, pow}
import scala.util.matching.Regex

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo, Partition}
import org.apache.beam.sdk.transforms.{ProcessFunction, SerializableFunction}
import org.apache.beam.sdk.transforms.{Filter, Flatten, Top}
import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{KV, PCollection, PCollectionList}

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

  /**
    * Cheaper but higher latency.
    *
    * Returns two PCollections, the first is top prefixes of size greater than minPrefix, and the
    * second is top prefixes of size exactly minPrefix.
    */
  class ComputeTopRecursive(candidatesPerPrefix: JInteger, minPrefix: JInteger)
      extends PTransform[
        PCollection[CompletionCandidate],
        PCollectionList[KV[String, JList[CompletionCandidate]]]] {

    private[this] class KeySizePartitionFn
        extends PartitionFn[KV[String, JList[CompletionCandidate]]] {
      override def partitionFor(
          elem: KV[String, JList[CompletionCandidate]],
          numPartitions: Int): Int = {
        val _ = numPartitions
        if (elem.getKey.length > minPrefix) 0 else 1
      }
    }

    private[this] class FlattenTopsFn
        extends DoFn[KV[String, JList[CompletionCandidate]], CompletionCandidate] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit =
        for (completionCandidate <- ctx.element.getValue.asScala) {
          ctx.output(completionCandidate)
        }
    }

    override def expand(input: PCollection[CompletionCandidate])
        : PCollectionList[KV[String, JList[CompletionCandidate]]] =
      if (minPrefix > 10) {
        // Base case, partitioning to return the output in the expected format.
        input
          .apply(new ComputeTopFlat(candidatesPerPrefix, minPrefix))
          .apply(Partition.of(2, new KeySizePartitionFn()))
      } else {
        // If a candidate is in the top N for prefix a...b, it must also be in the top
        // N for a...bX for every X, which is typlically a much smaller set to consider.
        // First, compute the top candidate for prefixes of size at least minPrefix + 1.
        val larger: PCollectionList[KV[String, JList[CompletionCandidate]]] =
          input.apply(new ComputeTopRecursive(candidatesPerPrefix, minPrefix + 1))
        // Consider the top candidates for each prefix of length minPrefix + 1...
        val small: PCollection[KV[String, JList[CompletionCandidate]]] =
          PCollectionList
            .of(larger.get(1).apply(ParDo.of(new FlattenTopsFn())))
            // ...together with those (previously excluded) candidates of length
              // exactly minPrefix...
            .and(input.apply(Filter.by(new ProcessFunction[CompletionCandidate, JBoolean] {
              override def apply(candidate: CompletionCandidate): JBoolean =
                candidate.value.length == minPrefix
            })))
            .apply("FlattenSmall", Flatten.pCollections())
              // ...set the key to be the minPrefix-length prefix...
            .apply(ParDo.of(new AllPrefixesFn(minPrefix, minPrefix)))
              // ...and (re)apply the Top operator to all of them together.
            .apply(Top.largestPerKey(candidatesPerPrefix))

        val flattenLarger: PCollection[KV[String, JList[CompletionCandidate]]] =
          larger.apply("FlattenLarge", Flatten.pCollections())

        PCollectionList.of(flattenLarger).and(small)
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
  case class CompletionCandidate(value: String, count: JLong) extends Ordered[CompletionCandidate] {
    // Empty constructor required for Avro decoding.
    def this() = this("", 0L)

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
