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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.Ordered
import scala.math.{min, pow}
import scala.util.matching.Regex

import com.google.api.services.bigquery.model.{
  TableFieldSchema,
  TableReference,
  TableRow,
  TableSchema
}
import com.google.datastore.v1.client.DatastoreHelper.{makeKey, makeValue}
import com.google.datastore.v1.{Entity, Key, Value}
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects
import org.apache.beam.examples.common.{ExampleBigQueryTableOptions, ExampleOptions, ExampleUtils}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo, Partition}
import org.apache.beam.sdk.transforms.{ProcessFunction, SerializableFunction}
import org.apache.beam.sdk.transforms.{Count, Filter, Flatten, Sum, Top}
import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{GlobalWindows, SlidingWindows, Window, WindowFn}
import org.apache.beam.sdk.values.{KV, PCollection, PCollectionList}
import org.joda.time.Duration

object AutoComplete {

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    runAutocompletePipeline(options)
  }

  @throws(classOf[java.io.IOException])
  def runAutocompletePipeline(options: Options): Unit = {
    options.setBigQuerySchema(FormatForBigqueryFn.getSchema)
    val exampleUtils = new ExampleUtils(options)

    // We support running the same pipeline in either
    // batch or windowed streaming mode.
    val windowFn: WindowFn[Object, _] =
      if (options.isStreaming) {
        // Checksum doesn't seems to work without setting WithoutDefaults(),
        // even then the same checksum still failed PAssert even though they
        // have same value
        checkArgument(!options.isStreaming, "Checksum is not supported in streaming.", Nil)
        SlidingWindows.of(Duration.standardMinutes(30)).every(Duration.standardSeconds(5))
      } else {
        new GlobalWindows()
      }

    // Create the pipeline.
    val pipeline = Pipeline.create(options)
    val toWrite: PCollection[KV[String, JList[CompletionCandidate]]] =
      pipeline
        .apply(TextIO.read().from(options.getInputFile))
        .apply(ParDo.of(new ExtractHashtagsFn()))
        .apply(Window.into(windowFn))
        .apply(ComputeTopCompletions.top(10, options.getRecursive))

    // Datastore output
    if (options.getOutputToDatastore) {
      toWrite
        .apply(
          "FormatForDatastore",
          ParDo.of(new FormatForDatastoreFn(options.getKind, options.getDatastoreAncestorKey)))
        .apply(
          DatastoreIO
            .v1()
            .write()
            .withProjectId(MoreObjects.firstNonNull(options.getOutputProject, options.getProject)))
    }

    if (options.getOutputToBigQuery) {
      exampleUtils.setupBigQueryTable()
      val tableRef: TableReference = new TableReference()
        .setProjectId(options.getProject)
        .setDatasetId(options.getBigQueryDataset)
        .setTableId(options.getBigQueryTable)

      val writeDisposition = options.isStreaming match {
        case true => BigQueryIO.Write.WriteDisposition.WRITE_APPEND
        case false => BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
      }

      toWrite
        .apply(ParDo.of(new FormatForBigqueryFn()))
        .apply(
          BigQueryIO
            .writeTableRows()
            .to(tableRef)
            .withSchema(FormatForBigqueryFn.getSchema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(writeDisposition))
    }

    // Checksum output
    if (options.getOutputToChecksum) {
      val checksum: PCollection[JLong] = toWrite
        .apply(ParDo.of(new CalculateChecksumFn()))
        .apply(Sum.longsGlobally())
      PAssert.that(checksum).containsInAnyOrder(options.getExpectedChecksum)
    }

    // Run the pipeline.
    val result: PipelineResult = pipeline.run()

    // ExampleUtils will try to cancel the pipeline and the injector before the program exists.
    exampleUtils.waitToFinish(result)
  }

  // CLI Opt
  trait Options extends ExampleOptions with StreamingOptions with ExampleBigQueryTableOptions {
    @Description("Input text file")
    @Validation.Required
    def getInputFile: String
    def setInputFile(value: String): Unit

    @Description("Whether to use the recursive algorithm")
    @Default.Boolean(true)
    def getRecursive: JBoolean
    def setRecursive(value: JBoolean): Unit

    @Description("Whether output to BigQuery")
    @Default.Boolean(true)
    def getOutputToBigQuery: JBoolean
    def setOutputToBigQuery(value: JBoolean): Unit

    @Description("Whether to send output to checksum Transform.")
    @Default.Boolean(true)
    def getOutputToChecksum: JBoolean
    def setOutputToChecksum(value: JBoolean): Unit

    @Description("Expected result of the checksum transform.")
    def getExpectedChecksum: JLong
    def setExpectedChecksum(value: JLong): Unit

    @Description("Whether output to Cloud Datastore")
    @Default.Boolean(false)
    def getOutputToDatastore: JBoolean
    def setOutputToDatastore(value: JBoolean): Unit

    @Description("Cloud Datastore ancestor key")
    @Default.String("root")
    def getDatastoreAncestorKey: String
    def setDatastoreAncestorKey(value: String): Unit

    @Description("Cloud Datastore output project ID, defaults to project ID")
    def getOutputProject: String
    def setOutputProject(value: String): Unit

    @Description("Cloud Datastore entity kind")
    @Default.String("autocomplete-demo")
    def getKind: String
    def setKind(value: String): Unit
  }

  /**
    * A PTransform that takes as input a list of tokens and returns the most common tokens per
    * prefix.
    */
  class ComputeTopCompletions(candidatesPerPrefix: JInteger, recursive: JBoolean)
      extends PTransform[PCollection[String], PCollection[KV[String, JList[CompletionCandidate]]]] {

    override def expand(
        input: PCollection[String]): PCollection[KV[String, JList[CompletionCandidate]]] = {
      val candidates: PCollection[CompletionCandidate] = input
        // First count how often each token appears.
        .apply(Count.perElement())
          // Map the KV outputs of Count into our own CompletionCandiate class.
        .apply(
          "CreateCompletionCandidates",
          ParDo.of(ComputeTopCompletions.createCompletionCandidatesFn))

      // Compute the top via either a flat or recursive algorithm.
      if (recursive) {
        candidates
          .apply(new ComputeTopRecursive(candidatesPerPrefix, 1))
          .apply(Flatten.pCollections())
      } else {
        candidates.apply(new ComputeTopFlat(candidatesPerPrefix, 1))
      }
    }
  }

  // companion object
  object ComputeTopCompletions {
    def top(candidatesPerPrefix: Int, recursive: Boolean): ComputeTopCompletions =
      new ComputeTopCompletions(candidatesPerPrefix, recursive)

    val createCompletionCandidatesFn = new DoFn[KV[String, JLong], CompletionCandidate] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit =
        ctx.output(CompletionCandidate(ctx.element.getKey, ctx.element.getValue))
    }
  }

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

    override def hashCode: Int =
      count.hashCode ^ value.hashCode
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

  /** Calculate the checksum of key and its candidate */
  class CalculateChecksumFn extends DoFn[KV[String, JList[CompletionCandidate]], JLong]() {
    @ProcessElement
    def process(ctx: ProcessContext): Unit = {
      val elm: KV[String, JList[CompletionCandidate]] = ctx.element
      val listHash: JLong = ctx.element.getValue.asScala.foldLeft(0L)(_ + _.hashCode.toLong)
      ctx.output(elm.getKey.hashCode.toLong + listHash)
    }
  }

  /** Output format for BigQuery **/
  class FormatForBigqueryFn extends DoFn[KV[String, JList[CompletionCandidate]], TableRow] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val completions: List[TableRow] = ctx.element.getValue.asScala
        .map(cc => new TableRow().set("count", cc.count).set("tag", cc.value))
        .toList
      val row: TableRow = new TableRow()
        .set("prefix", ctx.element.getKey)
        .set("tags", completions.asJava)
      ctx.output(row)
    }
  }

  object FormatForBigqueryFn {

    /** Defines the BigQuery schema used for the output. */
    def getSchema: TableSchema = {
      val tagFields = List[TableFieldSchema](
        new TableFieldSchema().setName("count").setType("INTEGER"),
        new TableFieldSchema().setName("tag").setType("STRING"))
      val fields = List[TableFieldSchema](
        new TableFieldSchema().setName("prefix").setType("STRING"),
        new TableFieldSchema()
          .setName("tags")
          .setType("RECORD")
          .setMode("REPEATED")
          .setFields(tagFields.asJava)
      )
      new TableSchema().setFields(fields.asJava)
    }
  }

  /**
    * Takes as input a the top candidates per prefix, and emits an entity suitable for writing to
    * Cloud Datastore.
    *
    * Note: We use ancestor keys for strong consistency. See the Cloud Datastore documentation on
    * https://cloud.google.com/datastore/docs/concepts/structuring_for_strong_consistency
    * Structuring Data for Strong Consistency
    */
  class FormatForDatastoreFn(kind: String, ancestorKey: String)
      extends DoFn[KV[String, JList[CompletionCandidate]], Entity] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val entityBuilder: Entity.Builder = Entity.newBuilder()
      val key: Key = makeKey(makeKey(kind, ancestorKey).build(), kind, ctx.element.getKey).build()

      entityBuilder.setKey(key)
      val candidates = mutable.ListBuffer.empty[Value]
      val properties = mutable.Map.empty[String, Value]
      for (tag <- ctx.element.getValue.asScala) {
        val tagEntity: Entity.Builder = Entity.newBuilder()
        properties.put("tag", makeValue(tag.value).build())
        properties.put("count", makeValue(tag.count).build())
        candidates.append(makeValue(tagEntity).build())
      }
      properties.put("candidates", makeValue(candidates.asJava).build())
      entityBuilder.putAllProperties(properties.asJava)
      ctx.output(entityBuilder.build())
    }
  }

}
