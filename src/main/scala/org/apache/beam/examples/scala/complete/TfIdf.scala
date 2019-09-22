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

import java.net.{URI, URISyntaxException}
import java.io.{File, IOException}

import scala.collection.JavaConverters._

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{KvCoder, StringDelegateCoder, StringUtf8Coder}
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.{
  Count,
  Distinct,
  DoFn,
  Flatten,
  Keys,
  PTransform,
  ParDo,
  Values,
  View,
  WithKeys
}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.{
  KV,
  PBegin,
  PCollection,
  PCollectionList,
  PCollectionView,
  PDone,
  TupleTag
}
import org.slf4j.{Logger, LoggerFactory}

/**
  * An example that computes a basic TF-IDF search table for a directory or GCS prefix.
  *
  * Concepts: joining data; side inputs; logging
  */
object TfIdf {

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    runTfIdf(options)
  }

  @throws(classOf[Exception])
  def runTfIdf(options: Options): Unit = {
    val pipeline = Pipeline.create(options)
    pipeline
      .getCoderRegistry()
      .registerCoderForClass(classOf[URI], StringDelegateCoder.of(classOf[URI]))

    pipeline
      .apply(new ReadDocuments(listInputDocuments(options)))
      .apply(new ComputeTfIdf())
      .apply(new WriteTfIdf(options.getOutput))

    pipeline.run().waitUntilFinish()
    ()
  }

  trait Options extends PipelineOptions {
    @Description("Path to the directory or GCS prefix containing files to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/")
    def getInput: String
    def setInput(value: String): Unit

    @Description("Prefix of output URI to write to")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String): Unit
  }

  /** Lists documents contained beneath the options.input prefix/directory. */
  @throws(classOf[URISyntaxException])
  @throws(classOf[IOException])
  def listInputDocuments(options: Options): JSet[URI] = {
    val baseUri: URI = new URI(options.getInput)

    // List all documents in the directory or GCS prefix.
    val absoluteUri: URI = Option(baseUri.getScheme) match {
      case Some(_) => baseUri
      case None =>
        new URI(
          "file",
          baseUri.getAuthority,
          baseUri.getPath,
          baseUri.getQuery,
          baseUri.getFragment)
    }

    val uris: Set[URI] = absoluteUri.getScheme match {
      case "file" => {
        val directory = new File(absoluteUri)
        Option(directory.list())
          .getOrElse(Array.empty[String])
          .toSet[String]
          .map(entry => new File(directory, entry).toURI())
      }
      case "gs" => {
        val gcsUriGlob: URI = new URI(
          absoluteUri.getScheme,
          absoluteUri.getAuthority,
          absoluteUri.getPath + "*",
          absoluteUri.getQuery,
          absoluteUri.getFragment)
        val gcsUtil: GcsUtil = options.as(classOf[GcsOptions]).getGcsUtil()
        gcsUtil
          .expand(GcsPath.fromUri(gcsUriGlob))
          .asScala
          .toSet[GcsPath]
          .map(_.toUri())
      }
    }

    uris.asJava
  }

  /**
    * Reads the documents at the provided uris and returns all lines from the documents tagged with
    * which document they are from.
    */
  class ReadDocuments(uris: JIterable[URI])
      extends PTransform[PBegin, PCollection[KV[URI, String]]] {
    override def expand(input: PBegin): PCollection[KV[URI, String]] = {
      val pipeline: Pipeline = input.getPipeline()

      // Create one TextIO.Read transform for each document
      // and add its output to a PCollectionList
      val urisToLines: PCollectionList[KV[URI, String]] =
        uris.asScala.foldLeft(PCollectionList.empty[KV[URI, String]](pipeline))((acc, uri) => {
          val uriString = uriToString(uri)
          val oneUriToLines: PCollection[KV[URI, String]] =
            pipeline
              .apply(s"TextIO.Read($uriString)", TextIO.read().from(uriString))
              .apply(s"WithKeys($uriString)", WithKeys.of(uri))
              .setCoder(KvCoder.of(StringDelegateCoder.of(classOf[URI]), StringUtf8Coder.of()))
          acc.and(oneUriToLines)
        })

      urisToLines.apply(Flatten.pCollections())
    }

    // TextIO.Read supports:
    //  - file: URIs and paths locally
    //  - gs: URIs on the service
    def uriToString(uri: URI): String = uri.getScheme match {
      case "file" => new File(uri).getPath
      case _ => uri.toString
    }
  }

  /**
    * A transform containing a basic TF-IDF pipeline. The input consists of KV objects where the key
    * is the document's URI and the value is a piece of the document's content. The output is mapping
    * from terms to scores for each document URI.
    */
  class ComputeTfIdf
      extends PTransform[PCollection[KV[URI, String]], PCollection[KV[String, KV[URI, JDouble]]]] {
    override def expand(
        uriToContent: PCollection[KV[URI, String]]): PCollection[KV[String, KV[URI, JDouble]]] = {
      // Compute the total number of documents, and
      // prepare this singleton PCollectionView for
      // use as a side input.
      val totalDocuments: PCollectionView[JLong] =
        uriToContent
          .apply("GetURIs", Keys.create())
          .apply("DistinctDocs", Distinct.create())
          .apply(Count.globally())
          .apply(View.asSingleton())

      // Create a collection of pairs mapping a URI to each
      // of the words in the document associated with that that URI.
      val uriToWords: PCollection[KV[URI, String]] =
        uriToContent.apply("SplitWords", ParDo.of(new UriToWordFn()))

      // Compute a mapping from each word to the total
      // number of documents in which it appears.
      val wordToDocCount: PCollection[KV[String, JLong]] =
        uriToWords
          .apply("DistinctWords", Distinct.create())
          .apply(Values.create())
          .apply("CountDocs", Count.perElement())

      // Compute a mapping from each URI to the total
      // number of words in the document associated with that URI.
      val uriToWordTotal: PCollection[KV[URI, JLong]] =
        uriToWords
          .apply("GetURIs2", Keys.create())
          .apply("CountWords", Count.perElement())

      // Count, for each (URI, word) pair, the number of
      // occurrences of that word in the document associated
      // with the URI.
      val uriAndWordToCount: PCollection[KV[KV[URI, String], JLong]] =
        uriToWords.apply("CountWordDocPairs", Count.perElement())

      // Adjust the above collection to a mapping from
      // (URI, word) pairs to counts into an isomorphic mapping
      // from URI to (word, count) pairs, to prepare for a join
      // by the URI key.
      val uriToWordAndCount: PCollection[KV[URI, KV[String, JLong]]] =
        uriAndWordToCount.apply("ShiftKeys", ParDo.of(new UriToWordAndCountFn()))

      // Prepare to join the mapping of URI to (word, count) pairs with
      // the mapping of URI to total word counts, by associating
      // each of the input PCollection[KV[URI, ...]] with
      // a tuple tag. Each input must have the same key type, URI
      // in this case. The type parameter of the tuple tag matches
      // the types of the values for each collection.
      val wordTotalsTag: TupleTag[JLong] = new TupleTag()
      val wordCountsTag: TupleTag[KV[String, JLong]] = new TupleTag()
      val coGbkInput: KeyedPCollectionTuple[URI] =
        KeyedPCollectionTuple
          .of(wordTotalsTag, uriToWordTotal)
          .and(wordCountsTag, uriToWordAndCount)

      // Perform a CoGroupByKey (a sort of pre-join) on the prepared
      // inputs. This yields a mapping from URI to a CoGbkResult
      // (CoGroupByKey Result). The CoGbkResult is a mapping
      // from the above tuple tags to the values in each input
      // associated with a particular URI. In this case, each
      // KV[URI, CoGbkResult] group a URI with the total number of
      // words in that document as well as all the (word, count)
      // pairs for particular words.
      val uriToWordAndCountAndTotal: PCollection[KV[URI, CoGbkResult]] =
        coGbkInput.apply("CoGroupByUri", CoGroupByKey.create())

      // Compute a mapping from each word to a (URI, term frequency)
      // pair for each URI. A word's term frequency for a document
      // is simply the number of times that word occurs in the document
      // divided by the total number of words in the document.
      val wordToUriAndTf: PCollection[KV[String, KV[URI, JDouble]]] =
        uriToWordAndCountAndTotal.apply(
          "ComputeTermFrequencies",
          ParDo.of(new ComputeTermFrequenciesPerUriFn(wordTotalsTag, wordCountsTag)))

      // Compute a mapping from each word to its document frequency.
      // A word's document frequency in a corpus is the number of
      // documents in which the word appears divided by the total
      // number of documents in the corpus. Note how the total number of
      // documents is passed as a side input; the same value is
      // presented to each invocation of the DoFn.
      val wordToDf: PCollection[KV[String, JDouble]] =
        wordToDocCount.apply(
          "ComputeDocFrequencies",
          ParDo
            .of(new ComputeDocFrequenciesFn(totalDocuments))
            .withSideInputs(totalDocuments))

      // Join the term frequency and document frequency
      // collections, each keyed on the word.
      val tfTag: TupleTag[KV[URI, JDouble]] = new TupleTag()
      val dfTag: TupleTag[JDouble] = new TupleTag()
      val wordToUriAndTfAndDf: PCollection[KV[String, CoGbkResult]] =
        KeyedPCollectionTuple
          .of(tfTag, wordToUriAndTf)
          .and(dfTag, wordToDf)
          .apply(CoGroupByKey.create())

      // Compute a mapping from each word to a (URI, TF-IDF) score
      // for each URI. There are a variety of definitions of TF-IDF
      // ("term frequency - inverse document frequency") score;
      // here we use a basic version that is the term frequency
      // divided by the log of the document frequency.
      val wordToUriAndTfIdf: PCollection[KV[String, KV[URI, JDouble]]] =
        wordToUriAndTfAndDf.apply("ComputeTfIdf", ParDo.of(new ComputeTfIdfFn(tfTag, dfTag)))

      wordToUriAndTfIdf
    }

    // Split words and output uri & word pair
    class UriToWordFn extends DoFn[KV[URI, String], KV[URI, String]] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit = {
        val uri: URI = ctx.element.getKey
        val line: String = ctx.element.getValue
        for (word <- line.split("\\W+", -1)) {
          // Log INFO messages when the word "love" is found.
          if ("love".equalsIgnoreCase(word)) {
            LOG.info("Found {}", word.toLowerCase)
          }

          if (word.nonEmpty) {
            ctx.output(KV.of(uri, word.toLowerCase))
          }
        }
      }
    }

    // shift keys to get (uri, occurrences) pair
    class UriToWordAndCountFn extends DoFn[KV[KV[URI, String], JLong], KV[URI, KV[String, JLong]]] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit = {
        val uri: URI = ctx.element.getKey.getKey
        val word = ctx.element.getKey.getValue
        val occurrences: JLong = ctx.element.getValue
        ctx.output(KV.of(uri, KV.of(word, occurrences)))
      }
    }

    // Compute Term Frequencies for each URI
    class ComputeTermFrequenciesPerUriFn(
        wordTotalsTag: TupleTag[JLong],
        wordCountsTag: TupleTag[KV[String, JLong]])
        extends DoFn[KV[URI, CoGbkResult], KV[String, KV[URI, JDouble]]] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit = {
        val uri: URI = ctx.element.getKey
        val wordTotal: JLong = ctx.element.getValue.getOnly(wordTotalsTag)

        for (wordAndCount <- ctx.element.getValue.getAll(wordCountsTag).asScala) {
          val word = wordAndCount.getKey
          val wordCount: JLong = wordAndCount.getValue
          val termFrequency: JDouble = wordCount.doubleValue() / wordTotal.doubleValue()
          ctx.output(KV.of(word, KV.of(uri, termFrequency)))
        }
      }
    }

    // Computer Document Frequencies for each word
    class ComputeDocFrequenciesFn(totalDocuments: PCollectionView[JLong])
        extends DoFn[KV[String, JLong], KV[String, JDouble]] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit = {
        val word = ctx.element.getKey
        val documentCount: JLong = ctx.element.getValue
        val documentTotal: JLong = ctx.sideInput(totalDocuments)
        val documentFrequency: JDouble = documentCount.doubleValue() / documentTotal.doubleValue()
        ctx.output(KV.of(word, documentFrequency))
      }
    }

    class ComputeTfIdfFn(tfTag: TupleTag[KV[URI, JDouble]], dfTag: TupleTag[JDouble])
        extends DoFn[KV[String, CoGbkResult], KV[String, KV[URI, JDouble]]] {
      @ProcessElement
      def processElement(ctx: ProcessContext): Unit = {
        val word = ctx.element.getKey
        val df: JDouble = ctx.element.getValue.getOnly(dfTag)

        for (uriAndTf <- ctx.element.getValue.getAll(tfTag).asScala) {
          val uri: URI = uriAndTf.getKey
          val tf: JDouble = uriAndTf.getValue
          val tfIdf: JDouble = tf * Math.log(1 / df)
          ctx.output(KV.of(word, KV.of(uri, tfIdf)))
        }
      }
    }

    // Instantiate Logger.
    // It is suggested that the user specify the class name of the containing class
    // (in this case ComputeTfIdf).
    val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  }

  /** A PTransform to write, in CSV format, a mapping from term and URI to score. */
  class WriteTfIdf(output: String)
      extends PTransform[PCollection[KV[String, KV[URI, JDouble]]], PDone] {
    override def expand(wordToUriAndTfIdf: PCollection[KV[String, KV[URI, JDouble]]]): PDone =
      wordToUriAndTfIdf
        .apply("Format", ParDo.of(new FormatTfIdfFn()))
        .apply(TextIO.write().to(output).withSuffix(".csv"))
  }

  class FormatTfIdfFn extends DoFn[KV[String, KV[URI, JDouble]], String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      ctx.output(
        String.format(
          "%s,\t%s,\t%f",
          ctx.element.getKey,
          ctx.element.getValue.getKey,
          ctx.element.getValue.getValue))
  }
}
