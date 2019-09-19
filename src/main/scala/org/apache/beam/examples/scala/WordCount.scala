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
package org.apache.beam.examples.scala

import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.{Counter, Distribution, Metrics}
import org.apache.beam.sdk.options.{
  Default,
  Description,
  PipelineOptions,
  PipelineOptionsFactory,
  Validation
}
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}
import org.apache.beam.sdk.transforms.{Count, DoFn, MapElements, PTransform, ParDo, SimpleFunction}
import org.apache.beam.sdk.values.{KV, PCollection}

object WordCount {
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[WordCountOptions])

    runWordCount(options)
  }

  trait WordCountOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    def getInputFile: String
    def setInputFile(path: String): Unit

    @Description("Path of the file to write to")
    @Validation.Required
    def getOutput: String
    def setOutput(path: String): Unit
  }

  def runWordCount(options: WordCountOptions): Unit = {
    val pipeline: Pipeline = Pipeline.create(options)

    pipeline
      .apply("ReadLines", TextIO.read().from(options.getInputFile))
      .apply(new CountWords())
      .apply(MapElements.via(new FormatAsTextFn()))
      .apply("WriteCounts", TextIO.write().to(options.getOutput))

    pipeline.run().waitUntilFinish()
    ()
  }

  /** Tokenizes lines of text into individual words */
  class ExtractWordsFn extends DoFn[String, String] {
    private final val emptyLines: Counter =
      Metrics.counter(classOf[ExtractWordsFn], "emptyLines")
    private final val lineLenDist: Distribution =
      Metrics.distribution(classOf[ExtractWordsFn], "lineLenDist")

    @ProcessElement
    def processElement(@Element element: String, receiver: OutputReceiver[String]): Unit = {
      lineLenDist.update(element.length.toLong)
      if (element.trim.isEmpty) emptyLines.inc()

      for (word <- element.split(ExampleUtils.TOKENIZER_PATTERN)) yield {
        if (!word.isEmpty) receiver.output(word)
      }
      ()
    }
  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  class FormatAsTextFn extends SimpleFunction[KV[String, JLong], String] {
    override def apply(input: KV[String, JLong]): String =
      s"${input.getKey}: ${input.getValue}"
  }

  /**
    * A PTransform that converts a PCollection containing lines of text into a PCollection of
    * formatted word counts.
    */
  class CountWords extends PTransform[PCollection[String], PCollection[KV[String, JLong]]] {
    override def expand(lines: PCollection[String]): PCollection[KV[String, JLong]] = {
      // Convert lines of text into individual words.
      val words: PCollection[String] = lines.apply(ParDo.of(new ExtractWordsFn()))

      // Count the number of times each word occurs.
      val wordCounts: PCollection[KV[String, JLong]] = words.apply(Count.perElement())
      // banally follow java guide
      wordCounts
    }
  }
}
