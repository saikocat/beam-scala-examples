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

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.{Default, Description, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{KV, PCollection}

import org.slf4j.LoggerFactory

import java.util.regex.Pattern

import scala.jdk.CollectionConverters._

object DebuggingWordCount {

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[WordCountOptions])

    runDebuggingWordCount(options)
  }

  def runDebuggingWordCount(options: WordCountOptions): Unit = {
    val pipeline = Pipeline.create(options)

    val filteredWords: PCollection[KV[String, JLong]] = pipeline
      .apply("ReadLines", TextIO.read().from(options.getInputFile))
      .apply(new WordCount.CountWords())
      .apply(ParDo.of(new FilterTextFn(options.getFilterPattern)))

    val expectedResults = List(
      KV.of("Flourish", new JLong(3)),
      KV.of("stomach", new JLong(1))
    ).asJava
    PAssert.that(filteredWords).containsInAnyOrder(expectedResults)

    pipeline.run().waitUntilFinish()
    ()
  }

  class FilterTextFn(pattern: String) extends DoFn[KV[String, JLong], KV[String, JLong]] {
    import FilterTextFn.LOG
    private val filter: Pattern = Pattern.compile(pattern)

    private val matchedWords = Metrics.counter(classOf[FilterTextFn], "matchedWords")
    private val unmatchedWords = Metrics.counter(classOf[FilterTextFn], "unmatchedWords")

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit =
      if (filter.matcher(ctx.element.getKey).matches()) {
        LOG.debug(s"Matched: ${ctx.element.getKey}")
        matchedWords.inc()
        ctx.output(ctx.element)
      } else {
        LOG.trace(s"Did not match: ${ctx.element.getKey}")
        unmatchedWords.inc()
      }
  }

  object FilterTextFn {
    private val LOG = LoggerFactory.getLogger(classOf[FilterTextFn])
  }

  trait WordCountOptions extends WordCount.WordCountOptions {
    @Description(
      "Regex filter pattern to use in DebuggingWordCount. Only words matching this pattern will be counted.")
    @Default.String("Flourish|stomach")
    def getFilterPattern: String
    def setFilterPattern(value: String): Unit
  }
}
