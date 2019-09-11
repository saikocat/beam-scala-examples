package org.apache.beam.examples

import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.sdk.metrics.{Counter, Distribution, Metrics}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}

object WordCount {
  def main(args: Array[String]): Unit = {}

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
    }
  }
}
