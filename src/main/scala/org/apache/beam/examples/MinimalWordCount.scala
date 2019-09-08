package org.apache.beam.examples

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.{Count, Filter, FlatMapElements, MapElements}
import org.apache.beam.sdk.transforms.{ParDo, DoFn, SimpleFunction, SerializableFunction, ProcessFunction}
import org.apache.beam.sdk.transforms.DoFn.{Element, ProcessElement, OutputReceiver}
import org.apache.beam.sdk.values.{KV, TypeDescriptors}

object MinimalWordCount {

  type JLong = java.lang.Long

  def main(args: Array[String]): Unit = {

    val options: PipelineOptions = PipelineOptionsFactory.create()
    val pipeline: Pipeline = Pipeline.create(options)

    pipeline
      .apply(TextIO.read().from("/tmp/data-wc.txt"))
      .apply(ParDo.of(tokenizeWords))
      .apply(Filter.by(filterNotEmpty)) // NOPE this shit is not working due to closure and override issues
      .apply(Count.perElement[String]())
      .apply(MapElements.into(TypeDescriptors.strings()).via(formatResults))
      .apply(TextIO.write().to("/tmp/minimal-wordcounts"))

    pipeline.run().waitUntilFinish()
  }

  // Conceptually of DoFn and SimpleFunction
  // https://stackoverflow.com/a/50631667
  // Probably can simplify this with ProcessContext: c.element() & c.output()
  def tokenizeWords = new DoFn[String, List[String]] {
    @ProcessElement
    def processElement(
        @Element element: String,
        receiver: OutputReceiver[List[String]]): Unit = {
      receiver.output(element.split("[^\\p{L}]+").toList)
    }
  }

  def formatResults = new SimpleFunction[KV[String, JLong], String] {
    override def apply(input: KV[String, JLong]): String =
      s"${input.getKey}: ${input.getValue}"
  }

  // Doesn't work and compile
  def filterNotEmpty: SerializableFunction[String, Boolean] = new SerializableFunction[String, Boolean] {
    override def apply(input: String): Boolean = input.nonEmpty
  }
}
