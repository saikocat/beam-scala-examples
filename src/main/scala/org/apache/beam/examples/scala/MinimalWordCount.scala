package org.apache.beam.examples.scala

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.{Count, Filter, FlatMapElements, MapElements, ProcessFunction}
import org.apache.beam.sdk.values.{KV, TypeDescriptors}

object MinimalWordCount {
  def main(args: Array[String]): Unit = {
    val options: PipelineOptions = PipelineOptionsFactory.create()
    val pipeline: Pipeline = Pipeline.create(options)

    // Follow the Java guide faithfully, but personally I'd rather make those
    // anonymous ProcessFunction into individual atomic func for testability
    pipeline
      .apply(TextIO.read().from("/tmp/data-wc.txt"))
      .apply(FlatMapElements
        .into(TypeDescriptors.strings())
        .via(new ProcessFunction[String, JIterable[String]] {
          import scala.collection.JavaConverters._
          override def apply(input: String): JIterable[String] =
            input.split("[^\\p{L}]+").toIterable.asJava
        }))
      .apply(Filter.by(new ProcessFunction[String, JBoolean] {
        override def apply(input: String): JBoolean = Boolean.box(input.nonEmpty)
      }))
      .apply(Count.perElement[String]())
      .apply(MapElements
        .into(TypeDescriptors.strings())
        .via(new ProcessFunction[KV[String, JLong], String] {
          override def apply(input: KV[String, JLong]): String =
            s"${input.getKey}: ${input.getValue}"
        }))
      .apply(TextIO.write().to("/tmp/minimal-wordcounts"))

    pipeline.run().waitUntilFinish()
    ()
  }
}
