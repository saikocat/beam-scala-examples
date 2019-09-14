package org.apache.beam.examples.scala.cookbook

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options._
import org.apache.beam.sdk.transforms.Distinct

object DistinctExample {
  trait Options extends PipelineOptions {
    @Description("Path to the directory or GCS prefix containing files to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/*")
    def getInput: String
    def setInput(value: String): Unit

    @Description("Path of the file to write to")
    @Default.InstanceFactory(classOf[OutputFactory])
    def getOutput: String
    def setOutput(value: String): Unit

  }

  /** Returns gs://${TEMP_LOCATION}/"deduped.txt". */
  class OutputFactory extends DefaultValueFactory[String] {
    override def create(options: PipelineOptions): String =
      Option(options.getTempLocation) match {
        case Some(location) => GcsPath.fromUri(location).resolve("deduped.txt").toString
        case None => throw new IllegalArgumentException("Must specify --output or --tempLocation")
      }
  }

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline = Pipeline.create(options)

    pipeline
      .apply("ReadLines", TextIO.read().from(options.getInput))
      .apply(Distinct.create())
      .apply("DedupedShakespeare", TextIO.write().to(options.getOutput))

    pipeline.run().waitUntilFinish()
    ()
  }
}
