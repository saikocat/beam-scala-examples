package org.apache.beam.examples.scala

import java.util.concurrent.ThreadLocalRandom

import scala.util.{Failure, Success, Try}

import org.joda.time.{Duration, Instant}

import org.apache.beam.sdk.{Pipeline, PipelineResult}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.examples.common.{ExampleBigQueryTableOptions, ExampleOptions, WriteOneFilePerWindow}
import org.apache.beam.sdk.transforms.{DoFn, MapElements, ParDo}
import org.apache.beam.sdk.options.{Default, DefaultValueFactory, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.values.{KV, PCollection}

object WindowedWordCount {
  final val WINDOW_SIZE = 1 // Default window duration in minutes

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    runWindowedWordCount(options)
  }

  def runWindowedWordCount(options: Options): Unit = {
    val output = options.getOutput
    val minTimestamp = new Instant(options.getMinTimestampMillis)
    val maxTimestamp = new Instant(options.getMaxTimestampMillis)

    val pipeline = Pipeline.create(options)

    val input: PCollection[String] = pipeline
      .apply(TextIO.read().from(options.getInputFile))
      // artificial add timestamp
      .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)))

    val windowedWords: PCollection[String] = input.apply(
      Window.into(
        FixedWindows.of(
          Duration.standardMinutes(options.getWindowSize.toLong))))

    val wordCounts: PCollection[KV[String, JLong]] =
      windowedWords.apply(new WordCount.CountWords())

    /*
     * Format the results and write to a sharded file partitioned by window, using a
     * simple ParDo operation. Because there may be failures followed by retries, the
     * writes must be idempotent, but the details of writing to files is elided here.
     */
    wordCounts
        .apply(MapElements.via(new WordCount.FormatAsTextFn()))
        .apply(new WriteOneFilePerWindow(output, options.getNumShards));

    val result: PipelineResult = pipeline.run()
    Try(result.waitUntilFinish()) match {
      case Success(state@_) => ()
      case Failure(ex@_) => result.cancel(); ()
    }
  }

  trait Options extends WordCount.WordCountOptions
      with ExampleOptions
      with ExampleBigQueryTableOptions {
    @Description("Fixed window duration, in minutes")
    @Default.Integer(WINDOW_SIZE)
    def getWindowSize: JInteger
    def setWindowSize(value: JInteger): Unit

    @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
    @Default.InstanceFactory(classOf[DefaultToCurrentSystemTime])
    def getMinTimestampMillis: JLong
    def setMinTimestampMillis(value: JLong): Unit

    @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
    @Default.InstanceFactory(classOf[DefaultToMinTimestampPlusOneHour])
    def getMaxTimestampMillis: JLong
    def setMaxTimestampMillis(value: JLong): Unit

    @Description("Fixed number of shards to produce per window")
    def getNumShards: Integer
    def setNumShards(numShards: Integer): Unit
  }

  class DefaultToCurrentSystemTime extends DefaultValueFactory[Long] {
    override def create(options: PipelineOptions) = {
      System.currentTimeMillis()
    }
  }

  class DefaultToMinTimestampPlusOneHour extends DefaultValueFactory[Long] {
    override def create(options: PipelineOptions): Long = {
      options.as(classOf[Options])
        .getMinTimestampMillis + Duration.standardHours(1).getMillis
    }
  }

  class AddTimestampFn(private val minTimestamp: Instant, private val maxTimestamp: Instant)
      extends DoFn[String, String]() {
    import DoFn.{Element, OutputReceiver, ProcessElement}
    @ProcessElement
    def processElement(@Element element: String, receiver: OutputReceiver[String]): Unit = {
      val randomTimestamp = new Instant(
        ThreadLocalRandom
          .current()
          .nextLong(minTimestamp.getMillis, maxTimestamp.getMillis))

      receiver.outputWithTimestamp(element, new Instant(randomTimestamp))
    }
  }
}
