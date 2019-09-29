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
package org.apache.beam.examples.scala.complete.game.utils

import java.io.Serializable
import java.util.TimeZone

import scala.collection.JavaConverters._

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.io.FileBasedSink
import org.apache.beam.sdk.io.FileBasedSink.{FilenamePolicy, OutputFileHints}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow, PaneInfo}
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Generate, format, and write rows. Use provided information about the field names and types, as
  * well as lambda functions that describe how to generate their values.
  */
class WriteToText[InputT](
    filenamePrefix: String,
    fieldFn: JMap[String, WriteToText.FieldFn[InputT]],
    windowed: Boolean)
    extends PTransform[PCollection[InputT], PDone] {
  import WriteToText.formatter

  /** Convert each key/score pair into a row as specified by fieldFn. */
  protected class BuildRowFn extends DoFn[InputT, String] {
    @ProcessElement
    def processElement(ctx: ProcessContext, window: BoundedWindow): Unit = {
      val result = fieldFn.asScala
        .map { case (key, value) => s"$key: ${value.apply(ctx, window)}" }
        .mkString(", ")
      ctx.output(result)
    }
  }

  override def expand(teamAndScore: PCollection[InputT]): PDone = {
    val writeStrategy =
      if (windowed) new WriteOneFilePerWindow(filenamePrefix)
      else TextIO.write().to(filenamePrefix)

    teamAndScore
      .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
      .apply(writeStrategy)
    PDone.in(teamAndScore.getPipeline())
  }

  /**
    * A DoFn that writes elements to files with names deterministically derived from the
    * lower and upper bounds of their key (an IntervalWindow).
    */
  protected class WriteOneFilePerWindow(filenamePrefix: String)
      extends PTransform[PCollection[String], PDone] {
    override def expand(input: PCollection[String]): PDone = {
      // Verify that the input has a compatible window type.
      checkArgument(
        input.getWindowingStrategy().getWindowFn().windowCoder() == IntervalWindow.getCoder())

      val resource: ResourceId = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix)

      input.apply(
        TextIO
          .write()
          .to(new PerWindowFiles(resource))
          .withTempDirectory(resource.getCurrentDirectory())
          .withWindowedWrites()
          .withNumShards(3))
    }
  }

  /**
    * A FilenamePolicy produces a base file name for a write based on metadata about the data
    * being written. This always includes the shard number and the total number of shards. For
    * windowed writes, it also includes the window and pane index (a sequence number assigned to each
    * trigger firing).
    */
  class PerWindowFiles(prefix: ResourceId) extends FilenamePolicy {
    def filenamePrefixForWindow(window: IntervalWindow): String = {
      val filePrefix = if (prefix.isDirectory) "" else prefix.getFilename
      "%s-%s-%s".format(filePrefix, formatter.print(window.start()), formatter.print(window.end()))
    }

    override def windowedFilename(
        shardNumber: Int,
        numShards: Int,
        window: BoundedWindow,
        paneInfo: PaneInfo,
        outputFileHints: OutputFileHints): ResourceId = {
      val intervalWindow: IntervalWindow = window.asInstanceOf[IntervalWindow]
      val filename = "%s-%s-of-%s%s".format(
        filenamePrefixForWindow(intervalWindow),
        shardNumber,
        numShards,
        outputFileHints.getSuggestedFilenameSuffix())
      prefix.getCurrentDirectory().resolve(filename, StandardResolveOptions.RESOLVE_FILE)
    }

    override def unwindowedFilename(
        shardNumber: Int,
        numShards: Int,
        outputFileHints: OutputFileHints): ResourceId =
      throw new UnsupportedOperationException("Unsupported.")
  }
}

/** companion object */
object WriteToText {
  private final val formatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")))

  /**
    * A Serializable function from a DoFn.ProcessContext and BoundedWindow to
    * the value for that field.
    */
  trait FieldFn[InputT] extends Serializable {
    def apply(context: DoFn[InputT, String]#ProcessContext, window: BoundedWindow): Object
  }
}
