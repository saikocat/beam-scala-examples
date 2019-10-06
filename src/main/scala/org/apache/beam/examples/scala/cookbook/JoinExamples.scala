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
package org.apache.beam.examples.scala.cookbook

import scala.jdk.CollectionConverters._

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.{
  Description,
  PipelineOptions,
  PipelineOptionsFactory,
  Validation
}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag}

object JoinExamples {
  // A 1000-row sample of the GDELT data here: gdelt-bq:full.events.
  private final val GDELT_EVENTS_TABLE = "clouddataflow-readonly:samples.gdelt_sample"
  // A table that maps country codes to country names.
  private final val COUNTRY_CODES = "gdelt-bq:full.crosswalk_geocountrycodetohuman"

  @throws(classOf[Exception])
  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline = Pipeline.create(options)
    // the following two 'applys' create multiple inputs to our pipeline, one for each
    // of our two input sources.
    val eventsTable: PCollection[TableRow] =
      pipeline.apply(BigQueryIO.readTableRows().from(GDELT_EVENTS_TABLE))
    val countryCodes: PCollection[TableRow] =
      pipeline.apply(BigQueryIO.readTableRows().from(COUNTRY_CODES))
    val formattedResults: PCollection[String] = joinEvents(eventsTable, countryCodes)
    formattedResults.apply(TextIO.write().to(options.getOutput))
    pipeline.run().waitUntilFinish()
    ()
  }

  trait Options extends PipelineOptions {
    @Description("Path of the file to write to")
    @Validation.Required
    def getOutput: String
    def setOutput(value: String): Unit
  }

  /** Join two collections, using country code as the key. */
  @throws(classOf[Exception])
  def joinEvents(
      eventsTable: PCollection[TableRow],
      countryCodes: PCollection[TableRow]): PCollection[String] = {
    val eventInfoTag: TupleTag[String] = new TupleTag[String]()
    val countryInfoTag: TupleTag[String] = new TupleTag[String]()

    // transform both input collections to tuple collections, where the keys are country
    // codes in both cases.
    val eventInfo: PCollection[KV[String, String]] =
      eventsTable.apply(ParDo.of(new ExtractEventDataFn()))
    val countryInfo: PCollection[KV[String, String]] =
      countryCodes.apply(ParDo.of(new ExtractCountryInfoFn()))

    // country code 'key' -> CGBKR (<event info>, <country name>)
    val kvpCollection: PCollection[KV[String, CoGbkResult]] =
      KeyedPCollectionTuple
        .of(eventInfoTag, eventInfo)
        .and(countryInfoTag, countryInfo)
        .apply(CoGroupByKey.create())

    // Process the CoGbkResult elements generated by the CoGroupByKey transform.
    // country code 'key' -> string of <event info>, <country name>
    val finalResultCollection: PCollection[KV[String, String]] =
      kvpCollection.apply("Process", ParDo.of(new ProcessCoResultFn(countryInfoTag, eventInfoTag)))

    // write to GCS
    val formattedResults: PCollection[String] =
      finalResultCollection.apply("Format", ParDo.of(new FormatResultFn()))
    return formattedResults
  }

  /**
    * Examines each row (event) in the input table. Output a KV with the key the country code of the
    * event, and the value a string encoding event information.
    */
  class ExtractEventDataFn extends DoFn[TableRow, KV[String, String]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val countryCode = row.get("ActionGeo_CountryCode").toString
      val sqlDate = row.get("SQLDATE").toString
      val actor1Name = row.get("Actor1Name").toString
      val sourceUrl = row.get("SOURCEURL").toString
      val eventInfo = s"Date: $sqlDate, Actor1: $actor1Name, url: $sourceUrl"
      ctx.output(KV.of(countryCode, eventInfo))
    }
  }

  /**
    * Examines each row (country info) in the input table. Output a KV with the key the country code,
    * and the value the country name.
    */
  class ExtractCountryInfoFn extends DoFn[TableRow, KV[String, String]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val countryCode = row.get("FIPSCC").toString
      val countryName = row.get("HumanName").toString
      ctx.output(KV.of(countryCode, countryName))
    }
  }

  /**
    * FormatResultFn to avoid linter unused method
    */
  class FormatResultFn extends DoFn[KV[String, String], String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val outputstring = s"Country code: ${ctx.element.getKey}, ${ctx.element.getValue}"
      ctx.output(outputstring)
    }
  }

  class ProcessCoResultFn(countryInfoTag: TupleTag[String], eventInfoTag: TupleTag[String])
      extends DoFn[KV[String, CoGbkResult], KV[String, String]] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val elm: KV[String, CoGbkResult] = ctx.element
      val countryCode = elm.getKey
      val countryName = elm.getValue.getOnly(countryInfoTag, "none")
      for (eventInfo <- ctx.element.getValue.getAll(eventInfoTag).asScala) {
        // Generate a string that combines information from both collection values
        ctx.output(KV.of(countryCode, s"Country name: $countryName, Event info: $eventInfo"))
      }
    }
  }
}
