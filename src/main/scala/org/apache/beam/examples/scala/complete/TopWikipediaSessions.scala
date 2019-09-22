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

import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.extensions.gcp.util.Transport
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.SimpleFunction
import org.joda.time.Instant

/**
  * An example that reads Wikipedia edit data from Cloud Storage and computes the user with the
  * longest string of edits separated by no more than an hour within each month.
  *
  * Concepts: Using Windowing to perform time-based aggregations of data.
  */
object TopWikipediaSessions {
  private final val EXPORTED_WIKI_TABLE = "gs://apache-beam-samples/wikipedia_edits/*.json"

  class ParseTableRowJson extends SimpleFunction[String, TableRow] {
    override def apply(input: String): TableRow =
      // whew... do hope they have an example that handles failure with tags
      // instead of a rude runtime exception
      try {
        Transport.getJsonFactory.fromString(input, classOf[TableRow])
      } catch {
        case e: java.io.IOException =>
          throw new RuntimeException("Failed parsing table row json", e)
      }
  }

  /** Extracts user and timestamp from a TableRow representing a Wikipedia edit. */
  class ExtractUserAndTimestamp extends DoFn[TableRow, String] {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val row: TableRow = ctx.element
      val timestamp: Int = row.get("timestamp") match {
        case t: java.math.BigDecimal => t.intValue()
        case t: JInteger => t.intValue()
      }
      val userName = row.get("contributor_username").asInstanceOf[String]
      if (Option(userName).nonEmpty) {
        // Sets the implicit timestamp field to be used in windowing.
        ctx.outputWithTimestamp(userName, new Instant(timestamp * 1000L))
      }
    }
  }
}
