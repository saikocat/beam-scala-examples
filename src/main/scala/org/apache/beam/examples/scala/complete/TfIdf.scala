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

import java.net.URI
import java.io.File

import scala.collection.JavaConverters._

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{KvCoder, StringDelegateCoder, StringUtf8Coder}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.values.{KV, PBegin, PCollection, PCollectionList}
import org.apache.beam.sdk.transforms.{Flatten, PTransform, WithKeys}

/**
  * An example that computes a basic TF-IDF search table for a directory or GCS prefix.
  *
  * Concepts: joining data; side inputs; logging
  */
object TfIdf {

  /**
    * Reads the documents at the provided uris and returns all lines from the documents tagged with
    * which document they are from.
    */
  class ReadDocuments(uris: JIterable[URI])
      extends PTransform[PBegin, PCollection[KV[URI, String]]] {
    override def expand(input: PBegin): PCollection[KV[URI, String]] = {
      val pipeline: Pipeline = input.getPipeline()

      // Create one TextIO.Read transform for each document
      // and add its output to a PCollectionList
      val urisToLines: PCollectionList[KV[URI, String]] =
        uris.asScala.foldLeft(PCollectionList.empty[KV[URI, String]](pipeline))((acc, uri) => {
          val uriString = uriToString(uri)
          val oneUriToLines: PCollection[KV[URI, String]] =
            pipeline
              .apply(s"TextIO.Read($uriString)", TextIO.read().from(uriString))
              .apply(s"WithKeys($uriString)", WithKeys.of(uri))
              .setCoder(KvCoder.of(StringDelegateCoder.of(classOf[URI]), StringUtf8Coder.of()))
          acc.and(oneUriToLines)
        })

      urisToLines.apply(Flatten.pCollections())
    }

    // TextIO.Read supports:
    //  - file: URIs and paths locally
    //  - gs: URIs on the service
    def uriToString(uri: URI): String = uri.getScheme match {
      case "file" => new File(uri).getPath
      case _ => uri.toString
    }
  }
}
