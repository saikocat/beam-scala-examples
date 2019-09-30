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
package org.apache.beam.examples.scala.complete.game

import org.apache.beam.examples.scala.complete.game.utils.{GameConstants, WriteToText}
import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.KV

/**
  * This class is the second in a series of four pipelines that tell a story in a 'gaming' domain,
  * following UserScore. In addition to the concepts introduced in UserScore, new
  * concepts include: windowing and element timestamps; use of Filter.by().
  *
  * This pipeline processes data collected from gaming events in batch, building on
  * UserScore} but using fixed windows. It calculates the sum of scores per team, for each window,
  * optionally allowing specification of two timestamps before and after which data is filtered out.
  * This allows a model where late data collected after the intended analysis window can be included,
  * and any late-arriving data prior to the beginning of the analysis window can be removed as well.
  * By using windowing and adding element timestamps, we can do finer-grained analysis than with the
  * UserScore pipeline. However, our batch processing is high-latency, in that we don't get
  * results from plays at the beginning of the batch's time period until the batch is processed.
  */
object HourlyTeamScore {

  /**
    * Create a map of information that describes how to write pipeline output to text. This map is
    * passed to the WriteToText constructor to write team score sums and includes information
    * about window start time.
    */
  protected def configureOutput(): Map[String, WriteToText.FieldFn[KV[String, JInteger]]] =
    UserScore.configureOutput() ++ Map[String, WriteToText.FieldFn[KV[String, JInteger]]](
      "window_start" -> { (_, boundedWindow) =>
        {
          val window = boundedWindow.asInstanceOf[IntervalWindow]
          GameConstants.DATE_TIME_FORMATTER.print(window.start())
        }
      }
    )
}
