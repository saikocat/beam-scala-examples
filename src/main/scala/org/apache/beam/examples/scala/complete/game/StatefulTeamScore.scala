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

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.examples.scala.complete.game.utils.WriteToBigQuery.FieldInfo
import org.apache.beam.sdk.values.KV

/**
  * This class is part of a series of pipelines that tell a story in a gaming domain. Concepts
  * include: stateful processing.
  *
  * This pipeline processes an unbounded stream of 'game events'. It uses stateful processing to
  * aggregate team scores per team and outputs team name and it's total score every time the team
  * passes a new multiple of a threshold score. For example, multiples of the threshold could be the
  * corresponding scores required to pass each level of the game. By default, this threshold is set
  * to 5000.
  *
  * Stateful processing allows us to write pipelines that output based on a runtime state (when a
  * team reaches a certain score, in every 100 game events etc) without time triggers. See
  * https://beam.apache.org/blog/2017/02/13/stateful-processing.html for more information on using
  * stateful processing.
  */
object StatefulTeamScore {

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write team score sums.
   */
  def configureCompleteWindowedTableWrite():  Map[String, FieldInfo[KV[String, JInteger]]] =
    LeaderBoard.configureWindowedTableWrite().removedAll(Array("timing", "window_start"))
}
