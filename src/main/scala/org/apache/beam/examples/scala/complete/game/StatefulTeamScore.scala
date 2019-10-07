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
import org.apache.beam.sdk.coders.VarIntCoder
import org.apache.beam.sdk.state.{StateSpec, StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
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
  import UserScore.GameActionInfo

  /**
    * Tracks each team's score separately in a single state cell and outputs the score every time it
    * passes a new multiple of a threshold.
    *
    * State is key-partitioned. Therefore, the score is calculated per team.
    * tateful DoFn can determine when to output based on the state. This only allows
    * outputting when a team's score passes a given threshold.
    */
  class UpdateTeamScoreFn(thresholdScore: Integer)
      extends DoFn[KV[String, GameActionInfo], KV[String, JInteger]] {
    import UpdateTeamScoreFn.TOTAL_SCORE

    /**
      * Describes the state for storing team score. Let's break down this statement.
      *
      * StateSpec configures the state cell, which is provided by a runner during pipeline
      * execution.
      *
      * org.apache.beam.sdk.transforms.DoFn.StateId annotation assigns an identifier to
      * the state, which is used to refer the state in org.apache.beam.sdk.transforms.DoFn.ProcessElement.
      *
      * A ValueState stores single value per key and per window. Because our pipeline is
      * globally windowed in this example, this ValueState is just key partitioned, with one
      * score per team. Any other class that extends org.apache.beam.sdk.state.State can be
      * used.
      *
      * In order to store the value, the state must be encoded. Therefore, we provide a coder, in
      * this case the VarIntCoder. If the coder is not provided as in
      * StateSpecs.value(), Beam's coder inference will try to provide a coder automatically.
      */
    @StateId(TOTAL_SCORE)
    private final val _: StateSpec[ValueState[JInteger]] =
      StateSpecs.value(VarIntCoder.of())

    /**
      * To use a state cell, annotate a parameter with org.apache.beam.sdk.transforms.DoFn.StateId
      * that matches the state declaration. The type of the parameter should match the StateSpec type.
      */
    @ProcessElement
    def processElement(
        ctx: ProcessContext,
        @StateId(TOTAL_SCORE) totalScore: ValueState[JInteger]): Unit = {
      val teamName = ctx.element().getKey()
      val gInfo: GameActionInfo = ctx.element().getValue()

      // ValueState cells do not contain a default value. If the state is possibly not written, make
      // sure to check for null on read.
      val oldTotalScore: Int = totalScore.read().toInt
      totalScore.write(oldTotalScore + gInfo.score)

      // Since there are no negative scores, the easiest way to check whether a team just passed a
      // new multiple of the threshold score is to compare the quotients of dividing total scores by
      // threshold before and after this aggregation. For example, if the total score was 1999,
      // the new total is 2002, and the threshold is 1000, 1999 / 1000 = 1, 2002 / 1000 = 2.
      // Therefore, this team passed the threshold.
      if (oldTotalScore / this.thresholdScore < totalScore.read() / this.thresholdScore) {
        ctx.output(KV.of(teamName, totalScore.read()))
      }
    }
  }

  /** companion object */
  object UpdateTeamScoreFn {
    private final val TOTAL_SCORE = "totalScore"
  }

  /**
    * Create a map of information that describes how to write pipeline output to BigQuery. This map
    * is used to write team score sums.
    */
  def configureCompleteWindowedTableWrite(): Map[String, FieldInfo[KV[String, JInteger]]] =
    LeaderBoard.configureWindowedTableWrite().removedAll(Array("timing", "window_start"))
}
