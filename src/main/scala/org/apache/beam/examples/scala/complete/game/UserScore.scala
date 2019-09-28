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

import java.util.Objects

import scala.util.{Failure, Success, Try}

import org.apache.beam.examples.scala.typealias._
import org.apache.beam.sdk.coders.{AvroCoder, DefaultCoder}
import org.apache.beam.sdk.metrics.{Counter, Metrics}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.slf4j.{Logger, LoggerFactory}

/**
  * This class is the first in a series of four pipelines that tell a story in a 'gaming' domain.
  * Concepts: batch processing, reading input from text files, writing output to text files, using
  * standalone DoFns, use of the sum per key transform.
  *
  * In this gaming scenario, many users play, as members of different teams, over the course of a
  * day, and their actions are logged for processing. Some of the logged game events may be late-
  * arriving, if users play on mobile devices and go transiently offline for a period.
  *
  * This pipeline does batch processing of data collected from gaming events. It calculates the
  * sum of scores per user, over an entire batch of gaming data (collected, say, for each day). The
  * batch processing will not include any late data that arrives after the day's cutoff point.
  */
object UserScore {

  /** Class to hold info about a game event. */
  @DefaultCoder(classOf[AvroCoder[GameActionInfo]])
  case class GameActionInfo(user: String, team: String, score: JInteger, timestamp: JLong) {
    def this() {
      this("", "", 0, 0L)
    }

    def getKey(keyname: String): String =
      keyname match {
        case "team" => this.team
        case _ => this.user
      }

    override def hashCode: Int = Objects.hash(user, team, score, timestamp)
  }

  /**
    * Parses the raw game event info into GameActionInfo objects. Each event line has the following
    * format: username,teamname,score,timestamp_in_ms,readable_time e.g.:
    * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224 The human-readable
    * time string is not used here.
    */
  class ParseEventFn extends DoFn[String, GameActionInfo] {
    import ParseEventFn.LOG

    private final val numParseErrors: Counter = Metrics.counter("main", "ParseErrors")

    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val components = ctx.element.split(",", -1)

      val gInfo: Try[GameActionInfo] = for {
        score <- Try(components(2).trim().toInt)
        timestamp <- Try(components(3).trim().toLong)
        user = components(0).trim()
        team = components(1).trim()
      } yield new GameActionInfo(user, team, score, timestamp)

      gInfo match {
        case Success(result) => ctx.output(result)
        case f @ (Failure(_: ArrayIndexOutOfBoundsException) | Failure(_: NumberFormatException)) =>
          numParseErrors.inc()
          LOG.info(s"Parse error on ${ctx.element}, ${f.failed.get.getMessage}")
        case Failure(e) => LOG.error(s"Error encountered on ${ctx.element}, ${e.getMessage}")
      }
    }
  }

  /** Companion object for ParseEventFn */
  object ParseEventFn {
    // Log and count parse errors.
    private final val LOG: Logger = LoggerFactory.getLogger(classOf[ParseEventFn])
  }
}
