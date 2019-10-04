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

/**
  * This class is the fourth in a series of four pipelines that tell a story in a 'gaming' domain,
  * following UserScore, HourlyTeamScore, and LeaderBoard. New concepts:
  * session windows and finding session duration; use of both singleton and non-singleton side
  * inputs.
  *
  * This pipeline builds on the LeaderBoard functionality, and adds some "business
  * intelligence" analysis: abuse detection and usage patterns. The pipeline derives the Mean user
  * score sum for a window, and uses that information to identify likely spammers/robots. (The robots
  * have a higher click rate than the human users). The 'robot' users are then filtered out when
  * calculating the team scores.
  *
  * Additionally, user sessions are tracked: that is, we find bursts of user activity using
  * session windows. Then, the mean session duration information is recorded in the context of
  * subsequent fixed windowing. (This could be used to tell us what games are giving us greater user
  * retention).
  *
  * Run org.apache.beam.examples.complete.game.injector.Injector to generate pubsub data
  * for this pipeline. The Injector documentation provides more detail.
  *
  * The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
  * the same topic to which the Injector is publishing.
  */
object GameStats {}
