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
object UserScore {}
