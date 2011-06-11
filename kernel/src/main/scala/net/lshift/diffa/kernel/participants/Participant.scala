/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.kernel.participants

import java.io.Closeable
import net.lshift.diffa.kernel.frontend.wire.InvocationResult

/**
 * Trait supported by various RPC bindings providing communications with participants.
 */
trait Participant extends Closeable
{
  /**
   * Retrieves aggregated details about a participant's native versions. The participant is expected to aggregate
   * and filter their data based on the provided constraints. The digest is expected to be built upon well known
   * functions that are embodied in each QueryConstraint. Note that when no QueryConstraint is specified, i.e.
   * an empty list is passed in, the entire data set of the participant will be returned.
   */
  def queryAggregateDigests(bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint]) : Seq[AggregateDigest]

  /**
   * Retrieves details about a participant's native versions at the level of individual entities. The participant
   * is expected to filter their data based on the provided constraints. Note that when no QueryConstraint is specified, i.e.
   * an empty list is passed in, the entire data set of the participant will be returned.
   */
  def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion]

  /**
   * Requests that the participant return a serialized form of the item with the given identifier.
   */
  def retrieveContent(identifier:String): String
}

trait UpstreamParticipant extends Participant {}

trait DownstreamParticipant extends Participant {
  /**
   * Requests that the participant generate a processing response based on the given incoming entity data. The downstream
   * participant is not expected to re-admit the data into its system - it should simply parse the data so as to be able
   * to determine it's own version information.
   */
  def generateVersion(entityBody: String): ProcessingResponse
}

object ParticipantType extends Enumeration {
  type ParticipantType = Value
  val UPSTREAM = Value("upstream")
  val DOWNSTREAM = Value("downstream")
}