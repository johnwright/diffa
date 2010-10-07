/**
 * Copyright (C) 2010 LShift Ltd.
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

import org.joda.time.DateTime

/**
 * Trait supported by various RPC bindings providing communications with participants.
 */
trait Participant {
  /**
   * Retrieves aggregated details about a participant's native versions. The participant is expected to aggregate
   * and filter their data based on the provided parameters. The digest is expected to be built upon a key sorted list
   * of entries.
   */
  def queryDigests(start:DateTime, end:DateTime, granularity:RangeGranularity):Seq[VersionDigest]

  /**
   * This invokes a request against the participant for the the named action and supplies it with the id entity
   * for which the action should be executed on. 
   */
  def invoke(actionId:String, entityId:String) : ActionResult
}

trait UpstreamParticipant extends Participant {
  /**
   * Requests that the participant return a serialized form of the item with the given identifier.
   */
  def retrieveContent(identifier:String): String
}

trait DownstreamParticipant extends Participant {
  /**
   * Requests that the participant generate a processing response based on the given incoming entity data. The downstream
   * participant is not expected to re-admit the data into its system - it should simply parse the data so as to be able
   * to determine it's own version information.
   */
  def generateVersion(entityBody: String): ProcessingResponse
}