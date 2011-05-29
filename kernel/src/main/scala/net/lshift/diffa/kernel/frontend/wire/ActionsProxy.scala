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

package net.lshift.diffa.kernel.frontend.wire

import net.lshift.diffa.kernel.participants.ParticipantFactory
import net.lshift.diffa.kernel.client.{ActionableRequest, ActionsClient}
import net.lshift.diffa.kernel.config.{Actionable, Pair, ConfigStore}

/**
 * This is a conduit to the actions that are provided by participants
 */
class ActionsProxy(val config:ConfigStore, val factory:ParticipantFactory) extends ActionsClient {

  // TODO For the initial release we will hardcoded only the resend action (see ticket #39)
  def listActions(pairKey: String) : Seq[Actionable] = {
    val path = "/actions/" + pairKey + "/resend/${id}"
    def resend() = Array(Actionable("action-1", "Resend Source", "item", path, pairKey))
    withValidPair(pairKey, resend)
  }

  def invoke(request:ActionableRequest) : InvocationResult = {

    def result() = {
      val pair = config.getPair(request.pairKey)
      // TODO I think this creates a new instance for each invocation, not sure whether this is an issue or not
      val participant = factory.createUpstreamParticipant(pair.upstream)
      participant.invoke(request.actionId, request.entityId)
    }
    
    withValidPair(request.pairKey, result)
  }

  def withValidPair[T](pair:String, f: () => T) = {
    config.getPair(pair)
    f()
  }

}