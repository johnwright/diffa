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

package net.lshift.diffa.kernel.actors

import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.events.PairChangeEvent
import net.lshift.diffa.kernel.differencing.{DifferencingListener, VersionPolicy}
import se.scalablesolutions.akka.actor.{ActorRegistry, Actor}
import net.jcip.annotations.ThreadSafe
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant}

/**
 * This actor serializes access to the underlying version policy from concurrent processes.
 */
class PairActor(val pairKey:String,
                val us:UpstreamParticipant,
                val ds:DownstreamParticipant,
                val policy:VersionPolicy) extends Actor {

  val logger:Logger = LoggerFactory.getLogger(getClass)

  self.id_=(pairKey)

  def receive = {
    case ChangeMessage(event)               => policy.onChange(event)
    case DifferenceMessage(listener) => {
      policy.difference(pairKey, us, ds, listener)
    }
  }
}

case class ChangeMessage(event:PairChangeEvent)
case class DifferenceMessage(listener:DifferencingListener)

/**
 * This is a thread safe entry point to an underlying version policy.
 */
@ThreadSafe
trait PairPolicyClient {

  /**
   * Propagates the change event to the underlying policy implementation in a serial fashion.
   */
  def propagateChangeEvent(event:PairChangeEvent) : Unit

  /**
   * Runs a syncing difference report on the underlying policy implementation in a thread safe way.
   */
  def syncPair(pairKey:String, listener:DifferencingListener) : Unit
}
