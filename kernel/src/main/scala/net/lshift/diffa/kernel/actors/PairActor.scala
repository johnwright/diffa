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

package net.lshift.diffa.kernel.actors

import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.events.PairChangeEvent
import net.lshift.diffa.kernel.differencing.{DifferencingListener, VersionPolicy}
import se.scalablesolutions.akka.actor.{ActorRef, ActorRegistry, Actor}
import net.jcip.annotations.ThreadSafe
import net.lshift.diffa.kernel.participants.{QueryConstraint, DownstreamParticipant, UpstreamParticipant}

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
    case DifferenceMessage(constraints, listener) => {
      logger.debug("Running difference report for: " + constraints)
      val sync = policy.difference(pairKey, constraints, us, ds, listener)
      self.reply(sync)
    }
  }
}

case class ChangeMessage(event:PairChangeEvent)
case class DifferenceMessage(constraints:Seq[QueryConstraint], listener:DifferencingListener)

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
  def syncPair(pairKey:String, constraints:Seq[QueryConstraint], listener:DifferencingListener) : Boolean
}

class DefaultPairPolicyClient extends PairPolicyClient {

  val log = LoggerFactory.getLogger(getClass)

  def propagateChangeEvent(event:PairChangeEvent) = findActor(event.id.pairKey) ! ChangeMessage(event)

  def syncPair(pairKey:String, constraints:Seq[QueryConstraint], listener:DifferencingListener) = {
    val result = findActor(pairKey) !! DifferenceMessage(constraints, listener)
    result match {
      case Some(b) => b.asInstanceOf[Boolean]
      case None => {
        log.error("Message timeout")
        throw new RuntimeException("Message timeout")
      }
    }
  }

  def findActor(pairKey:String) = {
    val actors = ActorRegistry.actorsFor(pairKey)
    actors.length match {
      case 1 => actors(0)
      case 0 => {
        log.error("Could not resolve actor for key: " + pairKey)
        throw new RuntimeException("Unresolvable pair: " + pairKey)
      }
      case x => {
        log.error("Too many actors for key: " + pairKey + "; actors = " + x)
        throw new RuntimeException("Too many actors: " + pairKey)
      }
    }
  }
}
