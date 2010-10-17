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

package net.lshift.diffa.kernel.differencing

import se.scalablesolutions.akka.actor.ActorRegistry
import se.scalablesolutions.akka.actor.Actor._
import org.slf4j.LoggerFactory


class CorrelationActorSupervisor(store:VersionCorrelationStore) {

  private val log = LoggerFactory.getLogger(getClass)

  def startActor(key:String) = {
    val actors = ActorRegistry.actorsFor(key)
    actors.length match {
      case 0 => {
        ActorRegistry.register(actorOf(new CorrelationActor(key, store)).start)
        log.info("Started actor for key: " + key)
      }
      case 1    => log.warn("Attempting to re-spawn actor for key: " + key)
      case x    => log.error("Too many actors for key: " + key + "; actors = " + x)
    }
  }

  def stopActor(key:String) = {
    val actors = ActorRegistry.actorsFor(key)
    actors.length match {
      case 1 => {
        ActorRegistry.unregister(actors(0))
        log.info("Stopped actor for key: " + key)
      }
      case 0    => log.warn("Could not resolve actor for key: " + key)
      case x    => log.error("Too many actors for key: " + key + "; actors = " + x)
    }
  }
}