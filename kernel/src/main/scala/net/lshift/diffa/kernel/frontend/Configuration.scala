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

package net.lshift.diffa.kernel.frontend

import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.matching.MatchingManager
import net.lshift.diffa.kernel.differencing.SessionManager
import net.lshift.diffa.kernel.actors.PairActorSupervisor

class Configuration(val configStore: ConfigStore,
                    val matchingManager: MatchingManager,
                    val supervisor:PairActorSupervisor,
                    val sessionManager: SessionManager) {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  /*
  * Endpoint CRUD
  * */
  def declareEndpoint(endpoint: Endpoint): Unit = createOrUpdateEndpoint(endpoint)

  def createOrUpdateEndpoint(endpoint: Endpoint) = {
    log.debug("Processing endpoint declare/update request: " +  endpoint.name)
    configStore.createOrUpdateEndpoint(endpoint)
  }

  def deleteEndpoint(name: String) = {
    log.debug("Processing endpoint delete request: " + name)
    configStore.deleteEndpoint(name)
  }

  def listEndpoints: Seq[Endpoint] = {
    log.debug("Processing endpoint list request")
    configStore.listEndpoints
  }

  def listUsers: Seq[User] = {
    log.debug("Processing endpoint list users")
    configStore.listUsers
  }

  // TODO There is no particular reason why these are just passed through
  // basically the value of this Configuration frontend is that the matching Manager
  // is invoked when you perform CRUD ops for pairs
  // This might have to get refactored in light of the fact that we are now pretty much
  // just using REST to configure the agent
  def getEndpoint(x:String) = configStore.getEndpoint(x)
  def getGroup(x:String) = configStore.getGroup(x)
  def getPair(x:String) = configStore.getPair(x)
  def getUser(x:String) = configStore.getUser(x)

  def createOrUpdateUser(u: User): Unit = {
    log.debug("Processing user declare/update request: " + u)
    configStore.createOrUpdateUser(u)
  }

  def deleteUser(name: String): Unit = {
    log.debug("Processing user delete request: " + name)
    configStore.deleteUser(name)
  }
  /*
  * Pair CRUD
  * */
  def declarePair(pairDef: PairDef): Unit = createOrUpdatePair(pairDef)

  def createOrUpdatePair(pairDef: PairDef): Unit = {
    log.debug("Processing pair declare/update request: " + pairDef.pairKey)
    configStore.createOrUpdatePair(pairDef)
    supervisor.startActor(configStore.getPair(pairDef.pairKey))
    matchingManager.onUpdatePair(pairDef.pairKey)
    sessionManager.onUpdatePair(pairDef.pairKey)
  }

  def deletePair(key: String): Unit = {
    log.debug("Processing pair delete request: " + key)
    supervisor.stopActor(key)
    configStore.deletePair(key)
    matchingManager.onDeletePair(key)
  }

  /*
  * Pair group CRUD
  * */
  def createOrUpdateGroup(group: PairGroup): Unit = {
    log.debug("Processing group declare/update request: " + group.key)
    configStore.createOrUpdateGroup(group)
  }

  def deleteGroup(key: String): Unit = {
    log.debug("Processing group delete request: " + key)
    configStore.deleteGroup(key)
  }

  def listGroups: Seq[GroupContainer] = {
    log.debug("Processing group list request")
    configStore.listGroups
  }
}

abstract class KernelMessage
case class AuthRequest(username: String, password: String) extends KernelMessage
case class AuthResponse(response: Int) extends KernelMessage