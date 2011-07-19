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

package net.lshift.diffa.kernel.frontend

import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.matching.MatchingManager
import net.lshift.diffa.kernel.differencing.{SessionManager, VersionCorrelationStoreFactory}
import net.lshift.diffa.kernel.util.MissingObjectException
import net.lshift.diffa.kernel.actors.{ActivePairManager}
import net.lshift.diffa.kernel.participants.{EndpointLifecycleListener, InboundEndpointManager}
import net.lshift.diffa.kernel.scheduler.ScanScheduler

class Configuration(val configStore: ConfigStore,
                    val matchingManager: MatchingManager,
                    val versionCorrelationStoreFactory: VersionCorrelationStoreFactory,
                    val supervisor:ActivePairManager,
                    val sessionManager: SessionManager,
                    val endpointListener: EndpointLifecycleListener,
                    val scanScheduler: ScanScheduler) {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  def applyConfiguration(diffaConfig:DiffaConfig) = {
    // Ensure that the configuration is valid upfront
    diffaConfig.validate()
    
    // Apply configuration updates
    val removedProps = configStore.allConfigOptions.keys.filter(currK => !diffaConfig.properties.contains(currK))
    removedProps.foreach(p => configStore.clearConfigOption(p))
    diffaConfig.properties.foreach { case (k, v) => configStore.setConfigOption(k, v) }

    // Remove missing users, and create/update the rest
    val removedUsers = configStore.listUsers.filter(currU => diffaConfig.users.find(newU => newU.name == currU.name).isEmpty)
    removedUsers.foreach(u => deleteUser(u.name))
    diffaConfig.users.foreach(u => createOrUpdateUser(u))

    // Apply endpoint and pair updates
    diffaConfig.endpoints.foreach(e => createOrUpdateEndpoint(e))
    diffaConfig.pairs.foreach(p => createOrUpdatePair(p))

    // Remove missing repair actions, and create/update the rest
    val removedActions =
      configStore.listRepairActions.filter(a => diffaConfig.repairActions
        .find(newA => newA.name == a.name && newA.pairKey == a.pairKey).isEmpty)
    removedActions.foreach(a => deleteRepairAction(a.name, a.pairKey))
    diffaConfig.repairActions.foreach(createOrUpdateRepairAction)

    // Remove old pairs and endpoints
    val removedPairs = configStore.listPairs.filter(currP => diffaConfig.pairs.find(newP => newP.pairKey == currP.key).isEmpty)
    removedPairs.foreach(p => deletePair(p.key))
    var removedEndpoints = configStore.listEndpoints.filter(currE => diffaConfig.endpoints.find(newE => newE.name == currE.name).isEmpty)
    removedEndpoints.foreach(e => deleteEndpoint(e.name))
  }
  def retrieveConfiguration:DiffaConfig = {
    DiffaConfig(
      properties = configStore.allConfigOptions,
      users = configStore.listUsers.toSet,
      endpoints = configStore.listEndpoints.toSet,
      pairs = configStore.listPairs.map(
        p => PairDef(p.key, p.versionPolicyName, p.matchingTimeout, p.upstream.name, p.downstream.name, p.scanCronSpec)).toSet,
      repairActions = configStore.listRepairActions.toSet
    )
  }

  /*
  * Endpoint CRUD
  * */
  def declareEndpoint(endpoint: Endpoint): Unit = createOrUpdateEndpoint(endpoint)

  def createOrUpdateEndpoint(endpoint: Endpoint) = {
    log.debug("Processing endpoint declare/update request: " +  endpoint.name)
    endpoint.validate()
    configStore.createOrUpdateEndpoint(endpoint)
    endpointListener.onEndpointAvailable(endpoint)
  }

  def deleteEndpoint(name: String) = {
    log.debug("Processing endpoint delete request: " + name)
    configStore.deleteEndpoint(name)
    endpointListener.onEndpointRemoved(name)
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
  def getPair(x:String) = configStore.getPair(x)
  def getUser(x:String) = configStore.getUser(x)

  def createOrUpdateUser(u: User): Unit = {
    log.debug("Processing user declare/update request: " + u)
    u.validate()
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
    pairDef.validate()
    // Stop a running actor, if there is one
    withCurrentPair(pairDef, (k:String) => supervisor.stopActor(k) )
    configStore.createOrUpdatePair(pairDef)
    supervisor.startActor(configStore.getPair(pairDef.pairKey))
    matchingManager.onUpdatePair(pairDef.pairKey)
    scanScheduler.onUpdatePair(pairDef.pairKey)
    sessionManager.onUpdatePair(pairDef.pairKey)
  }

  def deletePair(key: String): Unit = {
    log.debug("Processing pair delete request: " + key)
    supervisor.stopActor(key)
    configStore.deletePair(key)
    versionCorrelationStoreFactory.remove(key)
    matchingManager.onDeletePair(key)
    scanScheduler.onDeletePair(key)
    sessionManager.onDeletePair(key)
  }

  def withCurrentPair(pairDef: PairDef, f:Function1[String,Unit]) = {
    try {
      val current = configStore.getPair(pairDef.pairKey)
      f(current.key)
    }
    catch {
      case e:MissingObjectException => // Do nothing, the pair doesn't currently exist
    }
  }

  def declareRepairAction(action: RepairAction) {
    createOrUpdateRepairAction(action)
  }

  def createOrUpdateRepairAction(action: RepairAction) {
    log.debug("Processing repair action declare/update request: " + action.name)
    action.validate()
    configStore.createOrUpdateRepairAction(action)
  }

  def deleteRepairAction(name: String, pairKey: String) {
    log.debug("Processing repair action delete request: (name="+name+", pairKey="+pairKey+")")
    configStore.deleteRepairAction(name, pairKey)
  }

  def listRepairActions: Seq[RepairAction] = {
    log.debug("Processing repair action list request")
    configStore.listRepairActions
  }

  def listRepairActionsForPair(pairKey: String): Seq[RepairAction] = {
    val pair = configStore.getPair(pairKey)
    configStore.listRepairActionsForPair(pair)
  }

  def createOrUpdateEscalation(escalation: Escalation) {
    log.debug("Processing escalation declare/update request: " + escalation.name)
    escalation.validate()
    configStore.createOrUpdateEscalation(escalation)
  }

  def deleteEscalation(name: String, pairKey: String) {
    log.debug("Processing escalation delete request: (name="+name+", pairKey="+pairKey+")")
    configStore.deleteEscalation(name, pairKey)
  }

  def listEscalations: Seq[Escalation] = {
    log.debug("Processing escalation list request")
    configStore.listEscalations
  }

  def listEscalationForPair(pairKey: String): Seq[Escalation] = {
    val pair = configStore.getPair(pairKey)
    configStore.listEscalationsForPair(pair)
  }
}
