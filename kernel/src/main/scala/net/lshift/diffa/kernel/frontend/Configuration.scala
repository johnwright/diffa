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
import net.lshift.diffa.kernel.participants.EndpointLifecycleListener
import net.lshift.diffa.kernel.scheduler.ScanScheduler
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}

class Configuration(val configStore: DomainConfigStore,
                    val matchingManager: MatchingManager,
                    val versionCorrelationStoreFactory: VersionCorrelationStoreFactory,
                    val supervisor:ActivePairManager,
                    val sessionManager: SessionManager,
                    val endpointListener: EndpointLifecycleListener,
                    val scanScheduler: ScanScheduler) {

  private val log:Logger = LoggerFactory.getLogger(getClass)

  def applyConfiguration(domain:String, diffaConfig:DiffaConfig) = {
    // Ensure that the configuration is valid upfront
    diffaConfig.validate()
    
    // Apply configuration updates
    val removedProps = configStore.allConfigOptions(domain).keys.filter(currK => !diffaConfig.properties.contains(currK))
    removedProps.foreach(p => configStore.clearConfigOption(domain, p))
    diffaConfig.properties.foreach { case (k, v) => configStore.setConfigOption(domain, k, v) }

    // Remove missing users, and create/update the rest
    val removedUsers = configStore.listUsers(domain).filter(currU => diffaConfig.users.find(newU => newU.name == currU.name).isEmpty)
    removedUsers.foreach(u => deleteUser(domain, u.name))
    diffaConfig.users.foreach(u => createOrUpdateUser(domain, u))

    // Apply endpoint and pair updates
    diffaConfig.endpoints.foreach(e => createOrUpdateEndpoint(domain, e))
    diffaConfig.pairs.foreach(p => createOrUpdatePair(domain, p))

    // Remove missing repair actions, and create/update the rest
    val removedActions =
      configStore.listRepairActions(domain).filter(a => diffaConfig.repairActions
        .find(newA => newA.name == a.name && newA.pair == a.pair).isEmpty)
    removedActions.foreach(a => deleteRepairAction(domain, a.name, a.pair.key))
    diffaConfig.repairActions.foreach(a => createOrUpdateRepairAction(domain,a))
      
    // Remove missing escalations, and create/update the rest
    var removedEscalations =
      configStore.listEscalations(domain).filter(e => diffaConfig.escalations
        .find(newE => newE.name == e.name && newE.pair == e.pair).isEmpty)
    removedEscalations.foreach(e => deleteEscalation(domain, e.name, e.pair.key))
    diffaConfig.escalations.foreach(createOrUpdateEscalation(domain,_))

    // Remove old pairs and endpoints
    val removedPairs = configStore.listPairs(domain).filter(currP => diffaConfig.pairs.find(newP => newP.pairKey == currP.key).isEmpty)
    removedPairs.foreach(p => deletePair(domain, p.key))
    var removedEndpoints = configStore.listEndpoints(domain).filter(currE => diffaConfig.endpoints.find(newE => newE.name == currE.name).isEmpty)
    removedEndpoints.foreach(e => deleteEndpoint(domain, e.name))
  }
  def retrieveConfiguration(domain:String) : DiffaConfig = {
    DiffaConfig(
      properties = configStore.allConfigOptions(domain),
      users = configStore.listUsers(domain).toSet,
      endpoints = configStore.listEndpoints(domain).toSet,
      pairs = configStore.listPairs(domain).map(
        p => PairDef(p.key, p.domain.name, p.versionPolicyName, p.matchingTimeout, p.upstream.name, p.downstream.name, p.scanCronSpec)).toSet,
      repairActions = configStore.listRepairActions(domain).toSet
    )
  }

  /*
  * Endpoint CRUD
  * */
  def declareEndpoint(domain:String, endpoint: Endpoint): Unit = createOrUpdateEndpoint(domain, endpoint)

  def createOrUpdateEndpoint(domain:String, endpoint: Endpoint) = {
    log.debug("[%s] Processing endpoint declare/update request: %s".format(domain,endpoint.name))
    endpoint.validate()
    configStore.createOrUpdateEndpoint(domain, endpoint)
    endpointListener.onEndpointAvailable(endpoint)
  }

  def deleteEndpoint(domain:String, endpoint: String) = {
    log.debug("[%s] Processing endpoint delete request: %s".format(domain,endpoint))
    configStore.deleteEndpoint(domain, endpoint)
    endpointListener.onEndpointRemoved(endpoint)
  }

  def listEndpoints(domain:String) : Seq[Endpoint] = configStore.listEndpoints(domain)
  def listUsers(domain:String) : Seq[User] = configStore.listUsers(domain)

  // TODO There is no particular reason why these are just passed through
  // basically the value of this Configuration frontend is that the matching Manager
  // is invoked when you perform CRUD ops for pairs
  // This might have to get refactored in light of the fact that we are now pretty much
  // just using REST to configure the agent
  def getEndpoint(domain:String, x:String) = configStore.getEndpoint(domain, x)
  def getPair(domain:String, x:String) = configStore.getPair(domain, x)
  def getUser(domain:String, x:String) = configStore.getUser(domain, x)

  def createOrUpdateUser(domain:String, u: User): Unit = {
    log.debug("[%s] Processing user declare/update request: %s".format(domain, u))
    u.validate()
    configStore.createOrUpdateUser(domain, u)
  }

  def deleteUser(domain:String, name: String): Unit = {
    log.debug("[%s] Processing user delete request: %s".format(domain,name))
    configStore.deleteUser(domain, name)
  }
  /*
  * Pair CRUD
  * */
  def declarePair(domain:String, pairDef: PairDef): Unit = createOrUpdatePair(domain, pairDef)

  def createOrUpdatePair(domain:String, pairDef: PairDef): Unit = {
    log.debug("[%s] Processing pair declare/update request: %s".format(domain,pairDef.pairKey))
    pairDef.validate()
    // Stop a running actor, if there is one
    withCurrentPair(domain, pairDef.pairKey, (p:DiffaPair) => supervisor.stopActor(p) )
    configStore.createOrUpdatePair(domain, pairDef)
    withCurrentPair(domain, pairDef.pairKey, (p:DiffaPair) => {
      supervisor.startActor(p)
      matchingManager.onUpdatePair(p)
      sessionManager.onUpdatePair(p)
    })

    scanScheduler.onUpdatePair(domain, pairDef.pairKey)

  }

  def deletePair(domain:String, key: String): Unit = {
    log.debug("Processing pair delete request: " + key)
    withCurrentPair(domain, key, (p:DiffaPair) => {
      supervisor.stopActor(p)
      matchingManager.onDeletePair(p)
      sessionManager.onDeletePair(p)
      versionCorrelationStoreFactory.remove(p)
    })
    configStore.deletePair(domain, key)
    scanScheduler.onDeletePair(domain, key)

  }

  def withCurrentPair(domain:String, pairKey: String, f:Function1[DiffaPair,Unit]) = {
    try {
      val current = configStore.getPair(domain, pairKey)
      f(current)
    }
    catch {
      case e:MissingObjectException => // Do nothing, the pair doesn't currently exist
    }
  }

  def declareRepairAction(domain:String, action: RepairAction) {
    createOrUpdateRepairAction(domain, action)
  }

  def createOrUpdateRepairAction(domain:String, action: RepairAction) {
    log.debug("Processing repair action declare/update request: " + action.name)
    action.validate()
    configStore.createOrUpdateRepairAction(domain, action)
  }

  def deleteRepairAction(domain:String, name: String, pairKey: String) {
    log.debug("Processing repair action delete request: (name="+name+", pairKey="+pairKey+")")
    configStore.deleteRepairAction(domain, name, pairKey)
  }

  def listRepairActions (domain:String) : Seq[RepairAction] = {
    log.debug("Processing repair action list request")
    configStore.listRepairActions(domain)
  }

  def listRepairActionsForPair(domain:String, pairKey: String): Seq[RepairAction] = {
    val pair = configStore.getPair(domain, pairKey)
    configStore.listRepairActionsForPair(domain, pair)
  }

  def createOrUpdateEscalation(domain:String, escalation: Escalation) {
    log.debug("Processing escalation declare/update request: " + escalation.name)
    escalation.validate()
    configStore.createOrUpdateEscalation(domain, escalation)
  }

  def deleteEscalation(domain:String, name: String, pairKey: String) {
    log.debug("Processing escalation delete request: (name="+name+", pairKey="+pairKey+")")
    configStore.deleteEscalation(domain, name, pairKey)
  }

  def listEscalations(domain:String) : Seq[Escalation] = {
    log.debug("Processing escalation list request")
    configStore.listEscalations(domain)
  }

  def listEscalationForPair(domain:String, pairKey: String): Seq[Escalation] = {
    val pair = configStore.getPair(domain, pairKey)
    configStore.listEscalationsForPair(domain, pair)
  }
}
