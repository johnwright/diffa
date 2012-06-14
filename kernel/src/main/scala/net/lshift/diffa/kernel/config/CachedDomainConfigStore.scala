/**
 * Copyright (C) 2010-2012 LShift Ltd.
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
package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.frontend._
import net.lshift.diffa.kernel.util.cache.CacheProvider


class CachedDomainConfigStore(underlying:DomainConfigStore,
                              cacheProvider:CacheProvider) extends DomainConfigStore {

  val pairsByEndpoint = cacheProvider.getCachedMap[String,Seq[DiffaPair]]("domain.pairs.by.endpoint")

  def reset {
    pairsByEndpoint.evictAll()
  }

  def listPairsForEndpoint(domain: String, endpoint: String): Seq[DiffaPair] = {
    pairsByEndpoint.readThrough(
      formatKey(domain, endpoint),
      () => underlying.listPairsForEndpoint(domain, endpoint)
    )
  }




  def listRepairActionsForPair(domain: String, key: String) = underlying.listRepairActionsForPair(domain, key)
  def createOrUpdateEndpoint(domain: String, endpoint: EndpointDef) = underlying.createOrUpdateEndpoint(domain, endpoint)
  def deleteEndpoint(domain: String, name: String) = underlying.deleteEndpoint(domain, name)
  def listEndpoints(domain: String): Seq[EndpointDef] = underlying.listEndpoints(domain)
  def createOrUpdatePair(domain: String, pairDef: PairDef) = underlying.createOrUpdatePair(domain, pairDef)
  def deletePair(domain: String, key: String) = underlying.deletePair(domain, key)
  def listPairs(domain: String) = underlying.listPairs(domain)
  def createOrUpdateRepairAction(domain: String, action: RepairActionDef) = underlying.createOrUpdateRepairAction(domain, action)
  def deleteRepairAction(domain: String, name: String, pairKey: String) = underlying.deleteRepairAction(domain, name, pairKey)
  def listRepairActions(domain: String) = underlying.listRepairActions(domain)
  def listEscalations(domain: String) = underlying.listEscalations(domain)
  def deleteEscalation(domain: String, s: String, s1: String) = underlying.deleteEscalation(domain, s, s1)
  def createOrUpdateEscalation(domain: String, escalation: EscalationDef) = underlying.createOrUpdateEscalation(domain, escalation)
  def listEscalationsForPair(domain: String, key: String) = underlying.listEscalationsForPair(domain, key)
  def listReports(domain: String) = underlying.listReports(domain)
  def deleteReport(domain: String, name: String, pairKey: String) = underlying.deleteReport(domain, name, pairKey)
  def createOrUpdateReport(domain: String, report: PairReportDef) = underlying.createOrUpdateReport(domain,report)
  def listReportsForPair(domain: String, key: String) = underlying.listReportsForPair(domain, key)
  def getEndpointDef(domain: String, name: String): EndpointDef = underlying.getEndpointDef(domain, name)
  def getEndpoint(domain: String, name: String): Endpoint = underlying.getEndpoint(domain, name)
  def getPairDef(domain: String, key: String): PairDef = underlying.getPairDef(domain, key)
  def getRepairActionDef(domain: String, name: String, pairKey: String) = underlying.getRepairActionDef(domain, name, pairKey)
  def getPairReportDef(domain: String, name: String, pairKey: String) = underlying.getPairReportDef(domain, name, pairKey)
  def getConfigVersion(domain: String) = underlying.getConfigVersion(domain)
  def allConfigOptions(domain: String) = underlying.allConfigOptions(domain)
  def maybeConfigOption(domain: String, key: String) = underlying.maybeConfigOption(domain, key)
  def configOptionOrDefault(domain: String, key: String, defaultVal: String) = underlying.configOptionOrDefault(domain, key, defaultVal)
  def setConfigOption(domain: String, key: String, value: String) = underlying.setConfigOption(domain, key, value)
  def clearConfigOption(domain: String, key: String) = underlying.clearConfigOption(domain, key)
  def makeDomainMember(domain: String, userName: String): Member = underlying.makeDomainMember(domain, userName)
  def removeDomainMembership(domain: String, userName: String) = underlying.removeDomainMembership(domain, userName)
  def listDomainMembers(domain: String): Seq[Member] = underlying.listDomainMembers(domain)

  private def formatKey(domain: String, objectName: String) = "%s/%s".format(domain, objectName)
}
