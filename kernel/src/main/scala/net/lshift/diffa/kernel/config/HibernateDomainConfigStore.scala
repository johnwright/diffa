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

package net.lshift.diffa.kernel.config

import net.lshift.diffa.kernel.util.SessionHelper._ // for 'SessionFactory.withSession'
import net.lshift.diffa.kernel.util.HibernateQueryUtils
import org.hibernate.{Session, SessionFactory}
import scala.collection.JavaConversions._

class HibernateDomainConfigStore(val sessionFactory: SessionFactory)
    extends DomainConfigStore
    with HibernateQueryUtils {
  def createOrUpdateEndpoint(domain:String, e: Endpoint): Unit = sessionFactory.withSession(s => s.saveOrUpdate(e))

  def deleteEndpoint(domain:String, name: String): Unit = sessionFactory.withSession(s => {
    val endpoint = getEndpoint(s, domain, name)

    // Delete children manually - Hibernate can't cascade on delete without a one-to-many relationship,
    // which would create an infinite loop in computing the hashCode of pairs
    s.createQuery("FROM Pair WHERE upstream = :endpoint OR downstream = :endpoint").
            setEntity("endpoint", endpoint).list.foreach(x => s.delete(x))

    s.delete(endpoint)
  })

  def listEndpoints(domain:String): Seq[Endpoint] = sessionFactory.withSession(s => {
    listQuery[Endpoint](s, "allEndpoints", Map())
  })

  def createOrUpdateRepairAction(domain:String, a: RepairAction) {
    sessionFactory.withSession(_.saveOrUpdate(a))
  }

  def deleteRepairAction(domain:String, name: String, pairKey: String) {
    sessionFactory.withSession(s => {
      val action = getRepairAction(s, domain, name, pairKey)
      s.delete(action)
    })
  }

  def createOrUpdatePair(domain:String, p: PairDef): Unit = sessionFactory.withSession(s => {
    p.validate()

    val up = getEndpoint(s, domain, p.upstreamName)
    val down = getEndpoint(s, domain, p.downstreamName)
    val toUpdate = new Pair(p.pairKey, p.domain, up, down, p.versionPolicyName, p.matchingTimeout, p.scanCronSpec)
    s.saveOrUpdate(toUpdate)
  })

  def deletePair(domain:String, key: String): Unit = sessionFactory.withSession(s => {
    val pair = getPair(s, domain, key)
    listRepairActionsForPair(domain, pair).foreach(s.delete)
    s.delete(pair)
  })

  def listPairs(domain:String) = sessionFactory.withSession(s => listQuery[Pair](s, "allPairs", Map()))

  def listRepairActionsForPair(domain:String, pair: Pair): Seq[RepairAction] =
    sessionFactory.withSession(s => getRepairActionsInPair(s, domain, pair))

  def listEscalations(domain:String) = sessionFactory.withSession(s =>
    listQuery[Escalation](s, "allEscalations", Map()))

  def deleteEscalation(domain:String, name: String, pairKey: String) = {
    sessionFactory.withSession(s => {
      val escalation = getEscalation(s, domain, name, pairKey)
      s.delete(escalation)
    })
  }

  def createOrUpdateEscalation(domain:String, e: Escalation) = sessionFactory.withSession( s => s.saveOrUpdate(e) )

  def listEscalationsForPair(domain:String, pair: Pair) =
    sessionFactory.withSession(s => getEscalationsForPair(s, domain, pair))

  private def getRepairActionsInPair(s: Session, domain:String, pair: Pair): Seq[RepairAction] =
    listQuery[RepairAction](s, "repairActionsByPair", Map("pairKey" -> pair.key))

  private def getEscalationsForPair(s: Session, domain:String, pair: Pair): Seq[Escalation] =
    listQuery[Escalation](s, "escalationsByPairKey", Map("pairKey" -> pair.key))

  def listRepairActions(domain:String) : Seq[RepairAction] = sessionFactory.withSession(s =>
    listQuery[RepairAction](s, "allRepairActions", Map()))

  def createOrUpdateUser(domain:String, u: User) = sessionFactory.withSession(s => s.saveOrUpdate(u))

  def deleteUser(domain:String, name: String) = sessionFactory.withSession(s => {
    val user = getUser(s, domain, name)
    s.delete(user)
  })
  
  def listUsers(domain:String) : Seq[User] = sessionFactory.withSession(s => {
    listQuery[User](s, "allUsers", Map())
  })

  def getPairsForEndpoint(domain:String, epName:String):Seq[Pair] = sessionFactory.withSession(s => {
    val q = s.createQuery("SELECT p FROM Pair p WHERE p.upstream.name = :epName OR p.downstream.name = :epName")
    q.setParameter("epName", epName)

    q.list.map(_.asInstanceOf[Pair]).toSeq
  })

  def getEndpoint(domain:String, name: String) = sessionFactory.withSession(s => getEndpoint(s, domain, name))
  def getPair(domain:String, key: String) = sessionFactory.withSession(s => getPair(s, domain, key))
  def getUser(domain:String, name: String) : User = sessionFactory.withSession(s => getUser(s, domain, name))
  def getRepairAction(domain:String, name: String, pairKey: String) = sessionFactory.withSession(s => getRepairAction(s, domain, name, pairKey))

  def allConfigOptions(domain:String) = {
    sessionFactory.withSession(s => {
      listQuery[ConfigOption](s, "allNonInternalConfigOptions", Map()).map(opt => opt.key -> opt.value).toMap
    })
  }

  def maybeConfigOption(domain:String, key:String) =
    sessionFactory.withSession(s => singleQueryOpt[String](s, "configOptionByKey", Map("key" -> key)))
  def configOptionOrDefault(domain:String, key: String, defaultVal: String) =
    maybeConfigOption(domain, key) match {
      case Some(str) => str
      case None      => defaultVal
    }

  def setConfigOption(domain:String, key:String, value:String) = writeConfigOption(domain, key, value)
  def clearConfigOption(domain:String, key:String) = deleteConfigOption(domain, key)

  private def getEndpoint(s: Session, domain:String, name: String) = singleQuery[Endpoint](s, "endpointByName", Map("name" -> name), "endpoint %s".format(name))
  private def getUser(s: Session, domain:String, name: String) = singleQuery[User](s, "userByName", Map("name" -> name), "user %s".format(name))
  //private def getEndpointOpt(s: Session, domain:String, name: String) = singleQueryOpt[Endpoint](s, "endpointByName", Map("name" -> name))
  private def getPair(s: Session, domain:String, key: String) = singleQuery[Pair](s, "pairByKey", Map("key" -> key), "pair %s".format(key))
  //private def getPairOpt(s: Session, domain:String, key: String) = singleQueryOpt[Pair](s, "pairByKey", Map("key" -> key))
  private def getRepairAction(s: Session, domain:String, name: String, pairKey: String) =
    singleQuery[RepairAction](s, "repairActionByNameAndPairKey", Map("name" -> name, "pairKey" -> pairKey), "repair action %s for pair %s".format(name,pairKey))
  private def getEscalation(s: Session, domain:String, name: String, pairKey: String) =
    singleQuery[Escalation](s, "escalationByNameAndPairKey", Map("name" -> name, "pairKey" -> pairKey), "esclation %s for pair %s".format(name,pairKey))

}
