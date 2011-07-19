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
//import net.lshift.diffa.kernel.config.{Pair => Pair}

class HibernateConfigStore(val sessionFactory: SessionFactory)
    extends ConfigStore
    with HibernateQueryUtils {
  def createOrUpdateEndpoint(e: Endpoint): Unit = sessionFactory.withSession(s => s.saveOrUpdate(e))

  def deleteEndpoint(name: String): Unit = sessionFactory.withSession(s => {
    val endpoint = getEndpoint(s, name)

    // Delete children manually - Hibernate can't cascade on delete without a one-to-many relationship,
    // which would create an infinite loop in computing the hashCode of pairs
    s.createQuery("FROM Pair WHERE upstream = :endpoint OR downstream = :endpoint").
            setEntity("endpoint", endpoint).list.foreach(x => s.delete(x))

    s.delete(endpoint)
  })

  def listEndpoints: Seq[Endpoint] = sessionFactory.withSession(s => {
    listQuery[Endpoint](s, "allEndpoints", Map())
  })

  def createOrUpdateRepairAction(a: RepairAction) {
    sessionFactory.withSession(_.saveOrUpdate(a))
  }

  def deleteRepairAction(name: String, pairKey: String) {
    sessionFactory.withSession(s => {
      val action = getRepairAction(s, name, pairKey)
      s.delete(action)
    })
  }

  def createOrUpdatePair(p: PairDef): Unit = sessionFactory.withSession(s => {
    p.validate()

    val up = getEndpoint(s, p.upstreamName)
    val down = getEndpoint(s, p.downstreamName)
    val toUpdate = new Pair(p.pairKey, up, down, p.versionPolicyName, p.matchingTimeout, p.scanCronSpec)
    s.saveOrUpdate(toUpdate)
  })

  def deletePair(key: String): Unit = sessionFactory.withSession(s => {
    val pair = getPair(s, key)
    listRepairActionsForPair(pair).foreach(s.delete)
    s.delete(pair)
  })

  def listPairs = sessionFactory.withSession(s => listQuery[Pair](s, "allPairs", Map()))

  def listRepairActionsForPair(pair: Pair): Seq[RepairAction] =
    sessionFactory.withSession(s => getRepairActionsInPair(s, pair))

  def listEscalations = sessionFactory.withSession(s =>
    listQuery[Escalation](s, "allEscalations", Map()))

  def deleteEscalation(name: String, pairKey: String) = {
    sessionFactory.withSession(s => {
      val escalation = getEscalation(s, name, pairKey)
      s.delete(escalation)
    })
  }

  def createOrUpdateEscalation(e: Escalation) = sessionFactory.withSession( s => s.saveOrUpdate(e) )

  def listEscalationsForPair(pair: Pair) =
    sessionFactory.withSession(s => getEscalationsForPair(s, pair))

  private def getRepairActionsInPair(s: Session, pair: Pair): Seq[RepairAction] =
    listQuery[RepairAction](s, "repairActionsByPair", Map("pairKey" -> pair.key))

  private def getEscalationsForPair(s: Session, pair: Pair): Seq[Escalation] =
    listQuery[Escalation](s, "escalationsByPairKey", Map("pairKey" -> pair.key))

  def listRepairActions: Seq[RepairAction] = sessionFactory.withSession(s =>
    listQuery[RepairAction](s, "allRepairActions", Map()))

  def createOrUpdateUser(u: User) = sessionFactory.withSession(s => s.saveOrUpdate(u))

  def deleteUser(name: String) = sessionFactory.withSession(s => {
    val user = getUser(s, name)
    s.delete(user)
  })
  
  def listUsers: Seq[User] = sessionFactory.withSession(s => {
    listQuery[User](s, "allUsers", Map())
  })

  def getPairsForEndpoint(epName:String):Seq[Pair] = sessionFactory.withSession(s => {
    val q = s.createQuery("SELECT p FROM Pair p WHERE p.upstream.name = :epName OR p.downstream.name = :epName")
    q.setParameter("epName", epName)

    q.list.map(_.asInstanceOf[Pair]).toSeq
  })

  def getEndpoint(name: String) = sessionFactory.withSession(s => getEndpoint(s, name))
  def getPair(key: String) = sessionFactory.withSession(s => getPair(s, key))
  def getUser(name: String) : User = sessionFactory.withSession(s => getUser(s, name))
  def getRepairAction(name: String, pairKey: String) = sessionFactory.withSession(s => getRepairAction(s, name, pairKey))

  def allConfigOptions = {
    sessionFactory.withSession(s => {
      listQuery[ConfigOption](s, "allNonInternalConfigOptions", Map()).map(opt => opt.key -> opt.value).toMap
    })
  }

  def maybeConfigOption(key:String) =
    sessionFactory.withSession(s => singleQueryOpt[String](s, "configOptionByKey", Map("key" -> key)))
  def configOptionOrDefault(key: String, defaultVal: String) =
    maybeConfigOption(key) match {
      case Some(str) => str
      case None      => defaultVal
    }
  def setConfigOption(key:String, value:String, isInternal:Boolean = false) = sessionFactory.withSession(s => {
    val co = s.get(classOf[ConfigOption], key) match {
      case null =>
        new ConfigOption(key = key, value = value, isInternal = isInternal)
      case current:ConfigOption =>  {
        current.value = value
        current.isInternal = isInternal
        current
      }
    }
    s.saveOrUpdate(co)
  })
  def clearConfigOption(key:String) = sessionFactory.withSession(s => {
    val co = s.get(classOf[ConfigOption], key) match {
      case null =>
      case current:ConfigOption =>  s.delete(current)

    }
  })

  private def getEndpoint(s: Session, name: String) = singleQuery[Endpoint](s, "endpointByName", Map("name" -> name), "endpoint %s".format(name))
  private def getUser(s: Session, name: String) = singleQuery[User](s, "userByName", Map("name" -> name), "user %s".format(name))
  private def getEndpointOpt(s: Session, name: String) = singleQueryOpt[Endpoint](s, "endpointByName", Map("name" -> name))
  private def getPair(s: Session, key: String) = singleQuery[Pair](s, "pairByKey", Map("key" -> key), "pair %s".format(key))
  private def getPairOpt(s: Session, key: String) = singleQueryOpt[Pair](s, "pairByKey", Map("key" -> key))
  private def getRepairAction(s: Session, name: String, pairKey: String) =
    singleQuery[RepairAction](s, "repairActionByNameAndPairKey", Map("name" -> name, "pairKey" -> pairKey), "repair action %s for pair %s".format(name,pairKey))
  private def getEscalation(s: Session, name: String, pairKey: String) =
    singleQuery[Escalation](s, "escalationByNameAndPairKey", Map("name" -> name, "pairKey" -> pairKey), "esclation %s for pair %s".format(name,pairKey))

}
