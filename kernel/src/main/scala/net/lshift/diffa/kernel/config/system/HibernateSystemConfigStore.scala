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

package net.lshift.diffa.kernel.config.system

import net.lshift.diffa.kernel.util.SessionHelper._
import net.lshift.diffa.kernel.config.{User, ConfigOption, RepairAction, Escalation, Endpoint, DomainConfigStore, Domain, Pair => DiffaPair}
import net.lshift.diffa.kernel.util.HibernateQueryUtils
import org.hibernate.{Session, SessionFactory}
import scala.collection.JavaConversions._

class HibernateSystemConfigStore(domainConfigStore:DomainConfigStore,
                                 val sessionFactory:SessionFactory)
    extends SystemConfigStore with HibernateQueryUtils {

  def createOrUpdateDomain(d: Domain) = sessionFactory.withSession( s => s.saveOrUpdate(d) )

  def deleteDomain(domain:String) = sessionFactory.withSession( s => {
    deleteByDomain[Escalation](s, domain, "escalationsByDomain")
    deleteByDomain[RepairAction](s, domain, "repairActionsByDomain")
    deleteByDomain[DiffaPair](s, domain, "pairsByDomain")
    deleteByDomain[Endpoint](s, domain, "endpointsByDomain")
    deleteByDomain[ConfigOption](s, domain, "configOptionsByDomain")
    s.flush()
    // TODO find out how to express this in HQL
    s.createSQLQuery("delete from domain_users where domain_name = '%s'".format(domain)).executeUpdate()
    s.createSQLQuery("delete from domains where name = '%s'".format(domain)).executeUpdate()
  })

  def listDomains = sessionFactory.withSession(s => listQuery[Domain](s, "allDomains", Map()))

  def getPairsForInboundEndpointURL(url: String) = {
    sessionFactory.withSession(s => listQuery[DiffaPair](s, "pairsByInboundEndpointUrl", Map("url" -> url)))
  }

  def getPair(domain:String, key: String) = sessionFactory.withSession(s => getPair(s, domain, key))

  def listPairs = sessionFactory.withSession(s => listQuery[DiffaPair](s, "allPairs", Map()))
  def listEndpoints = sessionFactory.withSession(s => listQuery[Endpoint](s, "allEndpoints", Map()))

  def maybeSystemConfigOption(key: String) = {
    sessionFactory.withSession(s => singleQueryOpt[String](s, "rootConfigOptionByKey", Map("key" -> key)))
  }
  def setSystemConfigOption(key:String, value:String) = writeConfigOption(Domain.DEFAULT_DOMAIN.name, key, value)
  def clearSystemConfigOption(key:String) = deleteConfigOption(Domain.DEFAULT_DOMAIN.name, key)

  def systemConfigOptionOrDefault(key:String, defaultVal:String) = {
    maybeSystemConfigOption(key) match {
      case Some(s)   => s
      case None      => defaultVal
    }
  }

  private def deleteByDomain[T](s:Session, domain:String, queryName:String)
    = listQuery[T](s, queryName, Map("domain_name" -> domain)).foreach(s.delete(_))
}