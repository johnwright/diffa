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

package net.lshift.diffa.kernel.config.internal

import net.lshift.diffa.kernel.util.SessionHelper._  // for 'SessionFactory.withSession'
import org.hibernate.{Session, SessionFactory}
import net.lshift.diffa.kernel.config.{HibernateConfigStore, Domain}

class HibernateInternalConfigStore(sessionFactory: SessionFactory)
    extends HibernateConfigStore(sessionFactory) with InternalConfigStore {

  def createOrUpdateDomain(d: Domain) = sessionFactory.withSession( s => s.saveOrUpdate(d) )

  def deleteDomain(name:String) = sessionFactory.withSession( s => {
    val domain = getDomain(name)
    s.delete(domain)
  })

  def getDomain(name: String) = sessionFactory.withSession(s => getDomain(s, name))

  def listDomains  = sessionFactory.withSession(s => listQuery[Domain](s, "allDomains", Map()))

  private def getDomain(s: Session, name: String) = singleQuery[Domain](s, "domainByName", Map("name" -> name), "domain %s".format(name))

  def getPairsForEndpoint(epName: String) = null

  def setInternalConfigOption(key: String, value: String) = null

  def maybeConfigOption(key: String) = null
}