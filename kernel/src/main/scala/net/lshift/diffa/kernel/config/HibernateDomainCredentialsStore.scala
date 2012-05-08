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

import net.lshift.diffa.kernel.frontend.FrontendConversions._
import net.lshift.diffa.kernel.frontend.{OutboundExternalHttpCredentialsDef, InboundExternalHttpCredentialsDef}
import net.lshift.diffa.kernel.util.HibernateQueryUtils
import net.lshift.diffa.kernel.util.SessionHelper._
import scala.collection.JavaConversions._
import org.hibernate.SessionFactory

class HibernateDomainCredentialsStore(val sessionFactory: SessionFactory) extends DomainCredentialsStore with HibernateQueryUtils {

  def addExternalHttpCredentials(domain:String, creds:InboundExternalHttpCredentialsDef) = sessionFactory.withSession( s => {
    s.saveOrUpdate(fromInboundExternalHttpCredentialsDef(domain, creds))
  })

  def deleteExternalHttpCredentials(domain:String, url:String, credentialType:String) = sessionFactory.withSession( s => {
    s.getNamedQuery("deleteExternalHttpCredentials").
      setString("domain", domain).
      setString("url", url).
      setString("type", credentialType).
      executeUpdate()
  })

  def listCredentials(domain:String) : Seq[OutboundExternalHttpCredentialsDef] = sessionFactory.withSession( s => {
    val resultSet = s.getNamedQuery("externalHttpCredentialsByDomain").setString("domain", domain).list()

    // Result set ordering: select url, key, credential_type from ....

    resultSet.map(result => {
      val row = result.asInstanceOf[Array[_]].map(e => e.asInstanceOf[String])
      OutboundExternalHttpCredentialsDef(url = row(0), key = row(1), `type` = row(2))
    })
  })
}
