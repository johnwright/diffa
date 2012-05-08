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

import net.lshift.diffa.kernel.frontend.{OutboundExternalHttpCredentialsDef, InboundExternalHttpCredentialsDef}

/**
 * Interface the administration and retrieval of external credentials.
 * This interface is split into a basic interface that will not
 * return any secrets from the database and an interface that only internal components can use.
 */
trait LimitedDomainCredentialsStore {

  /**
   * Adds a new set of external HTTP credentials for the given domain.
   */
  def addExternalHttpCredentials(domain:String, creds:InboundExternalHttpCredentialsDef)

  /**
   * Removes the external HTTP credentials for the given domain, url and credential type.
   */
  def deleteExternalHttpCredentials(domain:String, url:String, credentialType:String)

  /**
   * Lists all credentials in the current domain.
   */
  def listCredentials(domain:String) : Seq[OutboundExternalHttpCredentialsDef]
}

trait DomainCredentialsStore extends LimitedDomainCredentialsStore {
  /**
   * Returns the most specific credentials that matches the given URL
   */
  def credentialsForUrl(domain:String, url:String) : Option[HttpCredentials]
}

trait HttpCredentials
case class BasicAuthCredentials(username:String, password:String) extends HttpCredentials
case class QueryParameterCredentials(name:String, value:String) extends HttpCredentials

