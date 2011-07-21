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

import net.lshift.diffa.kernel.config.{Endpoint, Domain, ConfigStore, Pair => DiffaPair}

/**
 * This trait is only accessible from internally trusted components
 */
trait InternalConfigStore extends ConfigStore {

  def createOrUpdateDomain(domain: Domain) : Unit
  def deleteDomain(name: String): Unit
  def getDomain(name: String): Domain
  def listDomains : Seq[Domain]
  def getPairsForEndpoint(epName:String) : Seq[DiffaPair]

  /**
   * Sets the given configuration option to the given value.
   * This option is marked as internal will not be returned by the allConfigOptions method. This allows
   * properties to be prevented from being shown in the user-visible system configuration views.
   */
  def setInternalConfigOption(key:String, value:String)
  def maybeConfigOption(key:String) : Option[String]

  /**
   * Enumerate all pairs of all domains
   */
  def listPairs : Seq[DiffaPair] = listDomains.flatMap(d => listPairs(d.name))

  /**
   * Enumerate all pairs of all domains
   */
  def listEndpoints : Seq[Endpoint] = listDomains.flatMap(d => listEndpoints(d.name))

}