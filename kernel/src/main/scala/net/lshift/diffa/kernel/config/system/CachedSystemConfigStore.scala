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

package net.lshift.diffa.kernel.config.system

import net.lshift.diffa.kernel.config.{Domain, DiffaPairRef, User}
import net.lshift.diffa.kernel.util.cache.CacheProvider

class CachedSystemConfigStore(underlying:SystemConfigStore, cacheProvider:CacheProvider)
  extends SystemConfigStore {

  val userTokenCache = cacheProvider.getCachedMap[String, User]("user.tokens")
  val usersCache = cacheProvider.getCachedMap[String, User]("users")

  def reset {
    usersCache.evictAll()
    userTokenCache.evictAll()
  }

  def createOrUpdateDomain(domain:Domain) = underlying.createOrUpdateDomain(domain)
  def deleteDomain(domain:String) = underlying.deleteDomain(domain)
  def doesDomainExist(name: String) = underlying.doesDomainExist(name)
  def listDomains = underlying.listDomains
  def setSystemConfigOption(key: String, value: String) = underlying.setSystemConfigOption(key, value)
  def clearSystemConfigOption(key: String) = underlying.clearSystemConfigOption(key)
  def maybeSystemConfigOption(key: String) = underlying.maybeSystemConfigOption(key)
  def systemConfigOptionOrDefault(key: String, defaultVal: String) = underlying.systemConfigOptionOrDefault(key, defaultVal)
  def getPair(domain: String, pairKey: String) = underlying.getPair(domain, pairKey)
  def getPair(pair: DiffaPairRef) = underlying.getPair(pair)
  def listPairs = underlying.listPairs
  def listEndpoints = underlying.listEndpoints
  @Deprecated def createOrUpdateUser(user: User) = underlying.createOrUpdateUser(user)
  def createUser(user: User) = underlying.createUser(user)
  def updateUser(user: User) = underlying.updateUser(user)
  def getUserToken(username: String) = underlying.getUserToken(username)
  def clearUserToken(username: String) = underlying.clearUserToken(username)
  def deleteUser(name: String) = underlying.deleteUser(name)
  def listUsers = underlying.listUsers
  def listDomainMemberships(username: String) = underlying.listDomainMemberships(username)

  def getUser(username: String) = {
    usersCache.readThrough(username, () => underlying.getUser(username))
  }

  def getUserByToken(token: String) = {
    userTokenCache.readThrough(token, () => underlying.getUserByToken(token))
  }

  def containsRootUser(names: Seq[String]) = underlying.containsRootUser(names)
}
