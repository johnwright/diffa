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

import org.junit.Assert._
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.config.User
import net.lshift.diffa.kernel.util.cache.HazelcastCacheProvider
import org.junit.{Before, Test}
import net.lshift.diffa.kernel.util.MissingObjectException
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoint, Theories, Theory}

@RunWith(classOf[Theories])
class CachedSystemConfigStoreTest {

  import CachedSystemConfigStoreTest._

  val underlying = createStrictMock(classOf[SystemConfigStore])
  val cacheProvider = new HazelcastCacheProvider()

  val cachedSystemConfigStore = new CachedSystemConfigStore(underlying, cacheProvider)

  @Before
  def resetCache {
    cachedSystemConfigStore.reset
  }

  @Theory
  def shouldCacheLookupByToken(scenario:SimpleCacheScenario) {

    expect(scenario.cachingOperation(underlying)).andReturn(user).once()
    replay(underlying)

    val retrieved1 = scenario.cachingOperation(cachedSystemConfigStore)
    assertEquals(user, retrieved1)

    val retrieved2 = scenario.cachingOperation(cachedSystemConfigStore)
    assertEquals(user, retrieved2)

    verify(underlying)
  }

  @Theory
  def clearingOperationShouldClearTokenCache(scenario:CacheScenarioWithRemoval) {

    expect(scenario.cachingOperation(underlying)).andReturn(scenario.user).once()
    expect(scenario.clearingOperation(underlying)).once()
    expect(scenario.cachingOperation(underlying)).andThrow(new MissingObjectException("user")).once()
    replay(underlying)

    val retrieved = scenario.cachingOperation(cachedSystemConfigStore)
    assertEquals(scenario.user, retrieved)

    scenario.clearingOperation(cachedSystemConfigStore)

    try {
      scenario.cachingOperation(cachedSystemConfigStore)
      fail("Lookup for user (%s) should throw exception".format(scenario.user))
    }
    catch {
      case x:MissingObjectException => // This is expected
    }

    verify(underlying)
  }

}

case class SimpleCacheScenario(
  user:User,
  cachingOperation:SystemConfigStore => User
)

case class CacheScenarioWithRemoval(
  user:User,
  clearingOperation:SystemConfigStore => Unit,
  cachingOperation:SystemConfigStore => User
)

object CachedSystemConfigStoreTest {

  val user = new User(
    name = "username",
    email = "dev_null@acme.com",
    superuser = false,
    passwordEnc = "2309jfsd",
    token = "6f4g4b3c"
  )

  @DataPoint def shouldCacheUserToken = SimpleCacheScenario(
    user,
    (c:SystemConfigStore) => c.getUserByToken(user.token)
  )

  @DataPoint def shouldCacheFullUser = SimpleCacheScenario(
    user,
    (c:SystemConfigStore) => c.getUser(user.name)
  )

  @DataPoint def clearUserTokenShouldClearTokenCache = CacheScenarioWithRemoval(
    user,
    (c:SystemConfigStore) => c.clearUserToken(user.name),
    (c:SystemConfigStore) => c.getUserByToken(user.token)
  )

  @DataPoint def deleteUserShouldClearTokenCache = CacheScenarioWithRemoval(
    user,
    (c:SystemConfigStore) => c.deleteUser(user.name),
    (c:SystemConfigStore) => c.getUserByToken(user.token)
  )

  @DataPoint def deleteUserTokenShouldClearUserCache = CacheScenarioWithRemoval(
    user,
    (c:SystemConfigStore) => c.deleteUser(user.name),
    (c:SystemConfigStore) => c.getUser(user.name)
  )
}
