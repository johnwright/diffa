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


class CachedSystemConfigStoreTest {

  val underlying = createStrictMock(classOf[SystemConfigStore])
  val cacheProvider = new HazelcastCacheProvider()

  val cachedSystemConfigStore = new CachedSystemConfigStore(underlying, cacheProvider)

  @Before
  def resetCache {
    cachedSystemConfigStore.reset
  }

  @Test
  def shouldCacheLookupByToken {
    val user = new User(name = "username", email = "", superuser = false, passwordEnc = "")

    expect(underlying.getUserByToken("6f4g4b3c")).andReturn(user).once()
    replay(underlying)

    val retrieved1 = cachedSystemConfigStore.getUserByToken("6f4g4b3c")
    assertEquals(user, retrieved1)

    val retrieved2 = cachedSystemConfigStore.getUserByToken("6f4g4b3c")
    assertEquals(user, retrieved2)

    verify(underlying)
  }

  @Test
  def shouldCacheLookupByName {
    val user = new User(name = "username", email = "", superuser = false, passwordEnc = "")

    expect(underlying.getUser("username")).andReturn(user).once()
    replay(underlying)

    val retrieved1 = cachedSystemConfigStore.getUser("username")
    assertEquals(user, retrieved1)

    val retrieved2 = cachedSystemConfigStore.getUser("username")
    assertEquals(user, retrieved2)

    verify(underlying)
  }
}
