/**
 * Copyright (C) 2010 LShift Ltd.
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

package net.lshift.diffa.kernel.differencing

import org.junit.Test
import org.junit.Assert._
import org.hamcrest.core.Is._
import org.hamcrest.core.IsInstanceOf._
import net.lshift.diffa.kernel.events.VersionID

/**
 * Test cases for the LocalSessionCacheProvider.
 */
class LocalSessionCacheProviderTest {
  val provider = new LocalSessionCacheProvider

  @Test
  def shouldReturnNoneForInvalidSessionID {
    assertEquals(None, provider.retrieveCache("asdasdasd"))
  }

  @Test
  def shouldReturnACacheForNewSessionIDWhenRetrieveOrCreating {
    assertThat(provider.retrieveOrAllocateCache("session1", SessionScope.all), is(instanceOf(classOf[LocalSessionCache])))
  }

  @Test
  def shouldReturnACacheWithTheProvidedScopeWhenCreating {
    val scope = SessionScope.forPairs("ab", "bc")

    val cache = provider.retrieveOrAllocateCache("session1", scope)
    assertEquals(true, cache.isInScope(VersionID("ab", "aaa")))
    assertEquals(true, cache.isInScope(VersionID("bc", "aaa")))
    assertEquals(false, cache.isInScope(VersionID("de", "aaa")))
  }

  @Test
  def shouldReturnSameCacheForSameSessionIDWhenRetrieveOrCreating {
    val cache = provider.retrieveOrAllocateCache("session2", SessionScope.all)
    assertSame(cache, provider.retrieveOrAllocateCache("session2", SessionScope.all))
  }

  @Test
  def shouldReturnSameCacheForSameSessionIDWhenJustRetrieving {
    val cache = provider.retrieveOrAllocateCache("session3", SessionScope.all)
    assertEquals(Some(cache), provider.retrieveCache("session3"))
  }

  @Test
  def shouldNotChangeTheSessionScopeWhenRetrievingAnExistingCache {
    val scope = SessionScope.forPairs("ab", "bc")

    // Allocate, then retrieve afresh
    provider.retrieveOrAllocateCache("session3", scope)
    val cache = provider.retrieveOrAllocateCache("session3", scope)
    assertEquals(true, cache.isInScope(VersionID("ab", "aaa")))
    assertEquals(true, cache.isInScope(VersionID("bc", "aaa")))
    assertEquals(false, cache.isInScope(VersionID("de", "aaa")))
  }
}