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

package net.lshift.diffa.kernel.differencing

import org.junit.Test
import org.junit.Assert._
import org.hamcrest.core.Is._
import org.hamcrest.core.IsInstanceOf._

/**
 * Test cases for the LocalDomainCacheProvider.
 */
class LocalDomainCacheProviderTest {
  val provider = new LocalDomainCacheProvider

  @Test
  def shouldReturnNoneForInvalidDomain() {
    assertEquals(None, provider.retrieveCache("asdasdasd"))
  }

  @Test
  def shouldReturnACacheForNewDomainWhenRetrieveOrCreating() {
    assertThat(provider.retrieveOrAllocateCache("domain1"), is(instanceOf(classOf[LocalDomainCache])))
  }

  @Test
  def shouldReturnSameCacheForSameDomainWhenRetrieveOrCreating() {
    val cache = provider.retrieveOrAllocateCache("domain2")
    assertSame(cache, provider.retrieveOrAllocateCache("domain2"))
  }

  @Test
  def shouldReturnSameCacheForSameDomainWhenJustRetrieving() {
    val cache = provider.retrieveOrAllocateCache("domain3")
    assertEquals(Some(cache), provider.retrieveCache("domain3"))
  }
}