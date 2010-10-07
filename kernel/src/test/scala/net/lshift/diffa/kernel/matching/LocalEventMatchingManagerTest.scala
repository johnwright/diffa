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

package net.lshift.diffa.kernel.matching

import org.junit.Test
import org.junit.Assert._
import org.easymock.EasyMock.{createStrictMock, expect, replay, reset}
import net.lshift.diffa.kernel.config.{GroupContainer, ConfigStore, Pair}

/**
 * Test cases for the LocalEventMatchingManager.
 */
class LocalEventMatchingManagerTest {
  val pair1 = new Pair("pair1", null, null, null, null, 10)
  val pair2 = new Pair("pair2", null, null, null, null, 5)

  val configStore = createStrictMock(classOf[ConfigStore])
  expect(configStore.getPair("pair1")).andStubReturn(pair1)
  expect(configStore.getPair("pair2")).andStubReturn(pair2)
  expect(configStore.listGroups).andStubReturn(Seq(new GroupContainer(null, Array(pair1, pair2))))
  replay(configStore)

  val matchingManager = new LocalEventMatchingManager(configStore)

  @Test
  def shouldNotReturnAMatcherForAnInvalidKey {
    assertEquals(None, matchingManager.getMatcher("invalid"))
  }

  @Test
  def shouldReturnAMatcherForAPairKeyThatWasKnownAtStartup {
    assertFalse(None.equals(matchingManager.getMatcher("pair1")))
  }

  @Test
  def shouldReturnAMatcherForAPairKeyLaterIntroduced {
    reset(configStore)
    expect(configStore.getPair("pair3")).andStubReturn(new Pair("pair3", null, null, null, null, 2))
    replay(configStore)

    matchingManager.onUpdatePair("pair3")
    assertFalse(None.equals(matchingManager.getMatcher("pair3")))
  }

  @Test
  def shouldNotReturnAMatcherForARemovedPair {
    reset(configStore)
    expect(configStore.getPair("pair3")).andStubReturn(new Pair("pair3", null, null, null, null, 2))
    replay(configStore)

    matchingManager.onUpdatePair("pair3")
    matchingManager.onDeletePair("pair3")

    assertEquals(None, matchingManager.getMatcher("pair3"))
  }

  @Test
  def shouldApplyListenersToExistingMatchers {
    val l1 = createStrictMock(classOf[MatchingStatusListener])
    matchingManager.addListener(l1)

    assertTrue(matchingManager.getMatcher("pair1").get.listeners.contains(l1))
    assertTrue(matchingManager.getMatcher("pair2").get.listeners.contains(l1))
  }

  @Test
  def shouldApplyListenersToNewMatchers {
    reset(configStore)
    expect(configStore.getPair("pair3")).andStubReturn(new Pair("pair3", null, null, null, null, 2))
    replay(configStore)

    val l1 = createStrictMock(classOf[MatchingStatusListener])
    matchingManager.addListener(l1)

    matchingManager.onUpdatePair("pair3")
    assertTrue(matchingManager.getMatcher("pair3").get.listeners.contains(l1))
  }
}