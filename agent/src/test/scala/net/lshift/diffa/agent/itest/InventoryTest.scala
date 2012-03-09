/**
 * Copyright (C) 2012 LShift Ltd.
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
package net.lshift.diffa.agent.itest

import net.lshift.diffa.agent.itest.support.TestEnvironments
import net.lshift.diffa.agent.itest.support.TestConstants._
import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.differencing.{MatchState, DifferenceEvent}

class InventoryTest extends AbstractEnvironmentTest {
  val envFactory = TestEnvironments.same _

  @Test
  def shouldGenerateDifferencesBasedUponAnInventoryBeingUploaded() {
    env.inventoryClient.uploadInventory(env.upstreamEpName, csv(
      "id,vsn,someString,someDate",
      "id1,v1,ss,2012-03-09T09:04:00Z",
      "id2,v2,tt,2012-03-10T10:05:12Z"
    ))

    val diffs = env.differencesHelper.waitForDiffCount(yesterday, tomorrow, 2).sortBy(e => e.objId.id)
    
    assertDiffEquals(
      DifferenceEvent(objId = VersionID(env.pairRef, "id1"), state = MatchState.UNMATCHED, upstreamVsn = "v1"),
      diffs(0))
    assertDiffEquals(
      DifferenceEvent(objId = VersionID(env.pairRef, "id2"), state = MatchState.UNMATCHED, upstreamVsn = "v2"),
      diffs(1))
  }

  @Test
  def shouldResolveDifferencesWhenMatchingInventoryIsUploadedForDownstream() {
    env.inventoryClient.uploadInventory(env.upstreamEpName, csv(
      "id,vsn,someString,someDate",
      "id1,v1,ss,2012-03-09T09:04:00Z",
      "id2,v2,tt,2012-03-10T10:05:12Z"
    ))
    env.differencesHelper.waitForDiffCount(yesterday, tomorrow, 2)

    env.inventoryClient.uploadInventory(env.downstreamEpName, csv(
      "id,vsn,someString,someDate",
      "id1,v1,ss,2012-03-09T09:04:00Z",
      "id2,v2,tt,2012-03-10T10:05:12Z"
    ))
    env.differencesHelper.waitForDiffCount(yesterday, tomorrow, 0)
  }

  @Test
  def shouldSeeTheDifferencesBetweenTwoInventories() {
    env.inventoryClient.uploadInventory(env.upstreamEpName, csv(
      "id,vsn,someString,someDate",
      "id1,v1,ss,2012-03-09T09:04:00Z",
      "id2,v2,tt,2012-03-10T10:05:12Z"
    ))
    env.inventoryClient.uploadInventory(env.downstreamEpName, csv(
      "id,vsn,someString,someDate",
      "id1,v1,ss,2012-03-09T09:04:00Z",
      "id2,v3,tt,2012-03-10T10:05:12Z",
      "id3,v3,tt,2012-03-10T10:05:12Z"
    ))
    val diffs = env.differencesHelper.waitForDiffCount(yesterday, tomorrow, 2).sortBy(e => e.objId.id)

    assertDiffEquals(
      DifferenceEvent(objId = VersionID(env.pairRef, "id2"), state = MatchState.UNMATCHED, upstreamVsn = "v2", downstreamVsn = "v3"),
      diffs(0))
    assertDiffEquals(
      DifferenceEvent(objId = VersionID(env.pairRef, "id3"), state = MatchState.UNMATCHED, downstreamVsn = "v3"),
      diffs(1))
  }

  private def csv(lines:String*) = lines.mkString("\n")
  private def assertDiffEquals(expected:DifferenceEvent, actual:DifferenceEvent) {
    assertEquals(expected.objId, actual.objId)
    assertEquals(expected.state, actual.state)
    assertEquals(expected.upstreamVsn, actual.upstreamVsn)
    assertEquals(expected.downstreamVsn, actual.downstreamVsn)
  }
}