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

import org.junit.Assert._
import net.lshift.diffa.kernel.events.VersionID
import org.junit.{Ignore, Test}
import org.joda.time.{Interval, DateTime}

/**
 * Test cases for the local session cache.
 */
class LocalSessionCacheTest {
  val cache = new LocalSessionCache("sessionID1234", SessionScope.forPairs("pair1", "pair2"))

  @Test
  def shouldMakeSessionIDAvailable {
    assertEquals("sessionID1234", cache.sessionId)
  }

  @Test
  def shouldDeferToScopeForInclusionChoice {
    assertEquals(true, cache.isInScope(VersionID("pair1", "aaa")))
    assertEquals(true, cache.isInScope(VersionID("pair2", "aaa")))
    assertEquals(false, cache.isInScope(VersionID("pair3", "aaa")))
  }

  @Test
  def shouldNotPublishPendingUnmatchedEventInAllUnmatchedList {
    cache.addPendingUnmatchedEvent(VersionID("pair1", "id1"), new DateTime(), "uV", "dV")
    assertEquals(0, cache.retrieveAllUnmatchedEvents.length)
  }

  @Test
  def shouldPublishUpgradedUnmatchedEventInAllUnmatchedList {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID("pair1", "id1"), timestamp, "uV", "dV")
    cache.upgradePendingUnmatchedEvent(VersionID("pair1", "id1"))

    val unmatched = cache.retrieveAllUnmatchedEvents
    assertEquals(1, unmatched.length)
    assertEquals(VersionID("pair1", "id1"), unmatched.first.objId)
    assertEquals(timestamp, unmatched.first.detectedAt)
    assertEquals("uV", unmatched.first.upstreamVsn)
    assertEquals("dV", unmatched.first.downstreamVsn)
  }

  @Test
  def shouldIgnoreUpgradeRequestsForUnknownIDs {
    cache.upgradePendingUnmatchedEvent(VersionID("pair1", "id1"))
    assertEquals(0, cache.retrieveAllUnmatchedEvents.length)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenUpgradedAlready {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID("pair1", "id1"), timestamp, "uV", "dV")
    cache.upgradePendingUnmatchedEvent(VersionID("pair1", "id1"))
    cache.upgradePendingUnmatchedEvent(VersionID("pair1", "id1"))

    assertEquals(1, cache.retrieveAllUnmatchedEvents.length)
  }

  @Test
  def shouldPublishAnAddedReportableUnmatchedEvent {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID("pair2", "id2"), timestamp, "uV", "dV")

    val unmatched = cache.retrieveAllUnmatchedEvents
    assertEquals(1, unmatched.length)
    assertEquals(MatchState.UNMATCHED, unmatched.first.state)
    assertEquals(VersionID("pair2", "id2"), unmatched.first.objId)
    assertEquals(timestamp, unmatched.first.detectedAt)
    assertEquals("uV", unmatched.first.upstreamVsn)
    assertEquals("dV", unmatched.first.downstreamVsn)
  }

  @Test
  def shouldReportUnmatchedEventWithinInterval = {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(size - frontFence - rearFence, unmatched.length)
  }

  def addUnmatchedEvents(start:DateTime, size:Int, frontFence:Int, rearFence:Int) : Interval = {
    for (i <- 1 to size) {
      val timestamp = start.plusMinutes(i)
      cache.addReportableUnmatchedEvent(VersionID("pair2", "id" + i), timestamp, "uV", "dV")
    }
    new Interval(start.plusMinutes(frontFence), start.plusMinutes(size - rearFence))
  }

  @Test
  def shouldPageReportableUnmatchedEvent = {
    val start = new DateTime(1982, 5, 5, 14, 15, 19, 0)
    val size = 100
    var frontFence = 20
    var rearFence = 50

    // Set a bound so that 30 events fall into the window
    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatched = cache.retrieveAllUnmatchedEvents
    assertEquals(size, unmatched.length)

    // Requesting 19 elements with an offset of 10 from 30 elements should yield elements 10 through to 28
    val containedPage = cache.retrievePagedEvents(interval, 10, 19)
    assertEquals(19, containedPage.length)

    // Requesting 19 elements with an offset of 20 from 30 elements should yield elements 20 through to 29
    val splitPage = cache.retrievePagedEvents(interval, 20, 19)
    assertEquals(10, splitPage.length)

  }

  @Test
  def shouldAddMatchedEventThatOverridesUnmatchedEventWhenAskingForSequenceUpdate {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID("pair2", "id2"), timestamp, "uuV", "ddV")

    val unmatched = cache.retrieveAllUnmatchedEvents
    val lastSeq = unmatched.last.seqId

    cache.addMatchedEvent(VersionID("pair2", "id2"), "uuV")
    val updates = cache.retrieveEventsSince(lastSeq)

    assertEquals(1, updates.length)
    assertEquals(MatchState.MATCHED, updates.first.state)
    // We don't know deterministically when the updated timestamp will be because this
    // is timestamped on the fly from within the implementation of the cache
    // but we do want to assert that it is not before the reporting timestamp
    assertFalse(timestamp.isAfter(updates.first.detectedAt))
    assertEquals(VersionID("pair2", "id2"), updates.first.objId)
  }

  @Test
  def shouldRemoveUnmatchedEventFromAllUnmatchedWhenAMatchHasBeenAdded {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID("pair2", "id2"), timestamp, "uuV", "ddV")
    cache.addMatchedEvent(VersionID("pair2", "id2"), "uuV")
    val updates = cache.retrieveAllUnmatchedEvents

    assertEquals(0, updates.length)
  }

  @Test
  def shouldIgnoreMatchedEventWhenNoOverridableUnmatchedEventIsStored {
    val timestamp = new DateTime()
    // Get an initial event and a sequence number
    cache.addReportableUnmatchedEvent(VersionID("pair2", "id2"), timestamp, "uV", "dV")
    val unmatched = cache.retrieveAllUnmatchedEvents
    val lastSeq = unmatched.last.seqId

    // Add a matched event for something that we don't have marked as unmatched
    cache.addMatchedEvent(VersionID("pair3", "id3"), "eV")
    val updates = cache.retrieveEventsSince(lastSeq)
    assertEquals(0, updates.length)
  }

  @Test
  def shouldOverrideOlderUnmatchedEventsWhenNewMismatchesOccur {
    // Add two events for the same object, and then ensure the old list only includes the most recent one
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID("pair2", "id2"), timestamp, "uV", "dV")
    cache.addReportableUnmatchedEvent(VersionID("pair2", "id2"), timestamp, "uV2", "dV2")

    val unmatched = cache.retrieveAllUnmatchedEvents
    assertEquals(1, unmatched.length)
    assertEquals(VersionID("pair2", "id2"), unmatched(0).objId)
    assertEquals("uV2", unmatched(0).upstreamVsn)
    assertEquals("dV2", unmatched(0).downstreamVsn)
  }
}