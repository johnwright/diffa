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
import org.junit.Test
import org.joda.time.{DateTime, Interval}
import net.lshift.diffa.kernel.config.DiffaPairRef

/**
 * Test cases for the local domain cache.
 */
class LocalDomainCacheTest {
  val cache:DomainCache = new LocalDomainCache("domain")

  @Test
  def shouldMakeDomainAvailable() {
    assertEquals("domain", cache.domain)
  }

  @Test
  def shouldNotPublishPendingUnmatchedEventInAllUnmatchedList() {
    val now = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), now, "uV", "dV", now)
    val interval = new Interval(now.minusDays(1), now.plusDays(1))
    assertEquals(0, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldPublishUpgradedUnmatchedEventInAllUnmatchedList() {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair1",  "domain"), "id1"), unmatched.head.objId)
    assertEquals(timestamp, unmatched.head.detectedAt)
    assertEquals("uV", unmatched.head.upstreamVsn)
    assertEquals("dV", unmatched.head.downstreamVsn)
  }

  @Test
  def shouldIgnoreUpgradeRequestsForUnknownIDs() {
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))
    val interval = new Interval(new DateTime(), new DateTime())
    assertEquals(0, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldOverwritePendingEventsWhenNewPendingEventsArrive() {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV2", "dV2", timestamp)
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    // Even if there were multiple pending registrations, we should only see one unmatched event when we upgrade, and
    // it should use the details of the final pending event.
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair1",  "domain"), "id1"), unmatched.head.objId)
    assertEquals(timestamp, unmatched.head.detectedAt)
    assertEquals("uV2", unmatched.head.upstreamVsn)
    assertEquals("dV2", unmatched.head.downstreamVsn)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenUpgradedAlready() {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(1, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenCancelled() {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    assertTrue(cache.cancelPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), "uV"))
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(0, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldNotCancelPendingEventWhenProvidedVersionIsDifferent() {
    val timestamp = new DateTime()
    cache.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    assertFalse(cache.cancelPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), "uV-different"))
    cache.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(1, cache.retrieveUnmatchedEvents(interval).length)
  }

  @Test
  def shouldPublishAnAddedReportableUnmatchedEvent() {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    assertEquals(MatchState.UNMATCHED, unmatched.head.state)
    assertEquals(VersionID(DiffaPairRef("pair2",  "domain"), "id2"), unmatched.head.objId)
    assertEquals(timestamp, unmatched.head.detectedAt)
    assertEquals("uV", unmatched.head.upstreamVsn)
    assertEquals("dV", unmatched.head.downstreamVsn)
  }

  @Test
  def shouldReportUnmatchedEventWithinInterval() {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(size - frontFence - rearFence, unmatched.length)
  }

  @Test
  def shouldCountUnmatchedEventWithinInterval() {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatchedCount = cache.countEvents("pair2", interval)
    assertEquals(size - frontFence - rearFence, unmatchedCount)
  }

  def addUnmatchedEvents(start:DateTime, size:Int, frontFence:Int, rearFence:Int) : Interval = {
    for (i <- 1 to size) {
      val timestamp = start.plusMinutes(i)
      cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id" + i), timestamp, "uV", "dV", timestamp)
    }
    new Interval(start.plusMinutes(frontFence), start.plusMinutes(size - rearFence))
  }

  @Test
  def shouldPageReportableUnmatchedEvent() {
    val start = new DateTime(1982, 5, 5, 14, 15, 19, 0)
    val size = 100
    var frontFence = 20
    var rearFence = 50

    // Set a bound so that 30 events fall into the window
    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    // Create an interval that is wide enough to get every event ever
    val veryWideInterval = new Interval(start.minusDays(1), start.plusDays(1))

    val unmatched = cache.retrieveUnmatchedEvents(veryWideInterval)
    assertEquals(size, unmatched.length)

    // Requesting 19 elements with an offset of 10 from 30 elements should yield elements 10 through to 28
    val containedPage = cache.retrievePagedEvents("pair2", interval, 10, 19)
    assertEquals(19, containedPage.length)

    // Requesting 19 elements with an offset of 20 from 30 elements should yield elements 20 through to 29
    val splitPage = cache.retrievePagedEvents("pair2", interval, 20, 19)
    assertEquals(10, splitPage.length)

  }

  @Test
  def shouldAddMatchedEventThatOverridesUnmatchedEventWhenAskingForSequenceUpdate() {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    val lastSeq = unmatched.last.seqId

    cache.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uuV")
    val updates = cache.retrieveEventsSince(lastSeq)

    assertEquals(1, updates.length)
    assertEquals(MatchState.MATCHED, updates.head.state)
    // We don't know deterministically when the updated timestamp will be because this
    // is timestamped on the fly from within the implementation of the cache
    // but we do want to assert that it is not before the reporting timestamp
    assertFalse(timestamp.isAfter(updates.head.detectedAt))
    assertEquals(VersionID(DiffaPairRef("pair2", "domain"), "id2"), updates.head.objId)
  }

  @Test
  def shouldRemoveUnmatchedEventFromAllUnmatchedWhenAMatchHasBeenAdded() {
    val timestamp = new DateTime()
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)
    cache.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uuV")
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val updates = cache.retrieveUnmatchedEvents(interval)

    assertEquals(0, updates.length)
  }

  @Test
  def shouldIgnoreMatchedEventWhenNoOverridableUnmatchedEventIsStored() {
    val timestamp = new DateTime()
    // Get an initial event and a sequence number
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    val lastSeq = unmatched.last.seqId

    // Add a matched event for something that we don't have marked as unmatched
    cache.addMatchedEvent(VersionID(DiffaPairRef("pair3","domain"), "id3"), "eV")
    val updates = cache.retrieveEventsSince(lastSeq)
    assertEquals(0, updates.length)
  }

  @Test
  def shouldOverrideOlderUnmatchedEventsWhenNewMismatchesOccurWithDifferentDetails() {
    // Add two events for the same object, and then ensure the old list only includes the most recent one
    val timestamp = new DateTime()
    val seen = new DateTime().plusSeconds(5)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", timestamp)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV2", "dV2", seen)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id2"), "uV2", "dV2", timestamp, seen)
  }

  @Test
  def shouldRetainOlderUnmatchedEventsWhenNewEventsAreAddedWithSameDetailsButUpdateTheSeenTime() {
    // Add two events for the same object with all the same details, and ensure that we don't modify the event
    val timestamp = new DateTime()
    val newSeen = new DateTime().plusSeconds(5)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", timestamp)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp.plusSeconds(15), "uV", "dV", newSeen)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    assertEquals(1, unmatched.length)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id2"), "uV", "dV", timestamp, newSeen)
  }

  @Test
  def shouldRemoveEventsNotSeenAfterTheGivenCutoff() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    val seen2 = timestamp.plusSeconds(8)
    val cutoff = timestamp.plusSeconds(9)
    val seen3 = timestamp.plusSeconds(10)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)   // Before the cutoff, will be removed
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)   // Before the cutoff, will be removed
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    cache.matchEventsOlderThan("pair2", cutoff)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id3"), "uV", "dV", timestamp, seen3)
  }

  @Test
  def shouldNotRemoveEventsFromADifferentPair() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    cache.matchEventsOlderThan("pair1", seen1)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV", "dV", timestamp, seen1)
  }

  @Test
  def shouldNotRemoveEventsSeenExactlyAtTheGivenCutoff() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    cache.matchEventsOlderThan("pair2", seen1)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = cache.retrieveUnmatchedEvents(interval)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV", "dV", timestamp, seen1)
  }

  @Test
  def shouldAddMatchEventsForThoseRemovedByACutoff() {
    val now = new DateTime
    val timestamp = now .minusSeconds(10)
    val seen1 = now .plusSeconds(5)
    val seen2 = now .plusSeconds(8)
    val cutoff = now .plusSeconds(9)
    val seen3 = now .plusSeconds(10)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)   // Before the cutoff, will be removed
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)   // Before the cutoff, will be removed
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    cache.matchEventsOlderThan("pair2", cutoff)

    val events = cache.retrieveEventsSince("0")
    assertEquals(3, events.length)

    validateUnmatchedEvent(events(0), VersionID(DiffaPairRef("pair2","domain"), "id3"), "uV", "dV", timestamp, seen3)
    validateMatchedEvent(events(1), VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV", now)
    validateMatchedEvent(events(2), VersionID(DiffaPairRef("pair2","domain"), "id2"), "uV", now)
  }
  @Test
  def shouldNotRemoveOrDuplicateMatchEventsSeenBeforeTheCutoff() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    val cutoff = timestamp.plusSeconds(20)
    cache.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    cache.addMatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV")
    cache.matchEventsOlderThan("pair2", cutoff)

    val events = cache.retrieveEventsSince("0")
    assertEquals(1, events.length)
    validateMatchedEvent(events(0), VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV", timestamp)
  }


  //
  // Helpers
  //

  def validateUnmatchedEvent(event:DifferenceEvent, id:VersionID, usVsn:String, dsVsn:String, timestamp:DateTime, seen:DateTime) {
    assertEquals(id, event.objId)
    assertEquals(MatchState.UNMATCHED, event.state)
    assertEquals(usVsn, event.upstreamVsn)
    assertEquals(dsVsn, event.downstreamVsn)
    assertEquals(timestamp, event.detectedAt)
    assertEquals(seen, event.lastSeen)
  }

  def validateMatchedEvent(event:DifferenceEvent, id:VersionID, vsn:String, now:DateTime) {
    assertEquals(id, event.objId)
    assertEquals(MatchState.MATCHED, event.state)
    assertEquals(vsn, event.upstreamVsn)
    assertEquals(vsn, event.downstreamVsn)
    assertTrue(!event.detectedAt.isBefore(now))      // Detection should be some time at or after now
    assertTrue(!event.lastSeen.isBefore(now))        // Last seen should be some time at or after now
  }
}