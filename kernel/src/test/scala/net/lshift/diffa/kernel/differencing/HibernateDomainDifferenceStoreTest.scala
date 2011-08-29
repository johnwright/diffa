package net.lshift.diffa.kernel.differencing

import org.hibernate.cfg.Configuration
import org.hibernate.exception.ConstraintViolationException
import org.joda.time.{Interval, DateTime}
import org.junit.Assert._
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.frontend.{EndpointDef, PairDef}
import org.junit._
import org.hibernate.SessionFactory
import system.HibernateSystemConfigStore
import net.lshift.diffa.kernel.util.DerbyHelper

/**
 * Test cases for the HibernateDomainDifferenceStore.
 */
class HibernateDomainDifferenceStoreTest {
  import HibernateDomainDifferenceStoreTest._

  @Before
  def clear() {
    diffStore.clearAllDifferences

    val configStore = new HibernateDomainConfigStore(sf)
    val systemConfigStore = new HibernateSystemConfigStore(sf)

    val domain = Domain("domain")
    systemConfigStore.createOrUpdateDomain(domain)
    val us = EndpointDef(name = "upstream", contentType = "application/json")
    val ds = EndpointDef(name = "downstream", contentType = "application/json")
    configStore.createOrUpdateEndpoint(domain.name, us)
    configStore.createOrUpdateEndpoint(domain.name, ds)

    val pairTemplate = PairDef(upstreamName = us.name, downstreamName = ds.name)
    val pair1 = pairTemplate.copy(key = "pair1")
    val pair2 = pairTemplate.copy(key = "pair2")

    configStore.listPairs(domain.name).foreach(p => configStore.deletePair(domain.name, p.key))
    configStore.createOrUpdatePair(domain.name, pair1)
    configStore.createOrUpdatePair(domain.name, pair2)
  }

  @Test
  def shouldNotPublishPendingUnmatchedEventInAllUnmatchedList() {
    val now = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), now, "uV", "dV", now)
    val interval = new Interval(now.minusDays(1), now.plusDays(1))
    assertEquals(0, diffStore.retrieveUnmatchedEvents("domain", interval).length)
  }

  @Test
  def shouldPublishUpgradedUnmatchedEventInAllUnmatchedList() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair1",  "domain"), "id1"), unmatched.head.objId)
    assertEquals(timestamp, unmatched.head.detectedAt)
    assertEquals("uV", unmatched.head.upstreamVsn)
    assertEquals("dV", unmatched.head.downstreamVsn)
  }

  @Test
  def shouldIgnoreUpgradeRequestsForUnknownIDs() {
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))
    val interval = new Interval(new DateTime(), new DateTime())
    assertEquals(0, diffStore.retrieveUnmatchedEvents("domain", interval).length)
  }

  @Test
  def shouldOverwritePendingEventsWhenNewPendingEventsArrive() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV2", "dV2", timestamp)
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    // Even if there were multiple pending registrations, we should only see one unmatched event when we upgrade, and
    // it should use the details of the final pending event.
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(1, unmatched.length)
    assertEquals(VersionID(DiffaPairRef("pair1",  "domain"), "id1"), unmatched.head.objId)
    assertEquals(timestamp, unmatched.head.detectedAt)
    assertEquals("uV2", unmatched.head.upstreamVsn)
    assertEquals("dV2", unmatched.head.downstreamVsn)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenUpgradedAlready() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(1, diffStore.retrieveUnmatchedEvents("domain", interval).length)
  }

  @Test
  def shouldIgnoreUpgradeRequestWhenPendingEventHasBeenCancelled() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    assertTrue(diffStore.cancelPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), "uV"))
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(0, diffStore.retrieveUnmatchedEvents("domain", interval).length)
  }

  @Test
  def shouldNotCancelPendingEventWhenProvidedVersionIsDifferent() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), timestamp, "uV", "dV", timestamp)
    assertFalse(diffStore.cancelPendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"), "uV-different"))
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair1", "domain"), "id1"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    assertEquals(1, diffStore.retrieveUnmatchedEvents("domain", interval).length)
  }

  @Test
  def shouldPublishAnAddedReportableUnmatchedEvent() {
    val timestamp = new DateTime()
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(1, unmatched.length)
    assertEquals(MatchState.UNMATCHED, unmatched.head.state)
    assertEquals(VersionID(DiffaPairRef("pair2",  "domain"), "id2"), unmatched.head.objId)
    assertEquals(timestamp, unmatched.head.detectedAt)
    assertEquals("uV", unmatched.head.upstreamVsn)
    assertEquals("dV", unmatched.head.downstreamVsn)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldNotBeAbleToIgnoreDifferenceViaWrongDomain() {
    val timestamp = new DateTime()
    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.ignoreEvent("domain2", evt.seqId)
  }

  @Test
  def shouldNotPublishAnIgnoredReportableUnmatchedEventInRetrieveUnmatchedEventsQuery() {
    val timestamp = new DateTime()
    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.ignoreEvent("domain", evt.seqId)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(0, unmatched.length)
  }

  @Test
  def shouldReportUnmatchedEventWithinInterval() {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(size - frontFence - rearFence, unmatched.length)
  }

  @Test
  def shouldCountUnmatchedEventWithinInterval() {
    val start = new DateTime(2004, 11, 6, 3, 5, 15, 0)
    val size = 60
    var frontFence = 10
    var rearFence = 10

    val interval = addUnmatchedEvents(start, size, frontFence, rearFence)

    val unmatchedCount = diffStore.countEvents(DiffaPairRef("pair2", "domain"), interval)
    assertEquals(size - frontFence - rearFence, unmatchedCount)
  }

  @Test
  def shouldNotCountIgnoredEvents() {
    val timestamp = new DateTime

    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.ignoreEvent("domain", evt.seqId)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatchedCount = diffStore.countEvents(DiffaPairRef("pair2", "domain"), interval)
    assertEquals(0, unmatchedCount)
  }

  def addUnmatchedEvents(start:DateTime, size:Int, frontFence:Int, rearFence:Int) : Interval = {
    for (i <- 1 to size) {
      val timestamp = start.plusMinutes(i)
      diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id" + i), timestamp, "uV", "dV", timestamp)
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

    val unmatched = diffStore.retrieveUnmatchedEvents("domain", veryWideInterval)
    assertEquals(size, unmatched.length)

    // Requesting 19 elements with an offset of 10 from 30 elements should yield elements 10 through to 28
    val containedPage = diffStore.retrievePagedEvents(DiffaPairRef("pair2", "domain"), interval, 10, 19)
    assertEquals(19, containedPage.length)

    // Requesting 19 elements with an offset of 20 from 30 elements should yield elements 20 through to 29
    val splitPage = diffStore.retrievePagedEvents(DiffaPairRef("pair2", "domain"), interval, 20, 19)
    assertEquals(10, splitPage.length)

  }

  @Test
  def shouldNotPublishAnIgnoredReportableUnmatchedEventInPagedEventQuery() {
    val timestamp = new DateTime()
    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.ignoreEvent("domain", evt.seqId)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val containedPage = diffStore.retrievePagedEvents(DiffaPairRef("pair2", "domain"), interval, 0, 100)
    assertEquals(0, containedPage.length)
  }

  @Test
  def shouldPublishAIgnoredReportableUnmatchedEventInPagedEventQueryWhenIgnoredEntitiesAreRequested() {
    val timestamp = new DateTime()
    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.ignoreEvent("domain", evt.seqId)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val containedPage = diffStore.retrievePagedEvents(DiffaPairRef("pair2", "domain"), interval, 0, 100, EventOptions(includeIgnored = true))

    assertEquals(1, containedPage.length)
    assertEquals(VersionID(DiffaPairRef("pair2", "domain"), "id2"), containedPage(0).objId)
  }

  @Test
  def shouldPublishAnEventThatHasBeenUnignored() {
    val timestamp = new DateTime()
    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    val ignored = diffStore.ignoreEvent("domain", evt.seqId)
    diffStore.unignoreEvent("domain", ignored.seqId)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val containedPage = diffStore.retrievePagedEvents(DiffaPairRef("pair2", "domain"), interval, 0, 100)

    assertEquals(1, containedPage.length)
    assertEquals(VersionID(DiffaPairRef("pair2", "domain"), "id2"), containedPage(0).objId)
  }

  @Test
  def shouldAddMatchedEventThatOverridesUnmatchedEventWhenAskingForSequenceUpdate() {
    val timestamp = new DateTime()
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    val lastSeq = unmatched.last.seqId

    diffStore.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uuV")
    val updates = diffStore.retrieveEventsSince("domain", lastSeq)

    assertEquals(1, updates.length)
    assertEquals(MatchState.MATCHED, updates.head.state)
    // We don't know deterministically when the updated timestamp will be because this
    // is timestamped on the fly from within the implementation of the cache
    // but we do want to assert that it is not before the reporting timestamp
    assertFalse(timestamp.isAfter(updates.head.detectedAt))
    assertEquals(VersionID(DiffaPairRef("pair2", "domain"), "id2"), updates.head.objId)
  }

  @Test
  def shouldAddMatchedEventThatOverridesIgnoredEventWhenAskingForSequenceUpdate() {
    val timestamp = new DateTime()
    val newUnmatched = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    val lastSeq = unmatched.last.seqId

    diffStore.ignoreEvent("domain", newUnmatched.seqId)
    val updates = diffStore.retrieveEventsSince("domain", lastSeq)

    assertEquals(1, updates.length)
    assertEquals(MatchState.IGNORED, updates.head.state)  // Match events for ignored differences have a state IGNORED
    // We don't know deterministically when the updated timestamp will be because this
    // is timestamped on the fly from within the implementation of the cache
    // but we do want to assert that it is not before the reporting timestamp
    assertFalse(timestamp.isAfter(updates.head.detectedAt))
    assertEquals(VersionID(DiffaPairRef("pair2", "domain"), "id2"), updates.head.objId)
  }

  @Test
  def shouldRemoveUnmatchedEventFromAllUnmatchedWhenAMatchHasBeenAdded() {
    val timestamp = new DateTime()
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uuV", "ddV", timestamp)
    diffStore.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uuV")
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val updates = diffStore.retrieveUnmatchedEvents("domain", interval)

    assertEquals(0, updates.length)
  }

  @Test
  def shouldIgnoreMatchedEventWhenNoOverridableUnmatchedEventIsStored() {
    val timestamp = new DateTime()
    // Get an initial event and a sequence number
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    val lastSeq = unmatched.last.seqId

    // Add a matched event for something that we don't have marked as unmatched
    diffStore.addMatchedEvent(VersionID(DiffaPairRef("pair3","domain"), "id3"), "eV")
    val updates = diffStore.retrieveEventsSince("domain", lastSeq)
    assertEquals(0, updates.length)
  }

  @Test
  def shouldOverrideOlderUnmatchedEventsWhenNewMismatchesOccurWithDifferentDetails() {
    // Add two events for the same object, and then ensure the old list only includes the most recent one
    val timestamp = new DateTime()
    val seen = new DateTime().plusSeconds(5)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV2", "dV2", seen)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(1, unmatched.length)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id2"), "uV2", "dV2", timestamp, seen)
  }

  @Test
  def shouldRetainOlderUnmatchedEventsWhenNewEventsAreAddedWithSameDetailsButUpdateTheSeenTime() {
    // Add two events for the same object with all the same details, and ensure that we don't modify the event
    val timestamp = new DateTime()
    val newSeen = new DateTime().plusSeconds(5)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp.plusSeconds(15), "uV", "dV", newSeen)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
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
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)   // Before the cutoff, will be removed
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)   // Before the cutoff, will be removed
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    diffStore.matchEventsOlderThan(DiffaPairRef("pair2","domain"), cutoff)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id3"), "uV", "dV", timestamp, seen3)
  }

  @Test
  def shouldNotRemoveEventsFromADifferentPair() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    diffStore.matchEventsOlderThan(DiffaPairRef("pair2","domain"), seen1)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    validateUnmatchedEvent(unmatched(0), VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV", "dV", timestamp, seen1)
  }

  @Test
  def shouldNotRemoveEventsSeenExactlyAtTheGivenCutoff() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    diffStore.matchEventsOlderThan(DiffaPairRef(domain = "domain", key = "pair2"), seen1)

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
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
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)   // Before the cutoff, will be removed
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)   // Before the cutoff, will be removed
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    diffStore.matchEventsOlderThan(DiffaPairRef("pair2","domain"), cutoff)

    val events = diffStore.retrieveEventsSince("domain", "0")
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
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)
    diffStore.addMatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV")
    diffStore.matchEventsOlderThan(DiffaPairRef("pair2","domain"), cutoff)

    val events = diffStore.retrieveEventsSince("domain", "0")
    assertEquals(1, events.length)
    validateMatchedEvent(events(0), VersionID(DiffaPairRef("pair2","domain"), "id1"), "uV", timestamp)
  }

  @Test
  def shouldAllowRetrievalOfReportedEvent() {
    val timestamp = new DateTime()
    val evt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)

    val retrieved = diffStore.getEvent("domain", evt.seqId)
    validateUnmatchedEvent(retrieved, evt.objId, evt.upstreamVsn, evt.downstreamVsn, evt.detectedAt, evt.lastSeen)
  }

  @Test
  def shouldThrowExceptionWhenRetrievingNonExistentSeqNumber() {
    try {
      diffStore.getEvent("domain", "55")    // Cache should be empty, so any seqId should be invalid
      fail("Should have thrown InvalidSequenceNumberException")
    } catch {
      case e:InvalidSequenceNumberException => assertEquals("55", e.id)
    }
  }

  @Test(expected = classOf[InvalidSequenceNumberException])
  def shouldThrowExceptionWhenRetrievingEventBySequenceNumberOfRemovedEvent() {
    val timestamp = new DateTime()
    val unmatchedEvt = diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.addMatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), "uV")

    diffStore.getEvent("domain", unmatchedEvt.seqId)    // Unmatched event should have been removed when matched event was added
  }

  @Test
  def shouldRemoveEventsWhenDomainIsRemoved() {
    val timestamp = new DateTime()
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id3"), timestamp, "uV", "dV", timestamp)

    diffStore.removeDomain("domain")

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(0, unmatched.length)
  }

  @Test
  def shouldRemovePendingEventsWhenDomainIsRemoved() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)

    diffStore.removeDomain("domain")

    // Upgrade the pending difference we previously created. We shouldn't see any differences, because we should
    // have just submitted an upgrade for a pending event that doesn't exist.
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(0, unmatched.length)
  }

  @Test
  def shouldRemoveMatches() {
    val timestamp = new DateTime()
    val seen1 = timestamp.plusSeconds(5)
    val seen2 = timestamp.plusSeconds(8)
    val cutoff = timestamp.plusSeconds(9)
    val seen3 = timestamp.plusSeconds(10)
    val afterAll = timestamp.plusSeconds(20)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id1"), timestamp, "uV", "dV", seen1)   // Before the cutoff, will be removed
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id2"), timestamp, "uV", "dV", seen2)   // Before the cutoff, will be removed
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2","domain"), "id3"), timestamp, "uV", "dV", seen3)
    diffStore.matchEventsOlderThan(DiffaPairRef("pair2","domain"), afterAll)     // Convert all the events to matches
    diffStore.expireMatches(cutoff)

    val events = diffStore.retrieveEventsSince("domain", "0")
    assertEquals(0, events.length)
  }

  @Test
  def shouldRemoveEventsWhenPairIsRemoved() {
    val timestamp = new DateTime()
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)
    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id3"), timestamp, "uV", "dV", timestamp)

    diffStore.removePair("pair2")

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(0, unmatched.length)
  }

  @Test
  def shouldRemovePendingEventsWhenPairIsRemoved() {
    val timestamp = new DateTime()
    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"), timestamp, "uV", "dV", timestamp)

    diffStore.removePair("pair2")

    // Upgrade the pending difference we previously created. We shouldn't see any differences, because we should
    // have just submitted an upgrade for a pending event that doesn't exist.
    diffStore.upgradePendingUnmatchedEvent(VersionID(DiffaPairRef("pair2", "domain"), "id2"))

    val interval = new Interval(timestamp.minusDays(1), timestamp.plusDays(1))
    val unmatched = diffStore.retrieveUnmatchedEvents("domain", interval)
    assertEquals(0, unmatched.length)
  }

  @Test(expected = classOf[ConstraintViolationException])
  def shouldFailToAddReportableEventForNonExistentPair() {
    val lastUpdate = new DateTime()
    val seen = lastUpdate.plusSeconds(5)

    diffStore.addReportableUnmatchedEvent(VersionID(DiffaPairRef("nonexistent-pair1", "domain"), "id1"), lastUpdate, "uV", "dV", seen)
  }

  @Test(expected = classOf[ConstraintViolationException])
  def shouldFailToAddPendingEventForNonExistentPair() {
    val lastUpdate = new DateTime()
    val seen = lastUpdate.plusSeconds(5)

    diffStore.addPendingUnmatchedEvent(VersionID(DiffaPairRef("nonexistent-pair2", "domain"), "id1"), lastUpdate, "uV", "dV", seen)
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

object HibernateDomainDifferenceStoreTest {
  private val config =
      new Configuration().
        addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
        addResource("net/lshift/diffa/kernel/differencing/DifferenceEvents.hbm.xml").
        setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
        setProperty("hibernate.connection.url", "jdbc:derby:target/domainCache;create=true").
        setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver").
        setProperty("hibernate.cache.region.factory_class", "net.sf.ehcache.hibernate.EhCacheRegionFactory").
        setProperty("hibernate.connection.autocommit", "true") // Turn this on to make the tests repeatable,
                                                               // otherwise the preparation step will not get committed

  val sf:SessionFactory = config.buildSessionFactory
  (new HibernateConfigStorePreparationStep).prepare(sf, config)
  val diffStore = new HibernateDomainDifferenceStore(sf)


  @AfterClass
  def close() {
    sf.close()
    DerbyHelper.shutdown("target/domainCache")
  }
}