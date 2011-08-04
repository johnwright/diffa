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

import org.easymock.EasyMock._
import net.lshift.diffa.kernel.events.VersionID
import org.junit.{Before, Test}
import org.junit.Assert._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.matching.{MatchingStatusListener, EventMatcher, MatchingManager}
import net.lshift.diffa.kernel.actors.PairPolicyClient
import org.easymock.EasyMock
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.participant.scanning.ScanConstraint
import net.lshift.diffa.kernel.config.{DiffaPairRef, Domain, Endpoint, DomainConfigStore, Pair => DiffaPair}

/**
 * Test cases for the default session manager.
 */
class StubParticipantProtocolFactory
    extends ScanningParticipantFactory {

  def supportsAddress(address:String, protocol:String) = true

  def createParticipantRef(address: String, protocol: String) = new ScanningParticipantRef {
    def scan(constraints: Seq[ScanConstraint], aggregations: Seq[CategoryFunction]) = null
    def close() {}
  }
}

class DefaultSessionManagerTest {
  val cacheProvider = new LocalSessionCacheProvider

  val listener1 = createStrictMock("listener1", classOf[DifferencingListener])
  val listener2 = createStrictMock("listener2", classOf[DifferencingListener])

  val matcher = createStrictMock("matcher", classOf[EventMatcher])

  val systemConfigStore = createStrictMock("systemConfigStore", classOf[SystemConfigStore])
  val matchingManager = createStrictMock("matchingManager", classOf[MatchingManager])
  matchingManager.addListener(anyObject.asInstanceOf[MatchingStatusListener]); expectLastCall.once
  replay(matchingManager)

  // TODO The versionPolicyManager and participantFactory should probably go into some
  // kind of test factory - i.e. look out for copy and paste

  val versionPolicy = createStrictMock("vp", classOf[VersionPolicy])


  val versionPolicyManager = new VersionPolicyManager()
  versionPolicyManager.registerPolicy("policy", versionPolicy)
  private val protocol1 = new StubParticipantProtocolFactory()
  private val protocol2 = new StubParticipantProtocolFactory()

  val participantFactory = new ParticipantFactory()
  participantFactory.registerScanningFactory(protocol1)
  participantFactory.registerScanningFactory(protocol2)

  val pairPolicyClient = createStrictMock("pairPolicyClient", classOf[PairPolicyClient])
  EasyMock.checkOrder(pairPolicyClient, false)

  val manager = new DefaultSessionManager(systemConfigStore, cacheProvider, matchingManager, versionPolicyManager, pairPolicyClient, participantFactory)
  verify(matchingManager); reset(matchingManager)    // The matching manager will have been called on session manager startup

  val u = Endpoint(name = "1", scanUrl = "http://foo.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")
  val d = Endpoint(name = "2", scanUrl = "http://bar.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")

  val domainName = "domain"
  val domain = Domain(name=domainName)

  // TODO Do we still need this pair?
  val pair = DiffaPair(key = "pair", domain = domain, upstream = u, downstream = d)

  val pair1 = DiffaPair(key = "pair1", domain = domain, versionPolicyName = "policy", upstream = u, downstream = d)
  val pair2 = DiffaPair(key = "pair2", domain = domain, versionPolicyName = "policy", upstream = u, downstream = d)
  
  @Before
  def setupStubs = {
    // TODO consider using a stub factory to build stateful objects
    // that always just get copy and pasted
    // i.e. only stub out the behavior that actually care about and want to test
    // so in this example, everything above this comment should be expect() calls
    // and everything below should be stub() calls on a factory
   
    participantFactory.createUpstreamParticipant(u)
    participantFactory.createDownstreamParticipant(d) 

    expect(systemConfigStore.getPair(domainName, "pair1")).andStubReturn(pair1)
    expect(systemConfigStore.getPair(domainName, "pair2")).andStubReturn(pair2)
    expect(systemConfigStore.listPairs).andStubReturn(Seq(pair1,pair2))
    expect(matchingManager.getMatcher(pair1)).andStubReturn(Some(matcher))
    expect(matchingManager.getMatcher(pair2)).andStubReturn(Some(matcher))
    expect(matcher.isVersionIDActive(VersionID(pair1.asRef, "id"))).andStubReturn(true)
    expect(matcher.isVersionIDActive(VersionID(pair2.asRef, "id"))).andStubReturn(true)
    expect(matcher.isVersionIDActive(VersionID(pair1.asRef, "id2"))).andStubReturn(false)

    replay(systemConfigStore, matchingManager, matcher)
  }

  def expectDifferenceForPair(pairs:DiffaPair*)  = {
    pairs.foreach(p => {
      expect(pairPolicyClient.difference(p)).atLeastOnce
    })

    replay(pairPolicyClient)
  }
  def expectScanAndDifferenceForPair(pairs:DiffaPair*)  = {
    pairs.foreach(p => {
      expect(pairPolicyClient.difference(p)).atLeastOnce
    })
    pairs.foreach(p => {
      expect(pairPolicyClient.scanPair(p)).atLeastOnce
    })

    replay(pairPolicyClient)
  }

  @Test
  def shouldTriggerScanOnRequest = {
    expectScanAndDifferenceForPair(pair1,pair2)
    replayAll

    manager.runScanForAllPairings

    verifyAll
  }

  @Test
  def shouldAlwaysInformMatchEvents {

    expect(listener1.onMatch(VersionID(DiffaPairRef("pair","domain"), "id"), "vsn", LiveWindow))
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.start(SessionScope.forPairs("pair"), listener1)
    manager.onMatch(VersionID(DiffaPairRef("pair","domain"), "id"), "vsn", LiveWindow)

    verifyAll
  }

  @Test
  def shouldNotTriggerMismatchEventsWhenIdIsActive {
    val timestamp = new DateTime()
    // Replay to get blank stubs
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.start(SessionScope.forPairs("pair1"), listener1)
    
    manager.onMismatch(VersionID(DiffaPairRef("pair1", domainName), "id"), timestamp, "uvsn", "dvsn", LiveWindow)
    verifyAll
  }

  @Test
  def shouldTriggerMismatchEventsWhenIdIsInactive {
    val timestamp = new DateTime()
    expect(listener1.onMismatch(VersionID(DiffaPairRef("pair1", domainName), "id2"), timestamp, "uvsn", "dvsn", LiveWindow))
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.start(SessionScope.forPairs("pair1"), listener1)

    manager.onMismatch(VersionID(DiffaPairRef("pair1", domainName), "id2"), timestamp, "uvsn", "dvsn", LiveWindow)
    verifyAll
  }

  // TODO Refactor this to deal with end() call in the manager
  @Test
  def shouldNoLongerInformRemovedListenerOfMatchEvents {
    val timestamp = new DateTime()
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.start(SessionScope.forPairs("pair"), listener1)
    manager.end(pair, listener1)
    manager.onMatch(VersionID(DiffaPairRef("pair1", domainName),  "id"), "vsn", LiveWindow)
    manager.onMismatch(VersionID(DiffaPairRef("pair2", domainName), "id"), timestamp, "uvsn", "dvsn", LiveWindow)
    verifyAll
  }

  // TODO This test seems to be very buggy
  @Test
  def shouldNotInformListenerOfEventsOnOtherPairs {
    val timestamp = new DateTime()
    expect(listener1.onMatch(VersionID(DiffaPairRef("pair1", domainName), "id"), "vsn", LiveWindow))
    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.start(SessionScope.forPairs("pair"), listener1)
    manager.onMatch(VersionID(DiffaPairRef("pair1", domainName), "id"), "vsn", LiveWindow)
    manager.onMismatch(VersionID(DiffaPairRef("pair2", domainName), "id"), timestamp, "uvsn", "dvsn", LiveWindow)
    verifyAll
  }

  // TODO This test appears to be wrong
  @Test
  def shouldHandleExpiryOfAnEventThatIsNotCurrentlyPending {
    // This will frequently occur when a repair action occurs. A miscellaneous downstream will be seen, which the
    // correlation store will immediately match, but the EventMatcher will see as expiring.

    replayAll

    expectDifferenceForPair(pair1, pair2)

    manager.start(SessionScope.forPairs("pair"), listener1)
    manager.onDownstreamExpired(VersionID(DiffaPairRef("pair",domainName), "unknownid"), "dvsn")
  }

  @Test
  def shouldTrackStateOfPairsForExplicitScopeSession {
    // Create a session that contains our given pair
    replayAll
    expectScanAndDifferenceForPair(pair1,pair2)
    val sessionId = manager.start(SessionScope.forPairs("pair"), listener1)

    // Initial state of all pairs should be "unknown"
    assertEquals(Map("pair1" -> PairScanState.UNKNOWN, "pair2" -> PairScanState.UNKNOWN), manager.retrievePairScanStates(sessionId))

    // Start the initial scan
    manager.runScan(sessionId)

    // Query for the states associated. We should get back an entry for pair in "scanning", since the stubs
    // don't notify of completion
    assertEquals(Map("pair1" -> PairScanState.SCANNING,"pair2" -> PairScanState.SCANNING), manager.retrievePairScanStates(sessionId))

    // Notify that the pair is now in Up To Date state
    manager.pairScanStateChanged(pair1, PairScanState.UP_TO_DATE)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.SCANNING), manager.retrievePairScanStates(sessionId))

    // Start a scan. We should enter the scanning state again
    manager.runScan(sessionId)
    assertEquals(Map("pair1" -> PairScanState.SCANNING,"pair2" -> PairScanState.SCANNING), manager.retrievePairScanStates(sessionId))

    // Notify that the pair is now in Failed state
    manager.pairScanStateChanged(pair1, PairScanState.FAILED)
    assertEquals(Map("pair1" -> PairScanState.FAILED,"pair2" -> PairScanState.SCANNING), manager.retrievePairScanStates(sessionId))
  }

  @Test
  def shouldTrackStateOfPairsForImplicitScopeSession {
    // Create a session that contains all pairs
    replayAll
    expectScanAndDifferenceForPair(pair1, pair2)
    val sessionId = manager.start(SessionScope.all, listener1)

    // Query for the states associated. We should get back an entry for pair in "unknown"
    assertEquals(Map("pair1" -> PairScanState.UNKNOWN, "pair2" -> PairScanState.UNKNOWN),
      manager.retrievePairScanStates(sessionId))

    assertEquals(Map("pair1" -> PairScanState.UNKNOWN, "pair2" -> PairScanState.UNKNOWN),
      manager.retrieveAllPairScanStates)

    // Notify that the pair1 is now in Up To Date state
    manager.pairScanStateChanged(pair1, PairScanState.UP_TO_DATE)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.UNKNOWN),
      manager.retrievePairScanStates(sessionId))

    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.UNKNOWN),
      manager.retrieveAllPairScanStates)

    // Notify that the pair2 is now in Failed state
    manager.pairScanStateChanged(pair2, PairScanState.FAILED)
    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.FAILED),
      manager.retrievePairScanStates(sessionId))

    assertEquals(Map("pair1" -> PairScanState.UP_TO_DATE, "pair2" -> PairScanState.FAILED),
      manager.retrieveAllPairScanStates)

    // Start a scan. We should enter the scanning state again
    manager.runScan(sessionId)
    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.SCANNING),
      manager.retrievePairScanStates(sessionId))

    assertEquals(Map("pair1" -> PairScanState.SCANNING, "pair2" -> PairScanState.SCANNING),
      manager.retrieveAllPairScanStates)
  }

  @Test
  def shouldReportUnknownScanStateForRemovedPairs {
    // Create a session that contains our given pair
    replayAll
    expectDifferenceForPair(pair1, pair2)
    val sessionId = manager.start(SessionScope.forPairs("pair"), listener1)

    // If we delete the pair, then the scan state should return the pair with an Unknown status
    manager.onDeletePair(pair1)
    assertEquals(Map("pair1" -> PairScanState.UNKNOWN,"pair2" -> PairScanState.UNKNOWN), manager.retrievePairScanStates(sessionId))
  }

  private def replayAll = replay(listener1, listener2)
  private def verifyAll = verify(listener1, listener2)
}