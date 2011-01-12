/**
 * Copyright (C) 2011 LShift Ltd.
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
import net.lshift.diffa.kernel.config.{Endpoint, ConfigStore}
import org.joda.time.DateTime
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.matching.{MatchingStatusListener, EventMatcher, MatchingManager}
import net.lshift.diffa.kernel.actors.PairPolicyClient
import org.easymock.EasyMock

/**
 * Test cases for the default session manager.
 */
class StubParticipantProtocolFactory extends ParticipantProtocolFactory {

  def createDownstreamParticipant(address: String, protocol: String) = {
    new DownstreamParticipant() {
      def generateVersion(entityBody:String) = null
      def invoke(actionId:String, entityId:String)  = null
      def queryAggregateDigests(constraints:Seq[QueryConstraint]) = null
      def queryEntityVersions(constraints:Seq[QueryConstraint]) = null
      def retrieveContent(id:String) = null
      def close() = ()
    }
  }
  def createUpstreamParticipant(address:String, protocol:String) = {
    new UpstreamParticipant() {
      def queryAggregateDigests(constraints:Seq[QueryConstraint]) = null
      def queryEntityVersions(constraints:Seq[QueryConstraint]) = null
      def invoke(actionId:String, entityId:String)  = null
      def retrieveContent(id:String) = null
      def close() = ()
    }
  }
  def supportsAddress(address:String, protocol:String) = true
}

class DefaultSessionManagerTest {
  val cacheProvider = new LocalSessionCacheProvider

  val listener1 = createStrictMock("listener1", classOf[DifferencingListener])
  val listener2 = createStrictMock("listener2", classOf[DifferencingListener])

  val matcher = createStrictMock("matcher", classOf[EventMatcher])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
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
  participantFactory.registerFactory(protocol1)
  participantFactory.registerFactory(protocol2)

  val pairPolicyClient = createStrictMock("pairPolicyClient", classOf[PairPolicyClient])

  val manager = new DefaultSessionManager(configStore, cacheProvider, matchingManager, versionPolicyManager, pairPolicyClient, participantFactory)
  verify(matchingManager); reset(matchingManager)    // The matching manager will have been called on session manager startup

  @Before
  def setupStubs = {
    // TODO consider using a stub factory to build stateful objects
    // that always just get copy and pasted
    // i.e. only stub out the behavior that actually care about and want to test
    // so in this example, everything above this comment should be expect() calls
    // and everything below should be stub() calls on a factory
    val u = Endpoint("1","http://foo.com", "application/json", "changes", true)
    val d = Endpoint("2","http://bar.com", "application/json", "changes", true)

    participantFactory.createUpstreamParticipant(u)
    participantFactory.createDownstreamParticipant(d)

    val pair1 = new net.lshift.diffa.kernel.config.Pair()
    pair1.versionPolicyName = "policy"
    pair1.upstream = u
    pair1.downstream = d

    val pair2 = new net.lshift.diffa.kernel.config.Pair()
    pair2.versionPolicyName = "policy"
    pair2.upstream = u
    pair2.downstream = d
        
    expect(configStore.getPair("pair")).andStubReturn(pair1)
    expect(configStore.getPair("pair2")).andStubReturn(pair2)
    expect(matchingManager.getMatcher("pair")).andStubReturn(Some(matcher))
    expect(matcher.isVersionIDActive(VersionID("pair", "id"))).andStubReturn(true)
    expect(matcher.isVersionIDActive(VersionID("pair", "id2"))).andStubReturn(false)

    replay(configStore, matchingManager, matcher)
  }

  def expectForPair(p:String)  = {
    val p1 = EasyMock.eq(p)
    val p2 = isA(classOf[DifferencingListener])
    expect(pairPolicyClient.syncPair(p1, p2)).andReturn(true).atLeastOnce
    replay(pairPolicyClient)
  }

  @Test
  def shouldAlwaysInformMatchEvents {

    expect(listener1.onMatch(VersionID("pair", "id"), "vsn"))
    replayAll

    expectForPair("pair")

    val session = manager.start(SessionScope.forPairs("pair"), listener1)
    manager.onMatch(VersionID("pair", "id"), "vsn")

    verifyAll
  }

  @Test
  def shouldNotTriggerMismatchEventsWhenIdIsActive {
    val timestamp = new DateTime()
    // Replay to get blank stubs
    replayAll

    expectForPair("pair")

    val session = manager.start(SessionScope.forPairs("pair"), listener1)
    
    manager.onMismatch(VersionID("pair", "id"), timestamp, "uvsn", "dvsn")
    verifyAll
  }

  @Test
  def shouldTriggerMismatchEventsWhenIdIsInactive {
    val timestamp = new DateTime()
    expect(listener1.onMismatch(VersionID("pair", "id2"), timestamp, "uvsn", "dvsn"))
    replayAll

    expectForPair("pair")

    val session = manager.start(SessionScope.forPairs("pair"), listener1)

    manager.onMismatch(VersionID("pair", "id2"), timestamp, "uvsn", "dvsn")
    verifyAll
  }

  // TODO Refactor this to deal with end() call in the manager
  @Test
  def shouldNoLongerInformRemovedListenerOfMatchEvents {
    val timestamp = new DateTime()
    replayAll

    expectForPair("pair")

    val session = manager.start(SessionScope.forPairs("pair"), listener1)
    manager.end("pair", listener1)
    manager.onMatch(VersionID("pair", "id"), "vsn")
    manager.onMismatch(VersionID("pair", "id"), timestamp, "uvsn", "dvsn")
    verifyAll
  }

  @Test
  def shouldNotInformListenerOfEventsOnOtherPairs {
    val timestamp = new DateTime()
    replayAll

    expectForPair("pair2")

    val session = manager.start(SessionScope.forPairs("pair2"), listener1)
    manager.onMatch(VersionID("pair", "id"), "vsn")
    manager.onMismatch(VersionID("pair", "id"), timestamp, "uvsn", "dvsn")
    verifyAll
  }

  @Test
  def shouldHandleExpiryOfAnEventThatIsNotCurrentlyPending {
    // This will frequently occur when a repair action occurs. A miscellaneous downstream will be seen, which the
    // correlation store will immediately match, but the EventMatcher will see as expiring.

    replayAll

    expectForPair("pair")

    val session = manager.start(SessionScope.forPairs("pair"), listener1)
    manager.onDownstreamExpired(VersionID("pair", "unknownid"), "dvsn")
  }

  private def replayAll = replay(listener1, listener2)
  private def verifyAll = verify(listener1, listener2)
}