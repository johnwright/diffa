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

package net.lshift.diffa.kernel.maintenance

import org.junit.Test
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.config.system.SystemConfigStore
import net.lshift.diffa.kernel.config.DiffaPairRef._
import net.lshift.diffa.kernel.indexing.LuceneVersionCorrelationStoreFactory
import org.apache.lucene.store.MMapDirectory
import net.lshift.diffa.kernel.events.VersionID._
import net.lshift.diffa.kernel.events.VersionID
import net.lshift.diffa.kernel.differencing.StringAttribute._
import org.joda.time.{DateTime, DateTimeZone}
import net.lshift.diffa.kernel.matching.{MatchingStatusListener, MatchingManager, EventMatcher}
import net.lshift.diffa.kernel.differencing._
import net.lshift.diffa.kernel.participants.ParticipantFactory
import net.lshift.diffa.kernel.actors.PairPolicyClient
import org.junit.Assert._
import net.lshift.diffa.kernel.util.StoreSynchronizationUtils._
import net.lshift.diffa.kernel.config.{HibernateDomainConfigStoreTest, DomainConfigStore, DiffaPairRef}
import net.lshift.diffa.kernel.config.{Domain,Endpoint,Pair => DiffaPair}

class StoreSynchronizationTest {

  val domainName = "domain"

  val domain = Domain(name=domainName)

  val u = Endpoint(name = "1", scanUrl = "http://foo.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")
  val d = Endpoint(name = "2", scanUrl = "http://bar.com/scan", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json")

  val pair = DiffaPair(key = "pair", domain = domain, versionPolicyName = "policy", upstream = u, downstream = d)
  val pairRef = pair.asRef

  val dummyConfigStore = createMock(classOf[SystemConfigStore])
  expect(dummyConfigStore.
      maybeSystemConfigOption(VersionCorrelationStore.schemaVersionKey)).
      andStubReturn(Some(VersionCorrelationStore.currentSchemaVersion.toString))
  replay(dummyConfigStore)


  val stores = new LuceneVersionCorrelationStoreFactory("target", classOf[MMapDirectory], dummyConfigStore)
  val store = stores(pairRef)

  val listener = createStrictMock("listener1", classOf[DifferencingListener])

  val matcher = createStrictMock("matcher", classOf[EventMatcher])

  val systemConfigStore = createStrictMock("systemConfigStore", classOf[SystemConfigStore])

  replay(systemConfigStore)

  val domainConfigStore = createStrictMock("domainConfigStore", classOf[DomainConfigStore])
  //expect(domainConfigStore.listPairs(domainName)).andStubReturn(Seq(toPairDef(pair1), toPairDef(pair2)))
  replay(domainConfigStore)

  val matchingManager = createStrictMock("matchingManager", classOf[MatchingManager])
  matchingManager.addListener(anyObject.asInstanceOf[MatchingStatusListener]); expectLastCall.once
  replay(matchingManager)

  // TODO The versionPolicyManager and participantFactory should probably go into some
  // kind of test factory - i.e. look out for copy and paste

  val versionPolicy = createStrictMock("vp", classOf[VersionPolicy])


  val versionPolicyManager = new VersionPolicyManager()
  versionPolicyManager.registerPolicy("policy", versionPolicy)

  private val protocol1 = new StubParticipantProtocolFactory()

  val participantFactory = new ParticipantFactory()
  participantFactory.registerScanningFactory(protocol1)

  val pairPolicyClient = createStrictMock("pairPolicyClient", classOf[PairPolicyClient])
  checkOrder(pairPolicyClient, false)

  val domainDifferenceStore = HibernateDomainDifferenceStoreTest.diffStore
  //val domainConfigStore = HibernateDomainConfigStoreTest.domainConfigStore

  val diffsManager = new DefaultDifferencesManager(
    systemConfigStore, domainConfigStore, domainDifferenceStore, matchingManager,
    participantFactory, listener)

  @Test
  def storesShouldSynchronizeIncrementally = {

    assertEquals(None, diffsManager.lastRecordedVersion(pairRef))

    val writer = store.openWriter()

    val attributes = Map("foo" -> StringAttribute("bar"))
    val lastUpdated = new DateTime(2019,5,7,8,12,15,0, DateTimeZone.UTC)

    writer.storeUpstreamVersion(VersionID(pairRef, "id1"), attributes, lastUpdated, "vsn")
    writer.flush()

    replayCorrelationStore(diffsManager, versionPolicy, pair, TriggeredByScan)

    assertEquals(Some(1L), diffsManager.lastRecordedVersion(pairRef))

  }
}