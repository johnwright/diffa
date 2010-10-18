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

package net.lshift.diffa.kernel.actors

import org.junit.{Test, After, Before}
import net.lshift.diffa.kernel.events.VersionID
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.participants.ParticipantFactory
import net.lshift.diffa.kernel.config.{PairDef, ConfigStore}
import org.junit.Assert._
import net.lshift.diffa.kernel.differencing.{VersionPolicy, VersionPolicyManager, VersionCorrelationStore}

class PairActorTest {

  val pairKey = "A-B"
  val policyName = ""
  val pairDef = new PairDef()
  pairDef.pairKey = pairKey
  pairDef.versionPolicyName = policyName

  val store = createStrictMock("versionStore", classOf[VersionCorrelationStore])

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
  val participantFactory = org.easymock.classextension.EasyMock.createStrictMock("participantFactory", classOf[ParticipantFactory])
  val versionPolicyManager = org.easymock.classextension.EasyMock.createStrictMock("versionPolicyManager", classOf[VersionPolicyManager])
  val versionPolicy = createStrictMock("versionPolicy", classOf[VersionPolicy])
  expect(versionPolicyManager.lookupPolicy(policyName)).andReturn(Some(versionPolicy))
  org.easymock.classextension.EasyMock.replay(versionPolicyManager)

  val supervisor = new PairActorSupervisor(versionPolicyManager, configStore, participantFactory)

  @Before def start = supervisor.startActor(pairDef)
  @After def stop = supervisor.stopActor(pairKey)

  @Test
  def kickTyres = {
    val id = VersionID(pairKey, "foo")
    //expect(store.clearDownstreamVersion(id)).andReturn(new Correlation())
    //replay(store)
    //var c = clearDownstreamVersion(id)
    //assertNotNull(c)
    //verify(store)
    assertTrue(true)
  }
}