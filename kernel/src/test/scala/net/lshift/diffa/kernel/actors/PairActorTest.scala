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
import org.easymock.EasyMock._
import org.junit.Assert._
import net.lshift.diffa.kernel.differencing._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.participants.{DownstreamParticipant, UpstreamParticipant, ParticipantFactory}
import net.lshift.diffa.kernel.config.{Endpoint, PairDef, ConfigStore}
import net.lshift.diffa.kernel.events.{UpstreamPairChangeEvent, VersionID}

class PairActorTest {

  val pairKey = "some-pairing"
  val policyName = ""
  val pairDef = new PairDef()
  pairDef.pairKey = pairKey
  pairDef.versionPolicyName = policyName

  val upstream = Endpoint("up","up",true)
  val downstream = Endpoint("down","down",true)

  val p = new net.lshift.diffa.kernel.config.Pair()
  p.upstream = upstream
  p.downstream = downstream

  val dates = DateConstraint(new DateTime().minusHours(1), new DateTime().plusHours(1))

  val us = createStrictMock("upstreamParticipant", classOf[UpstreamParticipant])
  val ds = createStrictMock("downstreamParticipant", classOf[DownstreamParticipant])

  val participantFactory = org.easymock.classextension.EasyMock.createStrictMock("participantFactory", classOf[ParticipantFactory])
  expect(participantFactory.createUpstreamParticipant(upstream.url)).andReturn(us)
  expect(participantFactory.createDownstreamParticipant(downstream.url)).andReturn(ds)
  org.easymock.classextension.EasyMock.replay(participantFactory)

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
  expect(configStore.getPair(pairKey)).andReturn(p)
  replay(configStore)

  val versionPolicyManager = org.easymock.classextension.EasyMock.createStrictMock("versionPolicyManager", classOf[VersionPolicyManager])
  val versionPolicy = createStrictMock("versionPolicy", classOf[VersionPolicy])
  expect(versionPolicyManager.lookupPolicy(policyName)).andReturn(Some(versionPolicy))
  org.easymock.classextension.EasyMock.replay(versionPolicyManager)

  val supervisor = new PairActorSupervisor(versionPolicyManager, configStore, participantFactory)

  val client = new DefaultChangeEventClient()

  val listener = createStrictMock("differencingListener", classOf[DifferencingListener])

  @Before
  def start = supervisor.startActor(pairDef)

  @After
  def stop = supervisor.stopActor(pairKey)

  @Test
  def runDifference = {
    val id = VersionID(pairKey, "foo")

    expect(versionPolicy.difference(pairKey, dates, us, ds, listener)).andReturn(true)
    replay(versionPolicy)

    assertTrue(client.syncPair(pairKey, dates, listener))
  }

  //@Test
  def propagateChange = {
    val id = VersionID(pairKey, "foo")
    val date = new DateTime()
    val lastUpdate = new DateTime()
    val vsn = "foobar"
    val event = UpstreamPairChangeEvent(id, date, lastUpdate, vsn)
    
    expect(versionPolicy.onChange(event))
    replay(versionPolicy)

    client.propagateChangeEvent(event)

    // propagateChangeEvent is an aysnc call, so yield the test thread to allow the actor to invoke the policy
    Thread.sleep(1000)

    verify(versionPolicy)
  }
}