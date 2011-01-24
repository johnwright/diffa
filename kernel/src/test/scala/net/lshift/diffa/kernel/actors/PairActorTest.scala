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

package net.lshift.diffa.kernel.actors

import org.junit.{Test, After, Before}
import org.easymock.EasyMock._
import org.junit.Assert._
import net.lshift.diffa.kernel.differencing._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.events.{UpstreamPairChangeEvent, VersionID}
import net.lshift.diffa.kernel.config.{GroupContainer, ConfigStore, Endpoint}
import net.lshift.diffa.kernel.participants._

class PairActorTest {

  val pairKey = "some-pairing"
  val policyName = ""
  val upstream = Endpoint("up","up","application/json", null,true)
  val downstream = Endpoint("down","down","application/json", null,true)

  val pair = new net.lshift.diffa.kernel.config.Pair()
  pair.key = pairKey
  pair.versionPolicyName = policyName
  pair.upstream = upstream
  pair.downstream = downstream

  val us = createStrictMock("upstreamParticipant", classOf[UpstreamParticipant])
  val ds = createStrictMock("downstreamParticipant", classOf[DownstreamParticipant])

  val participantFactory = org.easymock.classextension.EasyMock.createStrictMock("participantFactory", classOf[ParticipantFactory])
  expect(participantFactory.createUpstreamParticipant(upstream)).andReturn(us)
  expect(participantFactory.createDownstreamParticipant(downstream)).andReturn(ds)
  org.easymock.classextension.EasyMock.replay(participantFactory)

  val versionPolicyManager = org.easymock.classextension.EasyMock.createStrictMock("versionPolicyManager", classOf[VersionPolicyManager])
  val versionPolicy = createStrictMock("versionPolicy", classOf[VersionPolicy])
  expect(versionPolicyManager.lookupPolicy(policyName)).andReturn(Some(versionPolicy))
  org.easymock.classextension.EasyMock.replay(versionPolicyManager)

  val configStore = createStrictMock("configStore", classOf[ConfigStore])
  expect(configStore.listGroups).andReturn(Array[GroupContainer]())
  replay(configStore)

  val supervisor = new PairActorSupervisor(versionPolicyManager, configStore, participantFactory)

  verify(configStore)

  val client = new DefaultPairPolicyClient()

  val listener = createStrictMock("differencingListener", classOf[DifferencingListener])

  @Before
  def start = supervisor.startActor(pair)

  @After
  def stop = supervisor.stopActor(pairKey)

  @Test
  def runDifference = {
    val id = VersionID(pairKey, "foo")

    expect(versionPolicy.difference(pairKey, us, ds, listener)).andReturn(true)
    replay(versionPolicy)

    assertTrue(client.syncPair(pairKey, listener))
  }

  @Test
  def propagateChange = {
    val id = VersionID(pairKey, "foo")
    val lastUpdate = new DateTime()
    val vsn = "foobar"
    val event = UpstreamPairChangeEvent(id, Seq(), lastUpdate, vsn)
    
    expect(versionPolicy.onChange(event))
    replay(versionPolicy)

    client.propagateChangeEvent(event)

    // propagateChangeEvent is an aysnc call, so yield the test thread to allow the actor to invoke the policy
    Thread.sleep(1000)

    verify(versionPolicy)
  }
}