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
import org.easymock.IAnswer
import concurrent.{TIMEOUT, MailBox}

class PairActorTest {

  val pairKey = "some-pairing"
  val policyName = ""
  val upstream = Endpoint("up","up","application/json", null, null, true)
  val downstream = Endpoint("down","down","application/json", null, null, true)

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

  val writer = createMock("writer", classOf[VersionCorrelationWriter])

  val store = createMock("versionCorrelationStore", classOf[VersionCorrelationStore])
  expect(store.openWriter()).andReturn(writer).anyTimes
  replay(store)

  val stores = createStrictMock("versionCorrelationStoreFactory", classOf[VersionCorrelationStoreFactory])
  expect(stores.apply(pairKey)).andReturn(store)
  replay(stores)

  val supervisor = new PairActorSupervisor(versionPolicyManager, configStore, participantFactory, stores, 50, 100)
  supervisor.onAgentAssemblyCompleted
  supervisor.onAgentConfigurationActivated

  verify(configStore)

  val diffListener = createStrictMock("differencingListener", classOf[DifferencingListener])
  val syncListener = createStrictMock("syncListener", classOf[PairSyncListener])

  @After
  def stop = supervisor.stopActor(pairKey)

  @Test
  def runDifference = {
    val id = VersionID(pairKey, "foo")
    val monitor = new Object

    expect(writer.flush()).atLeastOnce
    replay(writer)
    syncListener.pairSyncStateChanged(pairKey, PairSyncState.SYNCHRONISING); expectLastCall
    expect(versionPolicy.difference(pairKey, writer, us, ds, diffListener)).andReturn(true)
    syncListener.pairSyncStateChanged(pairKey, PairSyncState.UP_TO_DATE); expectLastCall[Unit].andAnswer(new IAnswer[Unit] {
      def answer = { monitor.synchronized { monitor.notifyAll } }
    })
    replay(versionPolicy, syncListener)

    supervisor.startActor(pair)
    supervisor.syncPair(pairKey, diffListener, syncListener)
    monitor.synchronized {
      monitor.wait(1000)
    }
    verify(versionPolicy, syncListener)
  }

  @Test
  def reportDifferenceFailure = {
    val id = VersionID(pairKey, "foo")
    val monitor = new Object

    expect(writer.flush()).atLeastOnce
    replay(writer)
    syncListener.pairSyncStateChanged(pairKey, PairSyncState.SYNCHRONISING); expectLastCall
    expect(versionPolicy.difference(pairKey, writer, us, ds, diffListener)).andThrow(new RuntimeException("Foo!"))
    syncListener.pairSyncStateChanged(pairKey, PairSyncState.FAILED); expectLastCall[Unit].andAnswer(new IAnswer[Unit] {
      def answer { monitor.synchronized { monitor.notifyAll } }
    })
    replay(versionPolicy, syncListener)

    supervisor.startActor(pair)
    supervisor.syncPair(pairKey, diffListener, syncListener)
    monitor.synchronized {
      monitor.wait(1000)
    }
    verify(versionPolicy, syncListener)
    verify(syncListener)
  }

  @Test
  def propagateChange = {
    val id = VersionID(pairKey, "foo")
    val lastUpdate = new DateTime()
    val vsn = "foobar"
    val event = UpstreamPairChangeEvent(id, Seq(), lastUpdate, vsn)
    val monitor = new Object

    expect(writer.flush()).atLeastOnce
    replay(writer)
    expect(versionPolicy.onChange(writer, event)).andAnswer(new IAnswer[Unit] {
      def answer = {
        monitor.synchronized {
          monitor.notifyAll
        }
      }
    })
    replay(versionPolicy)

    supervisor.startActor(pair)
    supervisor.propagateChangeEvent(event)

    // propagateChangeEvent is an aysnc call, so yield the test thread to allow the actor to invoke the policy
    monitor.synchronized {
      monitor.wait(1000)
    }

    verify(versionPolicy)
  }

  @Test
  def scheduledFlush {
    val mailbox = new MailBox
    
    expect(writer.flush()).andStubAnswer(new IAnswer[Unit] {
      def answer = {
        mailbox.send(new Object)
        null
      }
    })
    replay(writer)

    supervisor.startActor(pair)
    mailbox.receiveWithin(1000) { case TIMEOUT => fail("Flush not called"); case _ => () }
    mailbox.receiveWithin(1000) { case TIMEOUT => fail("Flush not called"); case _ => () }
  }
}