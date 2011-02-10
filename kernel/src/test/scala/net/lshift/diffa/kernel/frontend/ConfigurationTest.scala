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
package net.lshift.diffa.kernel.frontend

import org.easymock.EasyMock._
import net.lshift.diffa.kernel.matching.MatchingManager
import net.lshift.diffa.kernel.actors.ActivePairManager
import net.lshift.diffa.kernel.differencing.SessionManager
import org.junit.{Test, Before}
import org.junit.Assert._
import net.lshift.diffa.kernel.participants.EndpointLifecycleListener
import net.lshift.diffa.kernel.config._
import scala.collection.JavaConversions._
import org.easymock.IArgumentMatcher

/**
 * Test cases for the Configuration frontend.
 */
class ConfigurationTest {
  private val matchingManager = createMock("matchingManager", classOf[MatchingManager])
  private val pairManager = createMock("pairManager", classOf[ActivePairManager])
  private val sessionManager = createMock("sessionManager", classOf[SessionManager])
  private val endpointListener = createMock("endpointListener", classOf[EndpointLifecycleListener])

  private val configuration = new Configuration(HibernateConfigStoreTest.configStore, matchingManager, pairManager, sessionManager, endpointListener)

  @Before
  def clearConfig {
    HibernateConfigStoreTest.clearAllConfig
  }

  @Test
  def shouldApplyEmptyConfigToEmptySystem {
    replayAll

    configuration.applyConfiguration(DiffaConfig())
    assertEquals(DiffaConfig(), configuration.retrieveConfiguration)
  }

  @Test
  def shouldApplyConfigurationToEmptySystem() {
    val ep1 = Endpoint(name = "upstream1", url = "http://localhost:1234", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml",
          categories = Map(
            "a" -> new RangeCategoryDescriptor("date", "2009", "2010"),
            "b" -> new SetCategoryDescriptor(Set("a", "b", "c"))))
    val ep2 = Endpoint(name = "downstream1", url = "http://localhost:5432", contentType = "application/json",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))
    val config = new DiffaConfig(
      properties = Map("diffa.host" -> "localhost:1234", "a" -> "b"),
      users = Set(User("abc", "a@example.com"), User("def", "b@example.com")),
      endpoints = Set(ep1, ep2),
      groups = Set(PairGroup("gaa"), PairGroup("gbb")),
      pairs = Set(
        PairDef("ab", "same", 5, "upstream1", "downstream1", "gaa"),
        PairDef("ac", "same", 5, "upstream1", "downstream1", "gbb"))
    )

    expect(endpointListener.onEndpointAvailable(ep1)).once
    expect(endpointListener.onEndpointAvailable(ep2)).once
    expect(pairManager.startActor(pairInstance("ab"))).once
    expect(matchingManager.onUpdatePair("ab")).once
    expect(sessionManager.onUpdatePair("ab")).once
    expect(pairManager.startActor(pairInstance("ac"))).once
    expect(matchingManager.onUpdatePair("ac")).once
    expect(sessionManager.onUpdatePair("ac")).once
    replayAll

    configuration.applyConfiguration(config)
    assertEquals(config, configuration.retrieveConfiguration)
    verifyAll
  }

  @Test
  def shouldUpdateConfigurationInNonEmptySystem() {
    // Apply the configuration used in the empty state test
    shouldApplyConfigurationToEmptySystem
    resetAll

      // upstream1 is kept but changed
    val ep1 = Endpoint(name = "upstream1", url = "http://localhost:6543", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml",
          categories = Map(
            "a" -> new RangeCategoryDescriptor("date", "2009", "2010"),
            "b" -> new SetCategoryDescriptor(Set("a", "b", "c"))))
      // downstream1 is gone, downstream2 is added
    val ep2 = Endpoint(name = "downstream2", url = "http://localhost:54321", contentType = "application/json",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))
    val config = new DiffaConfig(
        // diffa.host is changed, a -> b is gone, c -> d is added
      properties = Map("diffa.host" -> "localhost:2345", "c" -> "d"),
        // abc is changed, def is gone, ghi is added
      users = Set(User("abc", "a2@example.com"), User("ghi", "c@example.com")),
      endpoints = Set(ep1, ep2),
        // gaa is gone, gcc is created, gbb is the same
      groups = Set(PairGroup("gcc"), PairGroup("gbb")),
      pairs = Set(
          // ab has moved from gaa to gcc
        PairDef("ab", "same", 5, "upstream1", "downstream2", "gcc"),
          // ac is gone
        PairDef("ad", "same", 5, "upstream1", "downstream2", "gbb"))
    )

    expect(pairManager.stopActor("ab")).once
    expect(pairManager.startActor(pairInstance("ab"))).once
    expect(matchingManager.onUpdatePair("ab")).once
    expect(sessionManager.onUpdatePair("ab")).once
    expect(pairManager.stopActor("ac")).once
    expect(matchingManager.onDeletePair("ac")).once
//    expect(sessionManager.onDeletePair("ac")).once
    expect(pairManager.startActor(pairInstance("ad"))).once
    expect(matchingManager.onUpdatePair("ad")).once
    expect(sessionManager.onUpdatePair("ad")).once

    expect(endpointListener.onEndpointRemoved("downstream1")).once
    expect(endpointListener.onEndpointAvailable(ep1)).once
    expect(endpointListener.onEndpointAvailable(ep2)).once
    replayAll

    configuration.applyConfiguration(config)
    assertEquals(config, configuration.retrieveConfiguration)
    verifyAll
  }

  private def replayAll = replay(matchingManager, pairManager, sessionManager, endpointListener)
  private def verifyAll = verify(matchingManager, pairManager, sessionManager, endpointListener)
  private def resetAll = reset(matchingManager, pairManager, sessionManager, endpointListener)
  private def pairInstance(key:String):Pair = {
    reportMatcher(new IArgumentMatcher {
      def appendTo(buffer: StringBuffer) = buffer.append("pair with key " + key)
      def matches(argument: AnyRef) = argument.asInstanceOf[Pair].key == key
    })
    null
  }
}