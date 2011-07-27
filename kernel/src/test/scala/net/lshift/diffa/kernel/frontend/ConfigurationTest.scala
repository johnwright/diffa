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
import net.lshift.diffa.kernel.scheduler.ScanScheduler
import net.lshift.diffa.kernel.actors.ActivePairManager
import net.lshift.diffa.kernel.differencing.{SessionManager, VersionCorrelationStoreFactory}
import org.junit.{Test, Before}
import org.junit.Assert._
import net.lshift.diffa.kernel.participants.EndpointLifecycleListener
import net.lshift.diffa.kernel.config._
import scala.collection.JavaConversions._
import org.easymock.IArgumentMatcher
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}
import net.lshift.diffa.kernel.frontend.DiffaConfig._
import collection.mutable.HashSet
import scala.collection.JavaConversions._

/**
 * Test cases for the Configuration frontend.
 */
class ConfigurationTest {
  private val matchingManager = createMock("matchingManager", classOf[MatchingManager])
  private val versionCorrelationStoreFactory = createMock("versionCorrelationStoreFactory", classOf[VersionCorrelationStoreFactory])
  private val pairManager = createMock("pairManager", classOf[ActivePairManager])
  private val sessionManager = createMock("sessionManager", classOf[SessionManager])
  private val endpointListener = createMock("endpointListener", classOf[EndpointLifecycleListener])
  private val scanScheduler = createMock("endpointListener", classOf[ScanScheduler])

  private val configuration = new Configuration(HibernateDomainConfigStoreTest.domainConfigStore,
                                                matchingManager,
                                                versionCorrelationStoreFactory,
                                                pairManager,
                                                sessionManager,
                                                endpointListener,
                                                scanScheduler)

  @Before
  def clearConfig {
    HibernateDomainConfigStoreTest.clearAllConfig
  }

  @Test
  def shouldApplyEmptyConfigToEmptySystem {
    replayAll

    configuration.applyConfiguration("domain", DiffaConfig())
    assertEquals(DiffaConfig(), configuration.retrieveConfiguration("domain"))
  }

  @Test
  def shouldGenerateExceptionWhenInvalidConfigurationIsApplied() {
    val ep1 = Endpoint(name = "upstream1", scanUrl = "http://localhost:1234/scan", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml")
    val ep2 = Endpoint(name = "downstream1", scanUrl = "http://localhost:5432/scan", contentType = "application/json")
    val config = new DiffaConfig(
      endpoints = Set(ep1, ep2),
      pairs = Set(
        PairDef("ab", "domain", "same", 5, "upstream1", "downstream1", "bad-cron-spec"))
    )

    try {
      configuration.applyConfiguration("domain", config)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case ex:ConfigValidationException =>
        assertEquals("config/pair[key=ab]: Schedule 'bad-cron-spec' is not a valid: Illegal characters for this position: 'BAD'", ex.getMessage)
    }
  }

  @Test
  def shouldApplyConfigurationToEmptySystem() {
    val ep1 = Endpoint(name = "upstream1", scanUrl = "http://localhost:1234", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml",
          categories = Map(
            "a" -> new RangeCategoryDescriptor("datetime", "2009", "2010"),
            "b" -> new SetCategoryDescriptor(Set("a", "b", "c"))))
    val ep2 = Endpoint(name = "downstream1", scanUrl = "http://localhost:5432/scan", contentType = "application/json",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))
    val config = new DiffaConfig(
      properties = Map("diffa.host" -> "localhost:1234", "a" -> "b"),
      users = Set(User("abc", HashSet(Domain(name = "domain")), "a@example.com"),
                  User("def", HashSet(Domain(name = "domain")), "b@example.com")),
      endpoints = Set(ep1, ep2),
      pairs = Set(
        PairDef("ab", "domain", "same", 5, "upstream1", "downstream1", "0 * * * * ?"),
        PairDef("ac", "domain", "same", 5, "upstream1", "downstream1", "0 * * * * ?")),
      repairActions = Set(RepairAction("Resend Sauce", "resend", "pair", DiffaPair(key = "ab", domain = Domain(name="domain"))))
    )

    expect(endpointListener.onEndpointAvailable(ep1)).once
    expect(endpointListener.onEndpointAvailable(ep2)).once
    expect(pairManager.startActor(pairInstance("ab"))).once
    expect(matchingManager.onUpdatePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(scanScheduler.onUpdatePair("domain","ab")).once
    expect(sessionManager.onUpdatePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.startActor(pairInstance("ac"))).once
    expect(matchingManager.onUpdatePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(scanScheduler.onUpdatePair("domain","ac")).once
    expect(sessionManager.onUpdatePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    replayAll

    configuration.applyConfiguration("domain", config)
    assertEquals(config, configuration.retrieveConfiguration("domain"))
    verifyAll
  }

  @Test
  def shouldUpdateConfigurationInNonEmptySystem() {
    // Apply the configuration used in the empty state test
    shouldApplyConfigurationToEmptySystem
    resetAll

      // upstream1 is kept but changed
    val ep1 = Endpoint(name = "upstream1", scanUrl = "http://localhost:6543/scan", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml",
          categories = Map(
            "a" -> new RangeCategoryDescriptor("datetime", "2009", "2010"),
            "b" -> new SetCategoryDescriptor(Set("a", "b", "c"))))
      // downstream1 is gone, downstream2 is added
    val ep2 = Endpoint(name = "downstream2", scanUrl = "http://localhost:54321/scan", contentType = "application/json",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))
    val config = new DiffaConfig(
        // diffa.host is changed, a -> b is gone, c -> d is added
      properties = Map("diffa.host" -> "localhost:2345", "c" -> "d"),
        // abc is changed, def is gone, ghi is added
      users = Set(User("abc", HashSet(Domain(name = "domain")), "a2@example.com"),
                  User("ghi", HashSet(Domain(name = "domain")), "c@example.com")),
      endpoints = Set(ep1, ep2),
        // gaa is gone, gcc is created, gbb is the same
      pairs = Set(
          // ab has moved from gaa to gcc
        PairDef("ab", "domain", "same", 5, "upstream1", "downstream2", "0 * * * * ?"),
          // ac is gone
        PairDef("ad", "domain", "same", 5, "upstream1", "downstream2")),
      // name of repair action is changed
      repairActions = Set(RepairAction("Resend Source", "resend", "pair", DiffaPair(key = "ab", domain = Domain(name="domain"))))
    )

    expect(pairManager.stopActor(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.startActor(pairInstance("ab"))).once
    expect(matchingManager.onUpdatePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(scanScheduler.onUpdatePair("domain","ab")).once
    expect(sessionManager.onUpdatePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.stopActor(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(matchingManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(scanScheduler.onDeletePair("domain","ac")).once
    expect(versionCorrelationStoreFactory.remove(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(sessionManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(pairManager.startActor(pairInstance("ad"))).once
    expect(matchingManager.onUpdatePair(DiffaPair(key = "ad", domain = Domain(name="domain")))).once
    expect(scanScheduler.onUpdatePair("domain","ad")).once
    expect(sessionManager.onUpdatePair(DiffaPair(key = "ad", domain = Domain(name="domain")))).once

    expect(endpointListener.onEndpointRemoved("downstream1")).once
    expect(endpointListener.onEndpointAvailable(ep1)).once
    expect(endpointListener.onEndpointAvailable(ep2)).once
    replayAll

    configuration.applyConfiguration("domain",config)
    val newConfig = configuration.retrieveConfiguration("domain")
    assertEquals(config, newConfig)

    // check that the action was updated
    assertEquals(Set(RepairAction("Resend Source", "resend", "pair", DiffaPair(key = "ab", domain = Domain(name="domain")))), newConfig.repairActions)
    verifyAll
  }

  @Test
  def shouldBlankConfigurationOfNonEmptySystem() {
    // Apply the configuration used in the empty state test
    shouldApplyConfigurationToEmptySystem
    resetAll

    expect(pairManager.stopActor(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.stopActor(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(matchingManager.onDeletePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(matchingManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(scanScheduler.onDeletePair("domain","ab")).once
    expect(scanScheduler.onDeletePair("domain","ac")).once
    expect(versionCorrelationStoreFactory.remove(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(versionCorrelationStoreFactory.remove(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(sessionManager.onDeletePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(sessionManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(endpointListener.onEndpointRemoved("upstream1")).once
    expect(endpointListener.onEndpointRemoved("downstream1")).once
    replayAll

    configuration.applyConfiguration("domain",DiffaConfig())
    assertEquals(DiffaConfig(), configuration.retrieveConfiguration("domain"))
    verifyAll
  }

  private def replayAll = replay(matchingManager, pairManager, sessionManager, endpointListener, scanScheduler)
  private def verifyAll = verify(matchingManager, pairManager, sessionManager, endpointListener, scanScheduler)
  private def resetAll = reset(matchingManager, pairManager, sessionManager, endpointListener, scanScheduler)
  private def pairInstance(key:String):Pair = {
    reportMatcher(new IArgumentMatcher {
      def appendTo(buffer: StringBuffer) = buffer.append("pair with key " + key)
      def matches(argument: AnyRef) = argument.asInstanceOf[Pair].key == key
    })
    null
  }
}