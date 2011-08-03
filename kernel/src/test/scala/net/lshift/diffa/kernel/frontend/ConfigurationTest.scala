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
import system.{HibernateSystemConfigStore, SystemConfigStore}
import net.lshift.diffa.kernel.frontend.FrontendConversions._

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

  // TODO This is a strange mixture of mock and real objects
  private val domainConfigStore: DomainConfigStore = HibernateDomainConfigStoreTest.domainConfigStore
  private val sf = HibernateDomainConfigStoreTest.domainConfigStore.sessionFactory
  private val systemConfigStore = new HibernateSystemConfigStore(domainConfigStore, sf)

  private val configuration = new Configuration(domainConfigStore,
                                                systemConfigStore,
                                                matchingManager,
                                                versionCorrelationStoreFactory,
                                                pairManager,
                                                sessionManager,
                                                endpointListener,
                                                scanScheduler)

  val domainName = "domain"
  val domain = Domain(name = domainName)



  @Before
  def clearConfig = {
    systemConfigStore.deleteDomain(domainName)
    systemConfigStore.createOrUpdateDomain(domain)
  }

  @Test
  def shouldApplyEmptyConfigToEmptySystem {
    replayAll

    configuration.applyConfiguration("domain", DiffaConfig())
    assertEquals(DiffaConfig(), configuration.retrieveConfiguration("domain"))
  }

  @Test
  def shouldGenerateExceptionWhenInvalidConfigurationIsApplied() {
    val e1 = EndpointDef(name = "upstream1", scanUrl = "http://localhost:1234/scan", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml")
    val e2 = EndpointDef(name = "downstream1", scanUrl = "http://localhost:5432/scan", contentType = "application/json")
    val conf = new DiffaConfig(
      endpoints = Set(e1, e2),
      pairs = Set(
        PairDef("ab", "same", 5, "upstream1", "downstream1", "bad-cron-spec"))
    )

    try {
      configuration.applyConfiguration("domain", conf)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case ex:ConfigValidationException =>
        assertEquals("config/pair[key=ab]: Schedule 'bad-cron-spec' is not a valid: Illegal characters for this position: 'BAD'", ex.getMessage)
    }
  }

  @Test
  def shouldApplyConfigurationToEmptySystem() {

    val ep1 = EndpointDef(name = "upstream1", scanUrl = "http://localhost:1234", contentType = "application/json",
                inboundUrl = "http://inbound", inboundContentType = "application/xml",
                categories = Map(
                  "a" -> new RangeCategoryDescriptor("datetime", "2009", "2010"),
                  "b" -> new SetCategoryDescriptor(Set("a", "b", "c"))))
    val ep2 = EndpointDef(name = "downstream1", scanUrl = "http://localhost:5432/scan", contentType = "application/json",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))
    val config = new DiffaConfig(
      properties = Map("diffa.host" -> "localhost:1234", "a" -> "b"),
      users = Set(User("abc", "a@example.com"),
                  User("def", "b@example.com")),
      endpoints = Set(ep1, ep2),
      pairs = Set(
        PairDef("ab", "same", 5, "upstream1", "downstream1", "0 * * * * ?"),
        PairDef("ac", "same", 5, "upstream1", "downstream1", "0 * * * * ?")),
      repairActions = Set(RepairActionDef("Resend Sauce", "resend", "pair", "ab")),
      escalations = Set(EscalationDef("Resend Missing", "ab", "Resend Sauce", "repair", "downstream-missing", "scan"))
    )

    val ab = DiffaPair(key = "ab", domain = Domain(name="domain"), matchingTimeout = 5,
                       versionPolicyName = "same", scanCronSpec = "0 * * * * ?", upstream = fromEndpointDef(domain, ep1), downstream = fromEndpointDef(domain, ep2))

    val ac = DiffaPair(key = "ac", domain = Domain(name="domain"), matchingTimeout = 5,
                       versionPolicyName = "same", scanCronSpec = "0 * * * * ?", upstream = fromEndpointDef(domain, ep1), downstream = fromEndpointDef(domain, ep2))


    expect(endpointListener.onEndpointAvailable(fromEndpointDef(domain, ep1))).once
    expect(endpointListener.onEndpointAvailable(fromEndpointDef(domain, ep2))).once
    expect(pairManager.startActor(pairInstance("ab"))).once
    expect(matchingManager.onUpdatePair(ab)).once
    expect(scanScheduler.onUpdatePair(ab)).once
    expect(sessionManager.onUpdatePair(ab)).once
    expect(pairManager.startActor(pairInstance("ac"))).once
    expect(matchingManager.onUpdatePair(ac)).once
    expect(scanScheduler.onUpdatePair(ac)).once
    expect(sessionManager.onUpdatePair(ac)).once
    replayAll

    configuration.applyConfiguration("domain", config)
    assertEquals(config, configuration.retrieveConfiguration("domain"))
    verifyAll
  }

  // TODO comment back in
  //@Test
  def shouldUpdateConfigurationInNonEmptySystem() {
    // Apply the configuration used in the empty state test
    shouldApplyConfigurationToEmptySystem
    resetAll

      // upstream1 is kept but changed
    val ep1 = EndpointDef(name = "upstream1", scanUrl = "http://localhost:6543/scan", contentType = "application/json",
          inboundUrl = "http://inbound", inboundContentType = "application/xml",
          categories = Map(
            "a" -> new RangeCategoryDescriptor("datetime", "2009", "2010"),
            "b" -> new SetCategoryDescriptor(Set("a", "b", "c"))))
      // downstream1 is gone, downstream2 is added
    val ep2 = EndpointDef(name = "downstream2", scanUrl = "http://localhost:54321/scan", contentType = "application/json",
          categories = Map(
            "c" -> new PrefixCategoryDescriptor(1, 5, 1),
            "d" -> new PrefixCategoryDescriptor(1, 6, 1)
          ))
    val config = new DiffaConfig(
        // diffa.host is changed, a -> b is gone, c -> d is added
      properties = Map("diffa.host" -> "localhost:2345", "c" -> "d"),
        // abc is changed, def is gone, ghi is added
      users = Set(User("abc", "a2@example.com"),
                  User("ghi", "c@example.com")),
      endpoints = Set(ep1, ep2),
        // gaa is gone, gcc is created, gbb is the same
      pairs = Set(
          // ab has moved from gaa to gcc
        PairDef("ab", "same", 5, "upstream1", "downstream2", "0 * * * * ?"),
          // ac is gone
        PairDef("ad", "same", 5, "upstream1", "downstream2")),
      // name of repair action is changed
      repairActions = Set(RepairActionDef("Resend Source", "resend", "pair", "ab")),
      escalations = Set(EscalationDef("Resend Another Missing", "ab", "Resend Source", "repair", "downstream-missing", "scan"))
    )

    val ab = DiffaPair(key = "ab", domain = Domain(name="domain"), matchingTimeout = 5,
                          versionPolicyName = "same", scanCronSpec = "0 * * * * ?", upstream = fromEndpointDef(domain, ep1), downstream = fromEndpointDef(domain, ep2))

    val ac = DiffaPair(key = "ac", domain = Domain(name="domain"), matchingTimeout = 5,
                          versionPolicyName = "same", scanCronSpec = "0 * * * * ?", upstream = fromEndpointDef(domain, ep1), downstream = fromEndpointDef(domain, ep2))

    val ad = DiffaPair(key = "ac", domain = Domain(name="domain"), matchingTimeout = 5,
                          versionPolicyName = "same", upstream = fromEndpointDef(domain, ep1), downstream = fromEndpointDef(domain, ep2))

    expect(pairManager.stopActor(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.startActor(pairInstance("ab"))).once
    expect(matchingManager.onUpdatePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(scanScheduler.onUpdatePair(ab)).once
    expect(sessionManager.onUpdatePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.stopActor(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(matchingManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(scanScheduler.onDeletePair(ac)).once
    expect(versionCorrelationStoreFactory.remove(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(sessionManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(pairManager.startActor(pairInstance("ad"))).once
    expect(matchingManager.onUpdatePair(DiffaPair(key = "ad", domain = Domain(name="domain")))).once
    expect(scanScheduler.onUpdatePair(ad)).once
    expect(sessionManager.onUpdatePair(DiffaPair(key = "ad", domain = Domain(name="domain")))).once

    expect(endpointListener.onEndpointRemoved("downstream1")).once
    expect(endpointListener.onEndpointAvailable(fromEndpointDef(domain, ep1))).once
    expect(endpointListener.onEndpointAvailable(fromEndpointDef(domain, ep2))).once
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

    val ab = DiffaPair(key = "ab", domain = Domain(name="domain"))
    val ac = DiffaPair(key = "ac", domain = Domain(name="domain"))

    expect(pairManager.stopActor(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(pairManager.stopActor(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(matchingManager.onDeletePair(DiffaPair(key = "ab", domain = Domain(name="domain")))).once
    expect(matchingManager.onDeletePair(DiffaPair(key = "ac", domain = Domain(name="domain")))).once
    expect(scanScheduler.onDeletePair(ab)).once
    expect(scanScheduler.onDeletePair(ac)).once
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