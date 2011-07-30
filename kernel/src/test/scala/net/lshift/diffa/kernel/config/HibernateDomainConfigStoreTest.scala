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

package net.lshift.diffa.kernel.config

import org.junit.Assert._
import org.hibernate.cfg.Configuration
import net.lshift.diffa.kernel.util.MissingObjectException
import org.hibernate.exception.ConstraintViolationException
import org.junit.{Test, Before}
import scala.collection.Map
import org.joda.time.DateTime
import collection.mutable.HashSet
import scala.collection.JavaConversions._
import system.{HibernateSystemConfigStore, SystemConfigStore}
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}

class HibernateDomainConfigStoreTest {

  private val domainConfigStore: DomainConfigStore = HibernateDomainConfigStoreTest.domainConfigStore
  private val systemConfigStore: SystemConfigStore =
    new HibernateSystemConfigStore(domainConfigStore, HibernateDomainConfigStoreTest.sessionFactory)

  val dateCategoryName = "bizDate"
  val dateCategoryLower = new DateTime(1982,4,5,12,13,9,0).toString()
  val dateCategoryUpper = new DateTime(1982,4,6,12,13,9,0).toString()
  val dateRangeCategoriesMap =
    Map(dateCategoryName ->  new RangeCategoryDescriptor("datetime",dateCategoryLower,dateCategoryUpper))

  val setCategoryValues = Set("a","b","c")
  val setCategoriesMap = Map(dateCategoryName ->  new SetCategoryDescriptor(setCategoryValues))

  val intCategoryName = "someInt"
  val stringCategoryName = "someString"

  val intCategoryType = "int"
  val intRangeCategoriesMap = Map(intCategoryName ->  new RangeCategoryDescriptor(intCategoryType))

  val stringPrefixCategoriesMap = Map(stringCategoryName -> new PrefixCategoryDescriptor(1, 3, 1))

  val domainName = "domain"
  val domain = new Domain(domainName)

  val upstream1 = new Endpoint(name = "TEST_UPSTREAM", scanUrl = "testScanUrl1", contentType = "application/json",
                               categories = dateRangeCategoriesMap, domain = domain)
  val upstream2 = new Endpoint(name = "TEST_UPSTREAM_ALT", scanUrl = "testScanUrl2",
                               contentRetrievalUrl = "contentRetrieveUrl1", contentType = "application/json",
                               categories = setCategoriesMap, domain = domain)

  val downstream1 = new Endpoint(name = "TEST_DOWNSTREAM", domain = domain, scanUrl = "testScanUrl3",
                                 contentType = "application/json", categories = intRangeCategoriesMap)
  val downstream2 = new Endpoint(name = "TEST_DOWNSTREAM_ALT", domain = domain, scanUrl = "testScanUrl4",
                                 versionGenerationUrl = "generateVersionUrl1", contentType = "application/json",
                                 categories = stringPrefixCategoriesMap)

  val versionPolicyName1 = "TEST_VPNAME"
  val matchingTimeout = 120
  val versionPolicyName2 = "TEST_VPNAME_ALT"
  val pairKey = "TEST_PAIR"
  val pairDef = new PairDef(pairKey, domainName, versionPolicyName1, matchingTimeout, upstream1.name,
    downstream1.name)

  val pair = DiffaPair(key = pairKey, domain = domain)

  val repairAction = new RepairAction(name="REPAIR_ACTION_NAME",
                                      scope=RepairAction.ENTITY_SCOPE,
                                      url="resend", pair=pair)

  val escalation = Escalation(name="esc", action = "test_action", pair = pair,
                              event = EscalationEvent.UPSTREAM_MISSING,
                              actionType = EscalationActionType.REPAIR,
                              origin = EscalationOrigin.SCAN)

  val configKey = "foo"
  val configValue = "bar"

  val upstreamRenamed = "TEST_UPSTREAM_RENAMED"
  val pairRenamed = "TEST_PAIR_RENAMED"

  val TEST_USER = User("foo", HashSet(Domain(name = "domain")), "foo@bar.com")

  def declareAll() {
    systemConfigStore.createOrUpdateDomain(domain)
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream2)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream2)
    domainConfigStore.createOrUpdatePair(domainName, pairDef)
    domainConfigStore.createOrUpdateRepairAction(domainName, repairAction)
    domainConfigStore.createOrUpdateEscalation(domainName, escalation)
    domainConfigStore.setConfigOption(domainName, configKey, configValue)
  }

  @Before
  def setUp = HibernateDomainConfigStoreTest.clearAllConfig

  def exists (e:Endpoint, count:Int, offset:Int) : Unit = {
    val endpoints = domainConfigStore.listEndpoints(domainName)
    assertEquals(count, endpoints.length)
    assertEquals(e.name, endpoints(offset).name)
    assertEquals(e.inboundUrl, endpoints(offset).inboundUrl)
    assertEquals(e.inboundContentType, endpoints(offset).inboundContentType)
    assertEquals(e.scanUrl, endpoints(offset).scanUrl)
    assertEquals(e.contentRetrievalUrl, endpoints(offset).contentRetrievalUrl)
    assertEquals(e.versionGenerationUrl, endpoints(offset).versionGenerationUrl)
  }

  def exists (e:Endpoint, count:Int) : Unit = exists(e, count, count - 1)

  @Test
  def domainShouldBeDeletable = {
    declareAll()

    exists(upstream1, 4, 0)
    exists(upstream2, 4, 1)
    exists(downstream1, 4, 2)
    exists(downstream2, 4, 3)

    assertFalse(domainConfigStore.listPairs(domainName).isEmpty)
    assertFalse(domainConfigStore.allConfigOptions(domainName).isEmpty)
    assertFalse(domainConfigStore.listRepairActions(domainName).isEmpty)
    assertFalse(domainConfigStore.listEscalations(domainName).isEmpty)

    systemConfigStore.deleteDomain(domainName)

    assertTrue(domainConfigStore.listEndpoints(domainName).isEmpty)
    assertTrue(domainConfigStore.listPairs(domainName).isEmpty)
    assertTrue(domainConfigStore.allConfigOptions(domainName).isEmpty)
    assertTrue(domainConfigStore.listRepairActions(domainName).isEmpty)
    assertTrue(domainConfigStore.listEscalations(domainName).isEmpty)

    assertTrue(systemConfigStore.listDomains.filter(_.name == domainName).isEmpty)
  }

  @Test
  def testDeclare: Unit = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)

    // Declare endpoints
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    exists(upstream1, 1)

    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    exists(downstream1, 2)

    // Declare a pair
    domainConfigStore.createOrUpdatePair(domainName, pairDef)

    val retrPair = domainConfigStore.getPair(domainName, pairDef.pairKey)
    assertEquals(pairKey, retrPair.key)
    assertEquals(upstream1.name, retrPair.upstream.name)
    assertEquals(downstream1.name, retrPair.downstream.name)
    assertEquals(versionPolicyName1, retrPair.versionPolicyName)
    assertEquals(matchingTimeout, retrPair.matchingTimeout)

    // Declare a repair action
    domainConfigStore.createOrUpdateRepairAction(domainName, repairAction)
    val retrActions = domainConfigStore.listRepairActionsForPair(domainName, retrPair)
    assertEquals(1, retrActions.length)
    assertEquals(Some(pairKey), retrActions.headOption.map(_.pair.key))
  }

  @Test
  def testPairsAreValidatedBeforeUpdate() {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)
    // Declare endpoints
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    exists(upstream1, 1)

    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    exists(downstream1, 2)

    pairDef.scanCronSpec = "invalid"

    try {
      domainConfigStore.createOrUpdatePair(domainName, pairDef)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case ex:ConfigValidationException =>
        assertEquals("pair[key=TEST_PAIR]: Schedule 'invalid' is not a valid: Illegal characters for this position: 'INV'", ex.getMessage)
    }
  }

  @Test
  def testEndpointsWithSameScanURL {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)

    upstream2.scanUrl = upstream1.scanUrl
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream2)

    exists(upstream1, 2, 0)
    exists(upstream2, 2, 1)
  }


  @Test
  def testUpdateEndpoint: Unit = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)
    // Create endpoint
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    exists(upstream1, 1)

    domainConfigStore.deleteEndpoint(domainName, upstream1.name)
    expectMissingObject("endpoint") {
      domainConfigStore.getEndpoint(domainName, upstream1.name)
    }
        
    // Change its name
    domainConfigStore.createOrUpdateEndpoint(domainName, Endpoint(name = upstreamRenamed, domain = domain,
                                                                  scanUrl = upstream1.scanUrl, contentType = "application/json",
                                                                  inboundUrl = "changes", inboundContentType = "application/json"))

    val retrieved = domainConfigStore.getEndpoint(domainName, upstreamRenamed)
    assertEquals(upstreamRenamed, retrieved.name)
  }

  @Test
  def testUpdatePair: Unit = {
    declareAll

    // Rename, change a few fields and swap endpoints by deleting and creating new
    domainConfigStore.deletePair(domainName, pairKey)
    expectMissingObject("pair") {
      domainConfigStore.getPair(domainName, pairKey)
    }

    domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairRenamed, domainName,  versionPolicyName2, Pair.NO_MATCHING,
      downstream1.name, upstream1.name, "0 0 * * * ?"))
    
    val retrieved = domainConfigStore.getPair(domainName, pairRenamed)
    assertEquals(pairRenamed, retrieved.key)
    assertEquals(downstream1.name, retrieved.upstream.name) // check endpoints are swapped
    assertEquals(upstream1.name, retrieved.downstream.name)
    assertEquals(versionPolicyName2, retrieved.versionPolicyName)
    assertEquals("0 0 * * * ?", retrieved.scanCronSpec)
    assertEquals(Pair.NO_MATCHING, retrieved.matchingTimeout)
  }

  @Test
  def testDeleteEndpointCascade: Unit = {
    declareAll

    assertEquals(upstream1.name, domainConfigStore.getEndpoint(domainName, upstream1.name).name)
    domainConfigStore.deleteEndpoint(domainName, upstream1.name)
    expectMissingObject("endpoint") {
      domainConfigStore.getEndpoint(domainName, upstream1.name)
    }
    expectMissingObject("pair") {
      domainConfigStore.getPair(domainName, pairKey) // delete should cascade
    }
  }

  @Test
  def testDeletePair: Unit = {
    declareAll

    assertEquals(pairKey, domainConfigStore.getPair(domainName, pairKey).key)
    domainConfigStore.deletePair(domainName, pairKey)
    expectMissingObject("pair") {
      domainConfigStore.getPair(domainName, pairKey)
    }
  }

  @Test
  def testDeletePairCascade {
    declareAll()
    assertEquals(Some(repairAction.name), domainConfigStore.listRepairActions(domainName).headOption.map(_.name))
    domainConfigStore.deletePair(domainName, pairKey)
    expectMissingObject("repair action") {
      domainConfigStore.getRepairAction(domainName, repairAction.name, pairKey)
    }
  }

  @Test
  def testDeleteRepairAction {
    declareAll
    assertEquals(Some(repairAction.name), domainConfigStore.listRepairActions(domainName).headOption.map(_.name))

    domainConfigStore.deleteRepairAction(domainName, repairAction.name, pairKey)
    expectMissingObject("repair action") {
      domainConfigStore.getRepairAction(domainName, repairAction.name, pairKey)
    }
  }

  @Test
  def testDeleteMissing: Unit = {
    expectMissingObject("endpoint") {
      domainConfigStore.deleteEndpoint(domainName, "MISSING_ENDPOINT")
    }

    expectMissingObject("pair") {
      domainConfigStore.deletePair(domainName, "MISSING_PAIR")
    }
  }

  @Test
  def testDeclarePairNullConstraints: Unit = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)

      // TODO: We should probably get an exception indicating that the constraint was null, not that the object
      //       we're linking to is missing.
    expectMissingObject("endpoint") {
      domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName1, Pair.NO_MATCHING, null, downstream1.name))
    }
    expectMissingObject("endpoint") {
      domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName1, Pair.NO_MATCHING, upstream1.name, null))
    }
  }

  @Test
  def testRedeclareEndpointSucceeds = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, Endpoint(name = upstream1.name, scanUrl = "DIFFERENT_URL",
                                                                  contentType = "application/json", inboundUrl = "changes",
                                                                  inboundContentType = "application/json",
                                                                  domain = domain))
    assertEquals(1, domainConfigStore.listEndpoints(domainName).length)
    assertEquals("DIFFERENT_URL", domainConfigStore.getEndpoint(domainName, upstream1.name).scanUrl)
  }

  @Test
  def rangeCategory = {
    declareAll
    val pair = domainConfigStore.getPair(domainName, pairKey)
    assertNotNull(pair.upstream.categories)
    assertNotNull(pair.downstream.categories)
    val us_descriptor = pair.upstream.categories(dateCategoryName).asInstanceOf[RangeCategoryDescriptor]
    val ds_descriptor = pair.downstream.categories(intCategoryName).asInstanceOf[RangeCategoryDescriptor]
    assertEquals("datetime", us_descriptor.dataType)
    assertEquals(intCategoryType, ds_descriptor.dataType)
    assertEquals(dateCategoryLower, us_descriptor.lower)
    assertEquals(dateCategoryUpper, us_descriptor.upper)
  }

  @Test
  def setCategory = {
    declareAll
    val endpoint = domainConfigStore.getEndpoint(domainName, upstream2.name)
    assertNotNull(endpoint.categories)
    val descriptor = endpoint.categories(dateCategoryName).asInstanceOf[SetCategoryDescriptor]
    assertEquals(setCategoryValues, descriptor.values.toSet)
  }

  @Test
  def prefixCategory = {
    declareAll
    val endpoint = domainConfigStore.getEndpoint(domainName, downstream2.name)
    assertNotNull(endpoint.categories)
    val descriptor = endpoint.categories(stringCategoryName).asInstanceOf[PrefixCategoryDescriptor]
    assertEquals(1, descriptor.prefixLength)
    assertEquals(3, descriptor.maxLength)
    assertEquals(1, descriptor.step)
  }

  @Test
  def testUser = {
    // declare the domain
    systemConfigStore.deleteDomain(domainName)
    systemConfigStore.createOrUpdateDomain(domain)
    domainConfigStore.createOrUpdateUser(domainName, TEST_USER)
    val result = domainConfigStore.listUsers(domainName)
    assertEquals(1, result.length)
    // Hibernate doesn't seem to able to hydrate the many-to-many eagerly,
    // so let's just verify that the user object is fine for now
    assertEquals(TEST_USER.name, result(0).name)
    val updated = User(TEST_USER.name, HashSet(Domain(name = "domain")), "somethingelse@bar.com")
    domainConfigStore.createOrUpdateUser(domainName, updated)
    val user = domainConfigStore.getUser(domainName, TEST_USER.name)
    // See note above about lazy fetching
    assertEquals(updated.name, user.name)
    domainConfigStore.deleteUser(domainName, TEST_USER.name)
    val users = domainConfigStore.listUsers(domainName)
    assertEquals(0, users.length)    
  }

  @Test
  def testApplyingDefaultConfigOption = {
    assertEquals("defaultVal", domainConfigStore.configOptionOrDefault(domainName,"some.option", "defaultVal"))
  }

  @Test
  def testReturningNoneForConfigOption {
    assertEquals(None, domainConfigStore.maybeConfigOption(domainName, "some.option"))
  }

  @Test
  def testRetrievingConfigOption = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)
    domainConfigStore.setConfigOption(domainName, "some.option2", "storedVal")
    assertEquals("storedVal", domainConfigStore.configOptionOrDefault(domainName, "some.option2", "defaultVal"))
    assertEquals(Some("storedVal"), domainConfigStore.maybeConfigOption(domainName, "some.option2"))
  }

  @Test
  def testUpdatingConfigOption = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)

    domainConfigStore.setConfigOption(domainName, "some.option3", "storedVal")
    domainConfigStore.setConfigOption(domainName, "some.option3", "storedVal2")
    assertEquals("storedVal2", domainConfigStore.configOptionOrDefault(domainName, "some.option3", "defaultVal"))
    assertEquals(Some("storedVal2"), domainConfigStore.maybeConfigOption(domainName, "some.option3"))
  }

  @Test
  def testRemovingConfigOption = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)

    domainConfigStore.setConfigOption(domainName, "some.option3", "storedVal")
    domainConfigStore.clearConfigOption(domainName, "some.option3")
    assertEquals("defaultVal", domainConfigStore.configOptionOrDefault(domainName, "some.option3", "defaultVal"))
    assertEquals(None, domainConfigStore.maybeConfigOption(domainName, "some.option3"))
  }

  @Test
  def testRetrievingAllOptions = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(domain)

    domainConfigStore.setConfigOption(domainName, "some.option3", "storedVal")
    domainConfigStore.setConfigOption(domainName, "some.option4", "storedVal3")
    assertEquals(Map("some.option3" -> "storedVal", "some.option4" -> "storedVal3"), domainConfigStore.allConfigOptions(domainName))
  }

  @Test
  def testRetrievingOptionsIgnoresSystemOptions = {
    // declare the domain
    systemConfigStore.createOrUpdateDomain(Domain.DEFAULT_DOMAIN)
    systemConfigStore.createOrUpdateDomain(domain)

    domainConfigStore.setConfigOption(domainName, "some.option3", "storedVal")
    systemConfigStore.setSystemConfigOption("some.option4", "storedVal3")
    assertEquals(Map("some.option3" -> "storedVal"), domainConfigStore.allConfigOptions(domainName))
  }

  private def expectMissingObject(name:String)(f: => Unit) {
    try {
      f
      fail("Expected MissingObjectException")
    } catch {
      case e:MissingObjectException => assertTrue(
        "Missing Object Exception for wrong object. Expected for " + name + ", got msg: " + e.getMessage,
        e.getMessage.contains(name))
    }
  }

  private def expectConstraintViolation(f: => Unit) {
    try {
      f
      fail("Expected ConstraintViolationException")
    } catch {
      case e:ConstraintViolationException => 
    }
  }
}

object HibernateDomainConfigStoreTest {
  private val config =
      new Configuration().
        addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
        setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
        setProperty("hibernate.connection.url", "jdbc:derby:target/domainConfigStore;create=true").
        setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver").
        setProperty("hibernate.connection.autocommit", "true") // Turn this on to make the tests repeatable,
                                                               // otherwise the preparation step will not get committed

  val sessionFactory = {
    val sf = config.buildSessionFactory
    (new HibernateConfigStorePreparationStep).prepare(sf, config)
    sf
  }

  val domainConfigStore = new HibernateDomainConfigStore(sessionFactory)
  val systemConfigStore = new HibernateSystemConfigStore(domainConfigStore, sessionFactory)

  def clearAllConfig = systemConfigStore.deleteDomain("domain")
}
