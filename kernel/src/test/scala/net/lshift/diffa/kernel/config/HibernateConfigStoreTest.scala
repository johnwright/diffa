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
import org.slf4j.{Logger, LoggerFactory}
import net.lshift.diffa.kernel.util.MissingObjectException
import org.hibernate.exception.ConstraintViolationException
import org.junit.{Test, Before}
import scala.collection.Map
import scala.collection.JavaConversions._
import org.joda.time.DateTime

class HibernateConfigStoreTest {
  private val configStore: ConfigStore = HibernateConfigStoreTest.configStore
  private val log:Logger = LoggerFactory.getLogger(getClass)

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

  val upstream1 = new Endpoint(name = "TEST_UPSTREAM", scanUrl = "testScanUrl1", contentType = "application/json", categories = dateRangeCategoriesMap)
  val upstream2 = new Endpoint(name = "TEST_UPSTREAM_ALT", scanUrl = "testScanUrl2",
    contentRetrievalUrl = "contentRetrieveUrl1", contentType = "application/json", categories = setCategoriesMap)
  val downstream1 = new Endpoint(name = "TEST_DOWNSTREAM", scanUrl = "testScanUrl3", contentType = "application/json", categories = intRangeCategoriesMap)
  val downstream2 = new Endpoint(name = "TEST_DOWNSTREAM_ALT", scanUrl = "testScanUrl4",
    versionGenerationUrl = "generateVersionUrl1", contentType = "application/json", categories = stringPrefixCategoriesMap)

  val domainName = "domain"
  val domain = new Domain(domainName)

  val versionPolicyName1 = "TEST_VPNAME"
  val matchingTimeout = 120
  val versionPolicyName2 = "TEST_VPNAME_ALT"
  val pairKey = "TEST_PAIR"
  val pairDef = new PairDef(pairKey, domainName, versionPolicyName1, matchingTimeout, upstream1.name,
    downstream1.name)
  val repairAction = new RepairAction(name="REPAIR_ACTION_NAME",
                                      scope=RepairAction.ENTITY_SCOPE,
                                      url="resend",
                                      pairKey=pairKey)

  val upstreamRenamed = "TEST_UPSTREAM_RENAMED"
  val pairRenamed = "TEST_PAIR_RENAMED"

  val TEST_USER = User("foo","foo@bar.com")

  def declareAll() {
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    configStore.createOrUpdateEndpoint(domainName, upstream2)
    configStore.createOrUpdateEndpoint(domainName, downstream1)
    configStore.createOrUpdateEndpoint(domainName, downstream2)
    //configStore.createOrUpdateDomain(domain)
    configStore.createOrUpdatePair(domainName, pairDef)
    configStore.createOrUpdateRepairAction(domainName, repairAction)
  }

  @Before
  def setUp: Unit = {
    HibernateConfigStoreTest.clearAllConfig
  }

  def exists (e:Endpoint, count:Int, offset:Int) : Unit = {
    val endpoints = configStore.listEndpoints(domainName)
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
  def testDeclare: Unit = {
    // Declare endpoints
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    exists(upstream1, 1)

    configStore.createOrUpdateEndpoint(domainName, downstream1)
    exists(downstream1, 2)

    // Declare a pair
    configStore.createOrUpdatePair(domainName, pairDef)

    val retrPair = configStore.getPair(domainName, pairDef.pairKey)
    assertEquals(pairKey, retrPair.key)
    assertEquals(upstream1.name, retrPair.upstream.name)
    assertEquals(downstream1.name, retrPair.downstream.name)
    assertEquals(versionPolicyName1, retrPair.versionPolicyName)
    assertEquals(matchingTimeout, retrPair.matchingTimeout)

    // Declare a repair action
    configStore.createOrUpdateRepairAction(domainName, repairAction)
    val retrActions = configStore.listRepairActionsForPair(domainName, retrPair)
    assertEquals(1, retrActions.length)
    assertEquals(Some(pairKey), retrActions.headOption.map(_.pairKey))
  }

  @Test
  def testPairsAreValidatedBeforeUpdate() {
    // Declare endpoints
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    exists(upstream1, 1)

    configStore.createOrUpdateEndpoint(domainName, downstream1)
    exists(downstream1, 2)

    pairDef.scanCronSpec = "invalid"

    try {
      configStore.createOrUpdatePair(domainName, pairDef)
      fail("Should have thrown ConfigValidationException")
    } catch {
      case ex:ConfigValidationException =>
        assertEquals("pair[key=TEST_PAIR]: Schedule 'invalid' is not a valid: Illegal characters for this position: 'INV'", ex.getMessage)
    }
  }

  @Test
  def testEndpointsWithSameScanURL {
    configStore.createOrUpdateEndpoint(domainName, upstream1)

    upstream2.scanUrl = upstream1.scanUrl
    configStore.createOrUpdateEndpoint(domainName, upstream2)

    exists(upstream1, 2, 0)
    exists(upstream2, 2, 1)
  }


  @Test
  def testUpdateEndpoint: Unit = {
    // Create endpoint
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    exists(upstream1, 1)

    configStore.deleteEndpoint(domainName, upstream1.name)
    expectMissingObject("endpoint") {
      configStore.getEndpoint(domainName, upstream1.name)
    }
        
    // Change its name
    configStore.createOrUpdateEndpoint(domainName, Endpoint(name = upstreamRenamed, scanUrl = upstream1.scanUrl, contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json"))

    val retrieved = configStore.getEndpoint(domainName, upstreamRenamed)
    assertEquals(upstreamRenamed, retrieved.name)
  }

  @Test
  def testUpdatePair: Unit = {
    declareAll

    // Rename, change a few fields and swap endpoints by deleting and creating new
    configStore.deletePair(domainName, pairKey)
    expectMissingObject("pair") {
      configStore.getPair(domainName, pairKey)
    }

    configStore.createOrUpdatePair(domainName, new PairDef(pairRenamed, domainName,  versionPolicyName2, Pair.NO_MATCHING,
      downstream1.name, upstream1.name, "0 0 * * * ?"))
    
    val retrieved = configStore.getPair(domainName, pairRenamed)
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

    assertEquals(upstream1.name, configStore.getEndpoint(domainName, upstream1.name).name)
    configStore.deleteEndpoint(domainName, upstream1.name)
    expectMissingObject("endpoint") {
      configStore.getEndpoint(domainName, upstream1.name)
    }
    expectMissingObject("pair") {
      configStore.getPair(domainName, pairKey) // delete should cascade
    }
  }

  @Test
  def testDeletePair: Unit = {
    declareAll

    assertEquals(pairKey, configStore.getPair(domainName, pairKey).key)
    configStore.deletePair(domainName, pairKey)
    expectMissingObject("pair") {
      configStore.getPair(domainName, pairKey)
    }
  }

  @Test
  def testDeletePairCascade {
    declareAll()
    assertEquals(Some(repairAction.name), configStore.listRepairActions(domainName).headOption.map(_.name))
    configStore.deletePair(domainName, pairKey)
    expectMissingObject("repair action") {
      configStore.getRepairAction(domainName, repairAction.name, pairKey)
    }
  }

  @Test
  def testDeleteRepairAction {
    declareAll
    assertEquals(Some(repairAction.name), configStore.listRepairActions(domainName).headOption.map(_.name))

    configStore.deleteRepairAction(domainName, repairAction.name, pairKey)
    expectMissingObject("repair action") {
      configStore.getRepairAction(domainName, repairAction.name, pairKey)
    }
  }

  @Test
  def testDeleteMissing: Unit = {
    expectMissingObject("endpoint") {
      configStore.deleteEndpoint(domainName, "MISSING_ENDPOINT")
    }

    expectMissingObject("pair") {
      configStore.deletePair(domainName, "MISSING_PAIR")
    }
  }

  @Test
  def testDeclarePairNullConstraints: Unit = {
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    configStore.createOrUpdateEndpoint(domainName, downstream1)

      // TODO: We should probably get an exception indicating that the constraint was null, not that the object
      //       we're linking to is missing.
    expectMissingObject("endpoint") {
      configStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName1, Pair.NO_MATCHING, null, downstream1.name))
    }
    expectMissingObject("endpoint") {
      configStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName1, Pair.NO_MATCHING, upstream1.name, null))
    }
  }

  @Test
  def testRedeclareEndpointSucceeds = {
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    configStore.createOrUpdateEndpoint(domainName, Endpoint(name = upstream1.name, scanUrl = "DIFFERENT_URL", contentType = "application/json", inboundUrl = "changes", inboundContentType = "application/json"))
    assertEquals(1, configStore.listEndpoints(domainName).length)
    assertEquals("DIFFERENT_URL", configStore.getEndpoint(domainName, upstream1.name).scanUrl)
  }

  @Test
  def rangeCategory = {
    declareAll
    val pair = configStore.getPair(domainName, pairKey)
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
    val endpoint = configStore.getEndpoint(domainName, upstream2.name)
    assertNotNull(endpoint.categories)
    val descriptor = endpoint.categories(dateCategoryName).asInstanceOf[SetCategoryDescriptor]
    assertEquals(setCategoryValues, descriptor.values.toSet)
  }

  @Test
  def prefixCategory = {
    declareAll
    val endpoint = configStore.getEndpoint(domainName, downstream2.name)
    assertNotNull(endpoint.categories)
    val descriptor = endpoint.categories(stringCategoryName).asInstanceOf[PrefixCategoryDescriptor]
    assertEquals(1, descriptor.prefixLength)
    assertEquals(3, descriptor.maxLength)
    assertEquals(1, descriptor.step)
  }

  @Test
  def testUser = {
    configStore.createOrUpdateUser(domainName, TEST_USER)
    val result = configStore.listUsers(domainName)
    assertEquals(1, result.length)
    assertEquals(TEST_USER, result(0))
    val updated = User(TEST_USER.name, "somethingelse@bar.com")
    configStore.createOrUpdateUser(domainName, updated)
    val user = configStore.getUser(domainName, TEST_USER.name)
    assertEquals(updated, user)
    configStore.deleteUser(domainName, TEST_USER.name)
    val users = configStore.listUsers(domainName)
    assertEquals(0, users.length)    
  }

  @Test
  def testApplyingDefaultConfigOption = {
    assertEquals("defaultVal", configStore.configOptionOrDefault(domainName,"some.option", "defaultVal"))
  }

  @Test
  def testReturningNoneForConfigOption {
    assertEquals(None, configStore.maybeConfigOption(domainName, "some.option"))
  }

  @Test
  def testRetrievingConfigOption = {
    configStore.setConfigOption(domainName, "some.option2", "storedVal")
    assertEquals("storedVal", configStore.configOptionOrDefault(domainName, "some.option2", "defaultVal"))
    assertEquals(Some("storedVal"), configStore.maybeConfigOption(domainName, "some.option2"))
  }

  @Test
  def testUpdatingConfigOption = {
    configStore.setConfigOption(domainName, "some.option3", "storedVal")
    configStore.setConfigOption(domainName, "some.option3", "storedVal2")
    assertEquals("storedVal2", configStore.configOptionOrDefault(domainName, "some.option3", "defaultVal"))
    assertEquals(Some("storedVal2"), configStore.maybeConfigOption(domainName, "some.option3"))
  }

  @Test
  def testRemovingConfigOption = {
    configStore.setConfigOption(domainName, "some.option3", "storedVal")
    configStore.clearConfigOption(domainName, "some.option3")
    assertEquals("defaultVal", configStore.configOptionOrDefault(domainName, "some.option3", "defaultVal"))
    assertEquals(None, configStore.maybeConfigOption(domainName, "some.option3"))
  }

  @Test
  def testRetrievingAllOptions = {
    configStore.setConfigOption(domainName, "some.option3", "storedVal")
    configStore.setConfigOption(domainName, "some.option4", "storedVal3")
    assertEquals(Map("some.option3" -> "storedVal", "some.option4" -> "storedVal3"), configStore.allConfigOptions(domainName))
  }

  @Test
  def testRetrievingOptionsIgnoresInternalOptions = {
    configStore.setConfigOption(domainName, "some.option3", "storedVal")
    configStore.setConfigOption(domainName, "some.option4", "storedVal3")
    assertEquals(Map("some.option3" -> "storedVal"), configStore.allConfigOptions(domainName))
  }

  @Test
  def testOptionCanBeUpdatedToBeInternal = {
    configStore.setConfigOption(domainName, "some.option3", "storedVal")
    configStore.setConfigOption(domainName, "some.option3", "storedVal3")
    assertEquals(Map(), configStore.allConfigOptions(domainName))
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

object HibernateConfigStoreTest {
  private val config =
      new Configuration().
        addResource("net/lshift/diffa/kernel/config/Config.hbm.xml").
        setProperty("hibernate.dialect", "org.hibernate.dialect.DerbyDialect").
        setProperty("hibernate.connection.url", "jdbc:derby:target/configStore;create=true").
        setProperty("hibernate.connection.driver_class", "org.apache.derby.jdbc.EmbeddedDriver")

  val sessionFactory = {
    val sf = config.buildSessionFactory
    (new HibernateConfigStorePreparationStep).prepare(sf, config)
    sf
  }

  val configStore = new HibernateConfigStore(sessionFactory)

  def clearAllConfig = {
    val s = sessionFactory.openSession
    s.createCriteria(classOf[User]).list.foreach(u => s.delete(u))
    s.createCriteria(classOf[Pair]).list.foreach(p => s.delete(p))
    s.createCriteria(classOf[Endpoint]).list.foreach(p => s.delete(p))
    s.createCriteria(classOf[ConfigOption]).list.foreach(o => s.delete(o))
    s.createCriteria(classOf[RepairAction]).list.foreach(s.delete)
    s.createCriteria(classOf[Domain]).list.foreach(s.delete)
    s.flush
    s.close
  }
}
