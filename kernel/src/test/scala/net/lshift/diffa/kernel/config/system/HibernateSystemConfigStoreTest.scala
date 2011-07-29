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

package net.lshift.diffa.kernel.config.system

import org.junit.Assert._
import net.lshift.diffa.kernel.config.Endpoint
import collection.JavaConversions._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.config.{Domain, HibernateDomainConfigStoreTest, DomainConfigStore, Pair => DiffaPair, RangeCategoryDescriptor, Endpoint, PairDef}
import org.junit.{Before, Test}

class HibernateSystemConfigStoreTest {

  private val domainConfigStore: DomainConfigStore = HibernateDomainConfigStoreTest.domainConfigStore
  private val sf = HibernateDomainConfigStoreTest.domainConfigStore.sessionFactory
  private val systemConfigStore:SystemConfigStore = new HibernateSystemConfigStore(domainConfigStore, sf)

  val domainName = "domain"
  val domain = Domain(name=domainName)

  val versionPolicyName1 = "TEST_VPNAME"
  val matchingTimeout = 120
  val versionPolicyName2 = "TEST_VPNAME_ALT"
  val pairKey = "TEST_PAIR"

  val bound = new DateTime().toString()
  val categories = Map("cat" ->  new RangeCategoryDescriptor("datetime", bound, bound))

  val upstream1 = new Endpoint(name = "TEST_UPSTREAM", domain = domain, scanUrl = "testScanUrl1",
                               inboundUrl = "http://foo.com",
                               contentType = "application/json", categories = categories)
  val downstream1 = new Endpoint(name = "TEST_DOWNSTREAM", domain = domain, scanUrl = "testScanUrl3",
                                 inboundUrl = "http://bar.com",
                                 contentType = "application/json", categories = categories)

  val pairDef = new PairDef(pairKey, domainName, versionPolicyName1, matchingTimeout, upstream1.name,
    downstream1.name)

  @Before
  def setup = {
    systemConfigStore.deleteDomain(domainName)
    systemConfigStore.createOrUpdateDomain(domain)
  }

  @Test
  def shouldBeAbleToSetSystemProperty = {
    systemConfigStore.createOrUpdateDomain(Domain.DEFAULT_DOMAIN)
    systemConfigStore.setSystemConfigOption("foo", "bar")
    assertEquals("bar", systemConfigStore.maybeSystemConfigOption("foo").get)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsNothingForUnusedEndpoint {
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    assertEquals(0, systemConfigStore.getPairsForInboundEndpointURL(upstream1.name).length)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsPairUsingEndpointAsUpstream {
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName2, DiffaPair.NO_MATCHING,
                                               upstream1.name, downstream1.name))

    val res = systemConfigStore.getPairsForInboundEndpointURL(upstream1.inboundUrl)
    assertEquals(1, res.length)
    assertEquals(pairKey, res(0).key)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsPairUsingEndpointAsDownstream {
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName2, DiffaPair.NO_MATCHING,
                                               upstream1.name, downstream1.name))

    val res = systemConfigStore.getPairsForInboundEndpointURL(downstream1.inboundUrl)
    assertEquals(1, res.length)
    assertEquals(pairKey, res(0).key)
  }
}