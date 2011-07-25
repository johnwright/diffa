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

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.kernel.config.Endpoint._
import collection.JavaConversions._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.config.{HibernateDomainConfigStoreTest, DomainConfigStore, Pair => DiffaPair, RangeCategoryDescriptor, Endpoint, PairDef}

class HibernateSystemConfigStoreTest {

  private val domainConfigStore: DomainConfigStore = HibernateDomainConfigStoreTest.domainConfigStore
  private val systemConfigStore:SystemConfigStore = null

  val domainName = "domain"

  val versionPolicyName1 = "TEST_VPNAME"
  val matchingTimeout = 120
  val versionPolicyName2 = "TEST_VPNAME_ALT"
  val pairKey = "TEST_PAIR"

  val bound = new DateTime().toString()
  val categories = Map("cat" ->  new RangeCategoryDescriptor("datetime", bound, bound))

  val upstream1 = new Endpoint(name = "TEST_UPSTREAM", scanUrl = "testScanUrl1", contentType = "application/json", categories = categories)
  val downstream1 = new Endpoint(name = "TEST_DOWNSTREAM", scanUrl = "testScanUrl3", contentType = "application/json", categories = categories)

  val pairDef = new PairDef(pairKey, domainName, versionPolicyName1, matchingTimeout, upstream1.name,
    downstream1.name)

  @Test
  def testQueryingForAssociatedPairsReturnsNothingForUnusedEndpoint {
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    assertEquals(0, systemConfigStore.getPairsForEndpoint(upstream1.name).length)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsPairUsingEndpointAsUpstream {
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName2, DiffaPair.NO_MATCHING,
                                               upstream1.name, downstream1.name))

    val res = systemConfigStore.getPairsForEndpoint(upstream1.name)
    assertEquals(1, res.length)
    assertEquals(pairKey, res(0).key)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsPairUsingEndpointAsDownstream {
    domainConfigStore.createOrUpdateEndpoint(domainName, upstream1)
    domainConfigStore.createOrUpdateEndpoint(domainName, downstream1)
    domainConfigStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName2, DiffaPair.NO_MATCHING,
                                               upstream1.name, downstream1.name))

    val res = systemConfigStore.getPairsForEndpoint(downstream1.name)
    assertEquals(1, res.length)
    assertEquals(pairKey, res(0).key)
  }
}