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

package net.lshift.diffa.kernel.config.internal

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.kernel.config.Endpoint._
import collection.JavaConversions._
import net.lshift.diffa.kernel.config.{Pair => DiffaPair}
import net.lshift.diffa.kernel.config.{RangeCategoryDescriptor, Endpoint, PairDef}
import org.joda.time.DateTime

class HibernateInternalConfigStoreTest {

  private val configStore: InternalConfigStore = null

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
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    assertEquals(0, configStore.getPairsForEndpoint(upstream1.name).length)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsPairUsingEndpointAsUpstream {
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    configStore.createOrUpdateEndpoint(domainName, downstream1)
    configStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName2, DiffaPair.NO_MATCHING,
                                               upstream1.name, downstream1.name))

    val res = configStore.getPairsForEndpoint(upstream1.name)
    assertEquals(1, res.length)
    assertEquals(pairKey, res(0).key)
  }

  @Test
  def testQueryingForAssociatedPairsReturnsPairUsingEndpointAsDownstream {
    configStore.createOrUpdateEndpoint(domainName, upstream1)
    configStore.createOrUpdateEndpoint(domainName, downstream1)
    configStore.createOrUpdatePair(domainName, new PairDef(pairKey, domainName, versionPolicyName2, DiffaPair.NO_MATCHING,
                                               upstream1.name, downstream1.name))

    val res = configStore.getPairsForEndpoint(downstream1.name)
    assertEquals(1, res.length)
    assertEquals(pairKey, res(0).key)
  }
}