/**
 * Copyright (C) 2010 - 2012 LShift Ltd.
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
package net.lshift.diffa.agent.itest.preferences

import net.lshift.diffa.kernel.preferences.FilteredItemType
import net.lshift.diffa.agent.itest.support.TestConstants._
import net.lshift.diffa.agent.client.{SystemConfigRestClient, ConfigurationRestClient, UsersRestClient}
import org.junit.{Before, Test}
import org.junit.Assert._
import net.lshift.diffa.kernel.frontend.{DomainDef, DomainPairDef, EndpointDef}
import com.eaio.uuid.UUID

class UserPreferencesTest {

  val domain = new UUID().toString

  val preferencesClient = new UsersRestClient(agentURL, agentUsername)
  val systemConfigClient = new SystemConfigRestClient(agentURL)
  val configClient = new ConfigurationRestClient(agentURL, domain)

  val upstream = EndpointDef(name = new UUID().toString)
  val downstream = EndpointDef(name = new UUID().toString)
  val pair = DomainPairDef(key = new UUID().toString,
                           domain = domain,
                           upstreamName = upstream.name,
                           downstreamName = downstream.name)

  @Before
  def createTestData {
    systemConfigClient.declareDomain(DomainDef(name = domain))

    configClient.declareEndpoint(upstream)
    configClient.declareEndpoint(downstream)
    configClient.declarePair(pair.withoutDomain)

  }

  @Test
  def shouldSetAndDeleteFilters {

    preferencesClient.createFilter(pair.asRef, FilteredItemType.SWIM_LANE)

    val filtered = preferencesClient.getFilteredItems(domain, FilteredItemType.SWIM_LANE)
    assertEquals(Seq(pair.key), filtered)

    preferencesClient.removeFilter(pair.asRef, FilteredItemType.SWIM_LANE)

    val shouldBeEmpty = preferencesClient.getFilteredItems(domain, FilteredItemType.SWIM_LANE)
    assertEquals(Seq(), shouldBeEmpty)
  }
}
