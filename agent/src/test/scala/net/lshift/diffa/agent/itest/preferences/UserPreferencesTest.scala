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
import net.lshift.diffa.agent.client.{SecurityRestClient, SystemConfigRestClient, ConfigurationRestClient, UsersRestClient}
import org.junit.{Before, Test}
import org.junit.Assert._
import net.lshift.diffa.kernel.frontend.{UserDef, DomainDef, DomainPairDef, EndpointDef}
import com.eaio.uuid.UUID
import net.lshift.diffa.client.AccessDeniedException

class UserPreferencesTest {

  val domain = new UUID().toString

  val rootUserPreferencesClient = new UsersRestClient(agentURL, agentUsername)
  val systemConfigClient = new SystemConfigRestClient(agentURL)
  val configClient = new ConfigurationRestClient(agentURL, domain)
  val securityClient = new SecurityRestClient(agentURL)

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

    rootUserPreferencesClient.createFilter(pair.asRef, FilteredItemType.SWIM_LANE)

    val filtered = rootUserPreferencesClient.getFilteredItems(domain, FilteredItemType.SWIM_LANE)
    assertEquals(Seq(pair.key), filtered)

    rootUserPreferencesClient.removeFilter(pair.asRef, FilteredItemType.SWIM_LANE)

    val shouldBeEmpty = rootUserPreferencesClient.getFilteredItems(domain, FilteredItemType.SWIM_LANE)
    assertEquals(Seq(), shouldBeEmpty)
  }

  @Test(expected = classOf[AccessDeniedException])
  def shouldNotBeAbleToModifySettingsForADifferentUser {

    val nonRootUser = UserDef(name = new UUID().toString, superuser = false, external = true)
    securityClient.declareUser(nonRootUser)

    val nonRootUserPreferencesClient = new UsersRestClient(agentURL, nonRootUser.name)
    nonRootUserPreferencesClient.createFilter(pair.asRef, FilteredItemType.SWIM_LANE)
  }
}
