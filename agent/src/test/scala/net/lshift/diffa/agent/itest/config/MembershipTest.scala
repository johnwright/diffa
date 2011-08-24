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

package net.lshift.diffa.agent.itest.config

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.agent.itest.support.TestConstants._
import com.eaio.uuid.UUID
import net.lshift.diffa.agent.client.{ConfigurationRestClient, UsersRestClient, SystemConfigRestClient}
import net.lshift.diffa.kernel.frontend.{UserDef, DomainDef}

/**
 * Tests whether domain membership admin is accessible via the REST API
 */
class MembershipTest {

  val username = new UUID().toString
  val email = username + "@test.diffa.io"
  val domain = DomainDef(name = new UUID().toString)

  val systemConfigClient = new SystemConfigRestClient(agentURL)
  val usersClient = new UsersRestClient(agentURL)
  val configClient = new ConfigurationRestClient(agentURL, domain.name)

  @Test
  def shouldBeAbleToManageDomainMembership = {

    def assertIsDomainMember(username:String, expectation:Boolean) = {
      val members = configClient.listDomainMembers
      val isMember = members.toSeq.find(m => m.name == username).isDefined
      assertEquals(expectation, isMember)
    }
    systemConfigClient.declareDomain(domain)
    usersClient.declareUser(UserDef(username,email,false,"foo"))

    configClient.makeDomainMember(username)

    assertIsDomainMember(username, true)

    configClient.removeDomainMembership(username)
    assertIsDomainMember(username, false)
  }

}