/**
 * Copyright (C) 2010-2012 LShift Ltd.
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

import org.junit.Assert._
import net.lshift.diffa.agent.itest.support.TestConstants._
import scala.collection.JavaConversions._
import net.lshift.diffa.client.ScanningParticipantRestClient
import net.lshift.diffa.participant.scanning.StringPrefixConstraint
import org.junit.Test
import org.easymock.EasyMock._
import net.lshift.diffa.kernel.config._
import org.easymock.EasyMock
import java.net.URI

class UsersScanningTest {
  val limits = new PairServiceLimitsView {
    def getEffectiveLimitByNameForPair(limitName: String, domainName: String, pairKey: String): Int = ServiceLimit.UNLIMITED
  }

  val pair = DiffaPairRef("foo","bar")

  val domainCredentialsLookup = createStrictMock("domainCredentialsLookup", classOf[DomainCredentialsLookup])
  expect(domainCredentialsLookup.credentialsForUri(EasyMock.eq(pair.domain),
                                                   EasyMock.isA(classOf[URI]))).
                                 andStubReturn(Some(BasicAuthCredentials("guest", "guest")))

  replay(domainCredentialsLookup)



  val participant = new ScanningParticipantRestClient(pair, agentURL + "/security/scan", limits, domainCredentialsLookup)

  @Test
  def aggregationShouldIncludeGuestUser {

    val constraints = Seq(new StringPrefixConstraint("name", "g"))
    val bucketing = Seq()

    val results = participant.scan(constraints, bucketing).toSeq

    assertFalse(results.filter( r => {r.getId == "guest"} ).isEmpty)
  }

  @Test
  def aggregationShouldNotIncludeGuestUser {

    val constraints = Seq(new StringPrefixConstraint("name", "x"))
    val bucketing = Seq()

    val results = participant.scan(constraints, bucketing).toSeq

    assertTrue(results.filter( r => {r.getId == "guest"} ).isEmpty)
  }
}