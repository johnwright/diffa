/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.client

import com.sun.jersey.core.util.MultivaluedMapImpl
import java.net.URLDecoder
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.participant.scanning.{ScanningParticipantDelegator, ScanningParticipantHandler}
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.{Request, Server}
import org.junit.experimental.theories.{Theories, DataPoint, Theory}
import org.junit.runner.RunWith
import org.junit.{Before, Test}
import org.junit.Assert._

@RunWith(classOf[Theories])
class InternalRestClientTest {

  import InternalRestClientTest._

  @Theory
  def shouldBeAbleToCorrectlyAddQueryStringToBaseUrlWithQueryString(ex: Example) = {
    val query = new MultivaluedMapImpl()
    for ((k, v) <- ex.query) query.add(k, v)
    val client = clientFor(ex.requestUrl)

    assertEquals(rootUrl + ex.expected,
      client.queryRequestUrlFor(query, None))
  }

  @Theory
  def shouldBeAbleToCorrectlyAddQueryStringToBaseUrlWithQueryStringForSubmission(ex: Example) = {
    val query = new MultivaluedMapImpl()
    for ((k, v) <- ex.query) query.add(k, v)
    val client = clientFor(ex.requestUrl)

    assertEquals(rootUrl + ex.expected,
      client.querySubmissionUrlFor(query, None))
  }
}

object InternalRestClientTest {

  case class Example(requestUrl: String,
                     query: Map[String, String],
                     expected: String)

  @DataPoint def nullExample = Example("/", Map(), "/")

  @DataPoint def simpleExample = Example("/", Map("dummy" -> "value"), "/?dummy=value")

  @DataPoint def baseWithAuth = Example("/?auth=dummy", Map(), "/?auth=dummy")

  @DataPoint def baseWithAuthPlusQuery = Example(
    "/?auth=dummy", Map("query" -> "value"), "/?auth=dummy&query=value")

 // lazy val server = new DummyServer(serverPort)
  val pair = new DiffaPairRef("some-domain", "some-pair")
  val domainCredentialsLookup = new FixedDomainCredentialsLookup(pair.domain, None)
  val limits = new PairServiceLimitsView {
    def getEffectiveLimitByNameForPair(limitName: String, domainName: String, pairKey: String): Int = ServiceLimit.UNLIMITED
  }
  val rootUrl = "http://localhost"

  def clientFor(baseUrl: String) = new InternalRestClient(pair, rootUrl + baseUrl, limits, domainCredentialsLookup) {
    def queryRequestUrlFor(queryParams: MultivaluedMapImpl,
                           credentials: Option[QueryParameterCredentials]) = {
      buildGetRequest(queryParams, credentials).getURI().toString

    }

    def querySubmissionUrlFor(queryParams: MultivaluedMapImpl,
                           credentials: Option[QueryParameterCredentials]) = {
      buildPostRequest(queryParams, Map(), credentials).getURI().toString

    }

  }


}