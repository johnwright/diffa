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

  @Before
  def ensureServerStarted {
    if (!server.isRunning) server.start()
  }

  @Theory
  def shouldBeAbleToCorrectlyAddQueryStringToBaseUrlWithQueryString(ex: Example) = {
    val query = new MultivaluedMapImpl()
    for ((k, v) <- ex.query) query.add(k, v)
    val client = clientFor(ex.requestUrl)

    assertEquals(serverRootUrl + ex.expected,
      client.queryRequestUrlFor(query, None))
  }

  @Theory
  def shouldBeAbleToCorrectlyAddQueryStringToBaseUrlWithQueryStringForSubmission(ex: Example) = {
    val query = new MultivaluedMapImpl()
    for ((k, v) <- ex.query) query.add(k, v)
    val client = clientFor(ex.requestUrl)

    assertEquals(serverRootUrl + ex.expected,
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

  val serverPort = 41256
  lazy val server = new DummyServer(serverPort)
  val pair = new DiffaPairRef("some-domain", "some-pair")
  val domainCredentialsLookup = new FixedDomainCredentialsLookup(pair.domain, None)
  val limits = new PairServiceLimitsView {
    def getEffectiveLimitByNameForPair(limitName: String, domainName: String, pairKey: String): Int = ServiceLimit.UNLIMITED
  }
  val serverRootUrl = "http://localhost:" + serverPort

  def clientFor(baseUrl: String) = new InternalRestClient(pair, serverRootUrl + baseUrl, limits, domainCredentialsLookup) {
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

class DummyServer(port: Int) {
  private val server = new Server(port)
  var lastQueryReceived: Option[Map[String, String]] = None
  server.setHandler(new AbstractHandler {
    def urlDecode(s: String) = {
      URLDecoder.decode(s, "UTF-8")
    }

    override def handle(target: String, jettyReq: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
      System.out.println("Request target qs: %s".format(request.getParameterMap()))
      response.setStatus(HttpServletResponse.SC_NO_CONTENT)
      jettyReq.setHandled(true)

      //        val query = request.getParameterMap().foldLeft(Map[String, String]()) {
      //          case (map, (key, value)) => (map + (key -> value))
      //        }
      lastQueryReceived = Some(
        Map[String, String]())
    }
  })
  server.setStopAtShutdown(true)

  def start() {
    server.start()
  }

  def isRunning = server.isRunning
}