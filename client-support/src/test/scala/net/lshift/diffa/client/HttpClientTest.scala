package net.lshift.diffa.client

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.junit.{Test, Before}
import org.junit.Assert._
import org.hamcrest.Matchers._
import scala.collection.JavaConversions._
import java.net.URI
import org.apache.commons.codec.binary.Base64

/**
 * Copyright (C) 2010-2012 LShift Ltd.
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

class HttpClientTest {
  import HttpClientTest._

  val client: DiffaHttpClient = new ApacheHttpClient

  @Before def reset { HttpClientTest.reset }

  // @Test
  def makesCorrectRequestToServer {
    val req = DiffaHttpQuery(baseUrl + "foo").withQuery(Map("name" -> List("param")))
    client.get(req)
    println("client req seen: %s".format(lastRequest))

    assertThat[Option[URI]](
        lastRequest.map(_.fullUri), is(Some(new URI("/foo?name=param")).asInstanceOf[Option[URI]]))
  }

  // @Test
  def makesCorrectRequestToServerWithQueryParameters {
    val req = DiffaHttpQuery(baseUrl + "foo?from=baseUri").withQuery(Map("name" -> List("param")))
    client.get(req)
    val expected: Option[DiffaHttpQuery] = Some(DiffaHttpQuery("/foo").withQuery(Map("from" -> List("baseUri"), "name" -> List("param"))))

    assertThat(lastRequest, is(expected))
  }


  @Test
  def shouldIncludeBasicAuthWhenSpecified {
    val req = DiffaHttpQuery(baseUrl + "auth").withBasicAuth("user", "password")
    client.get(req)
    val expected: Option[(String, String)] = Some(("user", "password"))
    assertThat(lastRequest.flatMap(_.basicAuth), equalTo(expected))

  }



}

object HttpClientTest {
  val port = 23452
  private val server = new Server(port)
  server.setHandler(new AbstractHandler {
    override def handle(target: String, jettyReq: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
      val queryParams = request.getParameterMap.map { case (key, v) =>
        key.asInstanceOf[String] -> v.asInstanceOf[Array[String]].toSeq
      }.toMap

      // println("last req: %s".format(request.getParameterMap))
      if (target == "/auth" && jettyReq.getAuthentication == null) {
        response.setStatus(401)
        response.setHeader("WWW-Authenticate", "basic realm=\"Fnord\"" );
      }


      val authHeader = jettyReq.getHeader("Authorization") match {
        case h: String => Some(h);
        case null => None
      }
      val auth: Option[(String, String)] = for {
        h <- authHeader
        Array("Basic", enc:String) <- Some(h.split(' '))
        Array(u, p) <- Some(new String(Base64.decodeBase64(enc), "utf-8").split(':'))
      } yield (u, p)

      val query = (DiffaHttpQuery(request.getPathInfo).withQuery(queryParams) /: auth) { case (query, (u, p)) => query.withBasicAuth(u, p) }
      // also writable as: val query = DiffaHttpQuery(request.getPathInfo).withQuery(queryParams);
      // val queryWithAuth = auth.foldLeft(query) { case (query, (u, p)) => query.withBasicAuth(u, p) }

      lastRequest = Some(query)
      jettyReq.setHandled(true)
    }
  })
  val baseUrl = "http://127.0.0.1:%d/".format(port)

  def ensureStarted() = if(!server.isRunning()) server.start()

  var lastRequest: Option[DiffaHttpQuery] = None

  def reset { lastRequest = None; ensureStarted() }

}
