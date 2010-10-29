/**
 * Copyright (C) 2010 LShift Ltd.
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

package net.lshift.diffa.agent.http

import org.springframework.web.HttpRequestHandler
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.springframework.http.HttpStatus
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler, ProtocolMapper}
import java.io.OutputStream
import org.slf4j.LoggerFactory

/**
 * Handler for adapting HTTP requests to frontends via protocol handlers.
 */
class RequestHandlerProxy(mapper:ProtocolMapper) extends HttpRequestHandler {

  val log = LoggerFactory.getLogger(getClass)

  def handleRequest(request:HttpServletRequest, response:HttpServletResponse) = {

    val path = request.getServletPath.substring(1)
    log.trace("Handling path: " + path)

    val tRequest = new TransportRequest(path, request.getInputStream)
    val tResponse = new HttpTransportResponse(request, response)

    mapper.lookupHandler(path, request.getContentType) match {
      case Some(handler:ProtocolHandler) => handler.handleRequest(tRequest, tResponse)
      case None => {
        log.error("Cannot resolve handler for path: " + path + " and type: " + request.getContentType)
        response.setStatus(HttpStatus.BAD_REQUEST.value)
        response.getWriter.println("No protocol handlers available for content type")
      }
    }
  }

  class HttpTransportResponse(val request:HttpServletRequest, val response:HttpServletResponse) extends TransportResponse {
    val os = response.getOutputStream
    def setStatusCode(code: Int) = response.setStatus(code)
    def withOutputStream(f: (OutputStream) => Unit) = f(os)
  }
}
