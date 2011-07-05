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

package net.lshift.diffa.participants

import org.eclipse.jetty.server.handler.AbstractHandler
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.eclipse.jetty.server.{Request, Server}
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}
import java.io.OutputStream
import net.lshift.diffa.participant.content.{ContentParticipantHandler, ContentParticipantDelegator}
import net.lshift.diffa.participant.scanning.ScanningParticipantRequestHandler
import net.lshift.diffa.participant.correlation.{VersioningParticipantHandler, VersioningParticipantDelegator}

class ParticipantRpcServer(port: Int, handler: ProtocolHandler, scanning:ScanningParticipantRequestHandler, content:ContentParticipantHandler, versioning:VersioningParticipantHandler) {
  private val contentAdapter = new ContentParticipantDelegator(content)
  private val versioningAdapter = new VersioningParticipantDelegator(versioning)

  private val server = new Server(port)
  server.setHandler(new AbstractHandler {
    override def handle(target: String, jettyReq: Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
      if (target.startsWith("/scan")) {
        scanning.handleRequest(request, response)
      } else if (target.startsWith("/content")) {
        contentAdapter.handleRequest(request, response);
      } else if (versioning != null && target.startsWith("/corr-version")) {
        versioningAdapter.handleRequest(request, response);
      } else {
        val os = response.getOutputStream
        val tRequest = new TransportRequest(request.getPathInfo.substring(1), request.getInputStream)
        val tResponse = new TransportResponse {
          def setStatusCode(code: Int) = response.setStatus(code)
          def withOutputStream(f: (OutputStream) => Unit) = f(response.getOutputStream)
        }

        handler.handleRequest(tRequest, tResponse)
      }

      jettyReq.setHandled(true)
    }
  })
  server.setStopAtShutdown(true)
  
  def start: Unit = {
    server.start
  }
}