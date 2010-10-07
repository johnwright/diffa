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
import org.eclipse.jetty.continuation.{ContinuationSupport, Continuation}
import java.io.OutputStream

/**
 * Handler for adapting HTTP requests to frontends via protocol handlers.
 */
class FrontendRequestHandler(mapper:ProtocolMapper, endpointGroup:String) extends HttpRequestHandler {
  def handleRequest(request:HttpServletRequest, response:HttpServletResponse) = {
    val tRequest = new TransportRequest(request.getPathInfo.substring(1), request.getInputStream)
    val tResponse = new HttpTransportResponse(request, response)

    mapper.lookupHandler(endpointGroup, request.getContentType) match {
      case Some(handler:ProtocolHandler) =>
        if (!handler.handleRequest(tRequest, tResponse)) {
          // Request hasn't finished. Make it asynchronous
          tResponse.makeAsync
        }
      case None =>
        response.setStatus(HttpStatus.BAD_REQUEST.value)
        response.getWriter.println("No protocol handlers available for content type")
    }
  }

  class HttpTransportResponse(val servletRequest:HttpServletRequest, val servletResponse:HttpServletResponse)
      extends TransportResponse {
    var async = false

    val os = servletResponse.getOutputStream
    def continuation = ContinuationSupport.getContinuation(servletRequest)
    
    def setStatusCode(code: Int) = servletResponse.setStatus(code)
    def withOutputStream(f: (OutputStream) => Unit) = {
      // If the response is marked as async, then we'll need to resume and suspend a continuation
      // around it.
      if (async) {
        // TODO: Standard Jetty model would have continuation.resume called, which would re-trigger the request
        //        handling thread with an empty-ish request. This currently breaks the request handler, since
        //        the post data is null, and therefore the protocol parser crashes out. Turns out that the output
        //        stream is still valid though, so you can actually write to it from other threads even if you don't
        //        resume the connection.
//        continuation.resume
      }
      f(os)

      if (async) {
        os.flush

        // TODO: Unused due to the above comment.
//        continuation.suspend
      }
    }

    def makeAsync {
      val cont = continuation
      os.flush

      cont.setTimeout(0)
      cont.suspend
      async = true
    }
  }
}