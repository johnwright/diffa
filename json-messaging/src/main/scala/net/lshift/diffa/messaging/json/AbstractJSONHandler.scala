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

package net.lshift.diffa.messaging.json

import org.apache.commons.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}
import org.apache.http.HttpStatus
import org.codehaus.jettison.json.JSONObject
import net.lshift.diffa.kernel.protocol.{ProtocolHandler, TransportRequest, TransportResponse}

/**
 * Common Functionality shareable between multiple JSON handlers
 */
abstract class AbstractJSONHandler extends ProtocolHandler {
  type RequestHandler = Function2[TransportRequest, TransportResponse, Boolean]

  protected val log:Logger = LoggerFactory.getLogger(getClass)

  /**
   * The endpoints that can be triggered within the handler.
   */
  protected def endpoints:Map[String, RequestHandler]

  val contentTypes = Seq("application/json")
  def endpointNames = endpoints.keySet.toSeq
  def handleRequest(request: TransportRequest, response: TransportResponse) = {
    endpoints.get(request.endpoint) match {
      case Some(handler) =>
        try {
          handler(request, response)
        } catch {
          case ex => {
            log.info("Request failed", ex)

            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
            val errorMsg = ex.getMessage match {
              case null => ex.getClass.getName
              case m    => m
            }
            response.withOutputStream(os => os.write(buildError(errorMsg)))

            true
          }
        }
      case None => {
        log.error("No request handler for endpoint " + request.endpoint)

        response.setStatusCode(HttpStatus.SC_BAD_REQUEST)
        response.withOutputStream(os => os.write(buildError("No request handler for endpoint " + request.endpoint)))

        true
      }
    }
  }

  private def buildError(errorMsg:String):Array[Byte] = {
    val errorObj = new JSONObject
    errorObj.put("message", errorMsg)
    errorObj.toString.getBytes("UTF-8")
  }

  /**
   * Curries a given function into a callable handler that takes a request and writes out data for a response.
   */
  protected def defineRpc[T](requestDecoder:String => T)(f: T => String):RequestHandler = {
    (request: TransportRequest, response: TransportResponse) => {
      val bytes = IOUtils.toString(request.is, "UTF-8")
      val respMsg = f(requestDecoder(bytes))
      response.withOutputStream(os => {
        os.write(respMsg.getBytes("UTF-8"))
      })

      true
    }
  }

  protected def defineOnewayRpc[T](requestDecoder:String => T)(f: T => Unit):RequestHandler = {
    (request: TransportRequest, response: TransportResponse) => {
      val bytes = IOUtils.toString(request.is, "UTF-8")
      f(requestDecoder(bytes))
      response.withOutputStream(os => {
        os.write("{}".getBytes("UTF-8"))
      })

      true
    }
  }
}