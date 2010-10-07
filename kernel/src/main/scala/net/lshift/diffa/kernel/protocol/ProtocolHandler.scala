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

package net.lshift.diffa.kernel.protocol

import java.io.InputStream

/**
 * A protocol handler provides the bridge from an endpoint name/body combination, and actions a request to
 * an appropriate backend.
 */
trait ProtocolHandler {
  /**
   * Retrieves the content types accepted by the handler.
   */
  def contentTypes:Seq[String]

  /**
   * Informs the handler that a request needs to dealt with. The transport will provide details of the
   * request along with access to the response. The method will return a boolean value indicating whether the
   * response has been completed. An streaming style requests will return false, and require that the transport
   * support keeping the connection to the client available for further response data to be transmitted.
   */
  def handleRequest(request:TransportRequest, response:TransportResponse):Boolean

  /**
   * Retrieves a list of all valid endpoint names supported by the protocol handler.
   */
  def endpointNames:Seq[String]
}