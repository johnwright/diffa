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

package net.lshift.diffa.agent.rest.exceptions

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import org.slf4j.LoggerFactory
import javax.ws.rs.ext.{Provider, ExceptionMapper}

/**
 * Handles all normal exceptions that arise in the handling of an HTTP request, as opposed to exceptions that occur within
 * the application itself. This mapper matches on standard HTTP errors such as 404 errors.
 *
 * This class is required since the application specific exception mapper matches all subclasses
 * of Throwable, so mapping a specific handler for routing HTTP errors that either:
 *
 * <ul>
 *   <li>never hit the application because the inbound request itself is not routable;</li>
 *   <li>or get generated explicitly by RESTful resource handling code in the application.</li>
 * </ul>
 *
 * If a deployment wants to trace all HTTP style errors that occur, the log level needs to be set to TRACE.
 */
@Provider
class FrameworkExceptionMapper extends ExceptionMapper[WebApplicationException] {

  val log = LoggerFactory.getLogger(getClass)

  def toResponse(ex: WebApplicationException) = {
    if (log.isTraceEnabled) {
      log.trace("Handling a standard container exception", ex)
    }
    Response.fromResponse(ex.getResponse).build()
  }

}