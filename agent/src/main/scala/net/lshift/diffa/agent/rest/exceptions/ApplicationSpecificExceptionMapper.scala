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

import javax.ws.rs.core.Response
import org.slf4j.LoggerFactory
import javax.ws.rs.ext.{Provider, ExceptionMapper}

/**
 * This logs all unhandled errors that occur in the application and returns an HTTP 500 to the requester.
 */
@Provider
class ApplicationSpecificExceptionMapper extends ExceptionMapper[Throwable] {

  val log = LoggerFactory.getLogger(getClass)

  def toResponse(t: Throwable) = {
    log.error("Unhandled application exception", t)
    Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(t.toString).`type`("text/plain").build()
  }
}