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

package net.lshift.diffa.kernel.frontend.wire

import reflect.BeanProperty

/**
 * This encapsulates the result of invoking an action against a participant
 */
case class InvocationResult (
  @BeanProperty var result:String,
  @BeanProperty var output:String) {

  def this() = this(null, null)
}

object InvocationResult {
  /**
   * Indicates that the action was performed successfully, i.e. the endpoint responded with a 2XX status
   */
  val SUCCESS = "success"

  /**
   * Indicates that the agent was able to communicate with the endpoint configured for the action,
   * but that a non-2XX status code was received
   */
  val HTTP_ERROR = "http_error"

  /**
   * Indicates a failure of communication such as a timeout, DNS error, connection lost, et cetera.
   * In this case a description of the error should be provided in the payload.
   */
  val FAILURE = "failure"

  /**
   * Factory for a InvocationResult indicating success
   */
  def success(payload: String) = InvocationResult(SUCCESS, payload)

  /**
   * Factory for an InvocationResult indicating that the agent received a non-2XX HTTP code
   */
  def httpError(payload: String) = InvocationResult(HTTP_ERROR, payload)

  /**
   * Factory for an InvocationResult indicating that the agent could not communicate with the endpoint
   */
  def failure(thrown: Throwable) = InvocationResult(FAILURE, thrown.getStackTraceString)
}
