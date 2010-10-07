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

import java.io.OutputStream

/**
 * Provides access to the response that will be sent via the transport for a given request.
 */
trait TransportResponse {
  def setStatusCode(code:Int)

  /**
   * Provides a context in which a consumer can write to the response's output stream. 
   */
  def withOutputStream(f:(OutputStream) => Unit)
}