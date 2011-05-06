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

package net.lshift.diffa.messaging.amqp

import java.io.IOException

/**
 * Exception thrown when by AMQP RPC clients when a response is not received within a fixed
 * period of time.
 */
case class ReceiveTimeoutException(queueName:String, endpoint:String, timeoutMillis: Long) extends IOException {

  override def getMessage = "Receive (%s - %s) timed out after %dms".format(queueName, endpoint, timeoutMillis)
}