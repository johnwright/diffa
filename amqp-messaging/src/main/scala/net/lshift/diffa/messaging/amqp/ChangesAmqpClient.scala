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

import com.rabbitmq.messagepatterns.unicast.Connector
import net.lshift.diffa.kernel.client.ChangesClient
import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.frontend.wire.WireEvent
import net.lshift.diffa.messaging.json.JSONEncodingUtils._
import org.slf4j.LoggerFactory

/**
 * RPC client wrapper for clients to report change events to the diffa agent using JSON over AMQP.
 */
class ChangesAmqpClient(connector: Connector,
                        queueName: String,
                        timeout: Long)

  extends AmqpProducer(connector, queueName)
  with ChangesClient {

  private val log = LoggerFactory.getLogger(getClass)

  def onChangeEvent(evt: ChangeEvent) {
    val wire = evt match {
      case us: UpstreamChangeEvent              => WireEvent.toWire(us)
      case ds: DownstreamChangeEvent            => WireEvent.toWire(ds)
      case dsc: DownstreamCorrelatedChangeEvent => WireEvent.toWire(dsc)
    }
    val payload = serializeEvent(wire)
    if (log.isDebugEnabled) {
      log.debug("onChangeEvent: %s".format(payload))
    }
    send(payload)
  }
}
