/**
 *  Copyright (C) 2010-2011 LShift Ltd.
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

package net.lshift.diffa.agent.amqp

import collection.mutable.HashMap
import net.lshift.diffa.kernel.config.Endpoint
import net.lshift.diffa.kernel.frontend.Changes
import net.lshift.diffa.kernel.participants.InboundEndpointFactory
import org.slf4j.LoggerFactory
import net.lshift.accent.AccentConnection
import com.rabbitmq.client.ConnectionFactory

class AmqpInboundEndpointFactory(changes: Changes)
  extends InboundEndpointFactory {

  val log = LoggerFactory.getLogger(getClass)

  val consumers = new HashMap[String, AccentReceiver]

  def canHandleInboundEndpoint(inboundUrl: String) =
    inboundUrl.startsWith("amqp://")

  def ensureEndpointReceiver(e: Endpoint) {
    log.info("Starting consumer for endpoint: %s".format(e))

    val amqpUrl = AmqpQueueUrl.parse(e.inboundUrl)
    val params = new ReceiverParameters(amqpUrl.queue)

    val c = new AccentReceiver(createAccentConnection(amqpUrl),
                               params,
                               e.domain.name,
                               e.name,
                               changes)
    consumers.put(e.name, c)
  }

  private def createAccentConnection(url: AmqpQueueUrl) = {
    val cf = new ConnectionFactory()
    cf.setHost(url.host)
    cf.setPort(url.port)
    cf.setUsername(url.username)
    cf.setPassword(url.password)
    if (! url.isDefaultVHost) {
      cf.setVirtualHost(url.vHost)
    }

    new AccentConnection(cf, new AccentConnectionFailureHandler)
  }

  def endpointGone(key: String) {
    consumers.get(key).map { c =>
      try {
        c.close()
      } catch {
        case _ => log.error("Unable to shutdown consumer for endpoint name %s".format(key))
      }
    }
    consumers.remove(key)
  }
}