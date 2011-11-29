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

  case class ConnectionKey(host: String, port: Int, username: String, password: String, vHost: String)

  object ConnectionKey {
    def fromUrl(url: AmqpQueueUrl) =
      ConnectionKey(host = url.host, port = url.port, username = url.username, password = url.password, vHost = url.vHost)
  }

  class Consumers(val connection: AccentConnection,
                  val connectionKey: ConnectionKey) extends HashMap[String, AccentReceiver]

  val log = LoggerFactory.getLogger(getClass)

  val consumers = new HashMap[ConnectionKey, Consumers]

  def canHandleInboundEndpoint(inboundUrl: String) =
    inboundUrl.startsWith("amqp://")

  def ensureEndpointReceiver(e: Endpoint) {
    log.info("Starting consumer for endpoint: %s".format(e))

    val amqpUrl = AmqpQueueUrl.parse(e.inboundUrl)


    val consumersForUrl = getConsumersByUrl(amqpUrl)
    consumersForUrl.put(e.name,
                        createConsumer(consumersForUrl.connection, amqpUrl.queue, e.domain.name, e.name))
  }

  def endpointGone(endpointName: String) {
    getConsumersByEndpoint(endpointName) match {
      case None =>
        log.error("No consumers for endpoint name: %s".format(endpointName))

      case Some(cons) =>
        cons.get(endpointName) map { c =>
          try {
            c.close()
          } catch {
            case _ => log.error("Unable to shutdown consumer for endpoint name %s".format(endpointName))
          }
        }
        cons.remove(endpointName)

        // if there are no more consumers on the connection, close it
        if (cons.isEmpty) {
          try {
            cons.connection.close()
          } catch {
            case _ => log.error("Unable to shutdown connection for endpoint name %s".format(endpointName))
          }
          consumers.remove(cons.connectionKey)
        }
    }
  }

  protected def createConnectionFactory(url: AmqpQueueUrl) = {
    val cf = new ConnectionFactory()
    cf.setHost(url.host)
    cf.setPort(url.port)
    cf.setUsername(url.username)
    cf.setPassword(url.password)
    if (! url.isDefaultVHost) {
      cf.setVirtualHost(url.vHost)
    }
    cf
  }

  protected def createConnection(cf: ConnectionFactory) =
    new AccentConnection(cf, new AccentConnectionFailureHandler)

  protected def createConsumer(connection: AccentConnection, queue: String, domain: String, endpoint: String) = {
    val params = new ReceiverParameters(queue)
    new AccentReceiver(connection,
                       params,
                       domain,
                       endpoint,
                       changes)
  }

  private def getConsumersByUrl(url: AmqpQueueUrl): Consumers = {
    val connectionKey = ConnectionKey.fromUrl(url)
    consumers.getOrElseUpdate(connectionKey, {
      val connection = createConnection(createConnectionFactory(url))
      new Consumers(connection, connectionKey)
    })
  }

  private def getConsumersByEndpoint(endpointName: String): Option[Consumers] = {
    consumers.values.find(_.contains(endpointName))
  }
}