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

package net.lshift.diffa.messaging.amqp

import net.lshift.diffa.kernel.frontend.Changes
import net.lshift.diffa.kernel.protocol.ProtocolMapper
import net.lshift.diffa.messaging.json.ChangesHandler
import net.lshift.diffa.kernel.config.Endpoint
import collection.mutable.HashMap
import com.rabbitmq.messagepatterns.unicast.ReceivedMessage
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.participants.{EventFormatMapperManager, InboundEndpointFactory, InboundEndpointManager, ParticipantFactory}
import net.lshift.diffa.kernel.lifecycle.AgentLifecycleAware

/**
 * Utility class responsible for registering JSON over AMQP protocol support with necessary factories
 * and request handlers.
 */
class JsonAmqpMessagingRegistrar(connectorHolder: ConnectorHolder,
                                 inboundEndpointManager: InboundEndpointManager,
                                 protocolMapper: ProtocolMapper,
                                 participantFactory: ParticipantFactory,
                                 eventFormatMapperManager: EventFormatMapperManager,
                                 changes: Changes,
                                 timeoutMillis: Long)
  extends AgentLifecycleAware {

  private val log = LoggerFactory.getLogger(getClass)

  override def onAgentAssemblyCompleted {
    // Register the outbound participant factory for JSON/AMQP
    val factory = new JsonAmqpParticipantProtocolFactory(connectorHolder, timeoutMillis)
    participantFactory.registerFactory(factory)

    // Register the inbound changes handler
    inboundEndpointManager.registerFactory(new InboundEndpointFactory {

      val consumers = new HashMap[String, AmqpConsumer]

      // handler only has one endpoint, called "changes"
      object ChangesEndpointMapper extends EndpointMapper {
        def apply(msg: ReceivedMessage) = "changes"
      }

      def canHandleInboundEndpoint(inboundUrl: String, contentType: String) =
        inboundUrl.startsWith("amqp://") && eventFormatMapperManager.lookup(contentType).isDefined

      def ensureEndpointReceiver(e: Endpoint) {
        log.info("ensureEndpointReceiver: " + e)
        val eventFormatMapper = eventFormatMapperManager.lookup(e.inboundContentType).get
        val c = new AmqpConsumer(connectorHolder.connector,
                                 AmqpQueueUrl.parse(e.inboundUrl).queue,
                                 ChangesEndpointMapper,
                                 new ChangesHandler(changes, e.name, eventFormatMapper))
        consumers.put(e.name, c)
        c.start()
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
    })
  }
}