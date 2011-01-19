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

import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import java.io.{ByteArrayInputStream, Closeable}
import net.lshift.diffa.kernel.protocol.{TransportRequest, ProtocolHandler}
import com.rabbitmq.messagepatterns.unicast.{ReceivedMessage, ChannelSetupListener, Connector, Factory}

/**
 * Server for RPC-style communication over AMQP.
 */
class AmqpRpcServer(connector: Connector, queueName: String, handler: ProtocolHandler)
  extends AmqpConsumer(connector, queueName, handler) {
  
  override protected def handleMessage(msg: ReceivedMessage) {
    val endpoint = Option(msg.getProperties.getHeaders.get(AmqpRpc.endpointHeader))
      .map(_.toString).getOrElse("")

    val replyHeaders = new java.util.HashMap[String, Object]
    replyHeaders.put(AmqpRpc.endpointHeader, endpoint)

    if (log.isDebugEnabled) {
      log.debug("%s: %s".format(endpoint, new String(msg.getBody)))
    }

    val request = new TransportRequest(endpoint, new ByteArrayInputStream(msg.getBody))
    val response = new AmqpTransportResponse

    handler.handleRequest(request, response)
    replyHeaders.put(AmqpRpc.statusCodeHeader, response.status.asInstanceOf[AnyRef])

    val reply = msg.createReply()
    reply.getProperties.setHeaders(replyHeaders)
    reply.setBody(response.os.toByteArray)
    messaging.send(reply)
  }

}