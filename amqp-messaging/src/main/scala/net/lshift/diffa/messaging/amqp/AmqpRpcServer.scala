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
import com.rabbitmq.messagepatterns.unicast.{ReceivedMessage, ChannelSetupListener, Connector, Factory}
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}

/**
 * Server for RPC-style communication over AMQP.
 */
class AmqpRpcServer(connector: Connector, queueName: String, handler: ProtocolHandler)
  extends AmqpConsumer(connector, queueName, RpcEndpointMapper, handler) {
  
  override protected def createResponse() = new AmqpTransportResponse()
  
  override protected def postHandle(msg: ReceivedMessage, request: TransportRequest, response: TransportResponse) {
    val replyHeaders = new java.util.HashMap[String, Object]
    replyHeaders.put(AmqpRpc.endpointHeader, request.endpoint)
    
    val (status, body) = response match {
      case r: AmqpTransportResponse => (r.status, r.os.toByteArray)
      case _                        => (AmqpRpc.defaultStatusCode, new Array[Byte](0))
    }
    replyHeaders.put(AmqpRpc.statusCodeHeader, status.asInstanceOf[AnyRef])
    
    val reply = msg.createReply()
    reply.getProperties.setHeaders(replyHeaders)
    reply.setBody(body)
    messaging.send(reply)
  }

}

object RpcEndpointMapper extends EndpointMapper {
  
  def apply(msg: ReceivedMessage) = {
    Option(msg.getProperties.getHeaders.get(AmqpRpc.endpointHeader))
      .map(_.toString).getOrElse("")
  }
}