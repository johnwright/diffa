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

import com.rabbitmq.messagepatterns.unicast.{ChannelSetupListener, Connector, Factory}
import com.rabbitmq.client.Channel
import java.io.{Closeable, IOException}
import java.util.UUID
import org.slf4j.LoggerFactory

/**
 * Client for RPC-style communication over AMQP.
 */
class AmqpRpcClient(connector: Connector, queueName: String)
  extends Closeable {

  val defaultTimeout = 30000

  private val log = LoggerFactory.getLogger(getClass)

  protected var replyQueueName: Option[String] = None

  protected def nextMessageId() = UUID.randomUUID.toString

  private val messaging = {
    val m = Factory.createMessaging()
    m.setConnector(connector)
    m.setExchangeName("")

    // set up a reply queue
    m.addReceiverSetupListener(new ChannelSetupListener {
      def channelSetup(channel: Channel) {
        replyQueueName = Some(channel.queueDeclare().getQueue)
        m.setQueueName(replyQueueName.get)
      }
    })
    // The implementation checks whether the queue name is null at initialization time,
    // so unfortunately this must be set to a temporary junk value to avoid triggering an
    // exception. It'll be replaced with the proper value when the channel is set up.
    m.setQueueName("temp")
    m.init()
    m
  }

  def call(endpoint: String, payload: String, timeout: Long = defaultTimeout): String = {
    if (log.isDebugEnabled) {
      log.debug("%s: %s".format(endpoint, payload))
    }

    val msg = messaging.createMessage()
    msg.setRoutingKey(queueName)
    if (replyQueueName.isEmpty)
      throw new IllegalStateException("Reply queue not set up!")

    msg.setReplyTo(replyQueueName.get)
    val headers = new java.util.HashMap[String, Object]
    headers.put(AmqpRpc.endpointHeader, endpoint)
    msg.getProperties.setHeaders(headers)
    msg.setBody(payload.getBytes(AmqpRpc.encoding))
    msg.setMessageId(nextMessageId())
    messaging.send(msg)

    val endTime = System.currentTimeMillis + timeout

    while (System.currentTimeMillis < endTime) {
      val remainingTime = endTime - System.currentTimeMillis
      val reply = messaging.receive(if (remainingTime < 0) 0 else remainingTime)
      if (reply == null)
        throw new ReceiveTimeoutException(timeout)

      // if the reply doesn't have the right correlation ID, loop
      if (reply.getCorrelationId == msg.getMessageId) {
        val statusCode = try {
          reply.getProperties.getHeaders.get(AmqpRpc.statusCodeHeader).toString.toInt
        } catch {
          case e => throw new MissingResponseCodeException(e)
        }
        if (statusCode != AmqpRpc.defaultStatusCode) {
          throw new AmqpRemoteException(endpoint, statusCode)
        } else {
          return new String(reply.getBody, AmqpRpc.encoding)
        }
      } else {
        log.warn("Skipping over message with correlation ID [%s], expected message with ID [%s]"
                   .format(reply.getCorrelationId, msg.getMessageId))
      }
    }
    throw new ReceiveTimeoutException(timeout)
  }

  def close() {
    messaging.close()
  }
}