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

import com.rabbitmq.messagepatterns.unicast.{ChannelSetupListener, Connector, Factory, ReceivedMessage}
import org.slf4j.LoggerFactory
import com.rabbitmq.client.Channel
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}
import java.io.{OutputStream, ByteArrayInputStream, Closeable}


/**
 * AMQP message consumer. Uses a ProtocolHandler to process messages as TransportRequests,
 * and produces no response.
 */
class AmqpConsumer(connector: Connector, queueName: String, handler: ProtocolHandler)
  extends Closeable {

  protected val log = LoggerFactory.getLogger(getClass)

  val receiveTimeout = 100
  val stopTimeout = 10000
  
  private val queueIsDurable = true
  private val queueIsExclusive = false
  private val queueIsAutoDelete = false

  @volatile private var running = false

  protected val messaging = {
    val m = Factory.createMessaging()
    m.setConnector(connector)
    m.setQueueName(queueName)
    m.setExchangeName("")
    m.addSetupListener(new ChannelSetupListener {
      def channelSetup(channel: Channel) {
        channel.queueDeclare(queueName, queueIsDurable, queueIsExclusive, queueIsAutoDelete, null)
      }
    })
    m
  }

  // TODO handle shutdown signal
  val worker = new Thread(new Runnable {
    def run {
      while (running) {
        try {
          val msg = messaging.receive(receiveTimeout)
          if (msg != null) {
            messaging.ack(msg)

            handleMessage(msg)
          }
        } catch {
          case e: Exception => log.error("Failed to handle request: " + e.getMessage, e)
        }
      }
    }
  })

  protected def handleMessage(msg: ReceivedMessage) {
    // N.B. default endpoint is the queue name
    val request = new TransportRequest(queueName, new ByteArrayInputStream(msg.getBody))
    // pass in a do-nothing response
    val response = new TransportResponse {
      def setStatusCode(status: Int) {}
      def withOutputStream(f:(OutputStream) => Unit) {}
    }
    handler.handleRequest(request, response)
  }

  def start() {
    messaging.init()
    running = true
    worker.start()
  }

  def close() {
    messaging.cancel()
    running = false
    worker.join(stopTimeout)
    messaging.close()
  }
}