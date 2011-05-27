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

import com.rabbitmq.messagepatterns.unicast.{ChannelSetupListener, Connector, Factory, ReceivedMessage}
import org.slf4j.LoggerFactory
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}
import com.rabbitmq.client.{ShutdownSignalException, Channel}
import java.io.{IOException, OutputStream, ByteArrayInputStream, Closeable}


/**
 * AMQP message consumer. Uses a ProtocolHandler to process messages as TransportRequests,
 * and produces no response.
 */
class AmqpConsumer(connector: Connector,
                   queueName: String,
                   endpointMapper: EndpointMapper,
                   handler: ProtocolHandler)
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
        log.debug("Declaring queue: %s".format(queueName))
        channel.queueDeclare(queueName, queueIsDurable, queueIsExclusive, queueIsAutoDelete, null)
      }
    })
    m
  }

  val worker = new Thread(new Runnable {
    def run {
      while (running) {
        try {
          val msg = messaging.receive(receiveTimeout)
          if (msg != null) {
            try {
              handleMessage(msg)
              messaging.ack(msg)
            }
            catch {
              // Re-throw any exceptions that originate from the AMQP libraries
              case s:ShutdownSignalException => throw s
              case o:IOException => throw o
              case e:Exception => {
                messaging.ack(msg)
                throw e
              }
            }
          }
        } catch {
          case s: ShutdownSignalException =>
            log.info("Received shutdown signal, shutting down...")
            running = false
            close()
            log.info("Shutdown complete.")
          case e: Exception =>
            log.error("Failed to handle request: " + e.getMessage, e)
        }
      }
    }
  })

  protected def handleMessage(msg: ReceivedMessage) {
    val endpoint = endpointMapper(msg)
    val request = new TransportRequest(endpoint, new ByteArrayInputStream(msg.getBody))
    val response = createResponse()
    handler.handleRequest(request, response)
    postHandle(msg, request, response)
  }

  /**
   * Creates a do-nothing response by default.
   */
  protected def createResponse() = new TransportResponse {
    def setStatusCode(status: Int) {}
    def withOutputStream(f:(OutputStream) => Unit) {}
  }

  /**
   * Does nothing by default.
   */
  protected def postHandle(msg: ReceivedMessage, request: TransportRequest, response: TransportResponse) {}

  def start() {
    messaging.init()
    running = true
    worker.start()
  }

  def close() {
    if (running) {
      messaging.cancel()
      messaging.close()
      running = false
    }
    worker.join(stopTimeout)
  }
}
