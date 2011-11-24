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

import org.apache.commons.io.IOUtils
import org.junit.Assert._
import org.junit.Assume.assumeTrue
import org.junit.Test
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}
import net.lshift.accent.AccentConnection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.AMQP.BasicProperties
import com.eaio.uuid.UUID
import net.lshift.diffa.messaging.amqp.ReceiverParameters._

/**
 * Test cases for fire-and-forget AMQP messaging.
 */
class AmqpProducerConsumerTests {

  assumeTrue(AmqpConnectionChecker.isConnectionAvailable)

  val factory = new ConnectionFactory()
  val failureHandler = new AccentConnectionFailureHandler()

  @Test
  def fireAndForget() {
    val con = new AccentConnection(factory, failureHandler)

    val monitor = new Object
    var messageProcessed = false

    val params = new ReceiverParameters(new UUID().toString) {
      autoDelete = true;
    }

    new AccentReceiver(con, params, new EndpointMapper { def apply(props:BasicProperties) = "" }, new ProtocolHandler {
      val contentType = "text/plain"

      def handleRequest(req: TransportRequest, res: TransportResponse) = {
        assertEquals("expected payload", IOUtils.toString(req.is))
        messageProcessed = true
        monitor.synchronized {
          monitor.notifyAll
        }
        true
      }
    })
    
    val producer = new AccentSender(con, params.queueName)
    producer.send("expected payload")

    monitor.synchronized {
      monitor.wait(5000)
    }

    assertTrue(messageProcessed)
  }

  @Test(timeout = 5000)
  def applicationExceptionsShouldBeAcked() = {

    val con = new AccentConnection(factory, failureHandler)
    val monitor = new Object

    val params = new ReceiverParameters(queueName = new UUID().toString) {
      autoDelete = true;
    }

    val receiver = new AccentReceiver(con, params, new EndpointMapper { def apply(props:BasicProperties) = "" }, new ProtocolHandler {
      val contentType = "text/plain"

      def handleRequest(req: TransportRequest, res: TransportResponse) = {
        monitor.synchronized {
          monitor.notifyAll()
        }
        throw new Exception("Deliberate exception")
      }
    })

    val producer = new AccentSender(con, params.queueName)
    producer.send("some payload")

    monitor.synchronized {
      monitor.wait(1000)
    }
    
    receiver.close()

    val channel = factory.newConnection().createChannel()
    val response = channel.basicGet(params.queueName, true)
    assertNull(response.getBody)
    assertEquals(0, response.getMessageCount)

  }
}