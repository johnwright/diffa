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

class AmqpRpcTests {

  assumeTrue(AmqpConnectionChecker.isConnectionAvailable)

  def extract(req: TransportRequest): (String, String) = {
    (req.endpoint, IOUtils.toString(req.is))
  }

  @Test
  def pingPong() {
    val queueName = "testQueue"
    val holder = new ConnectorHolder()

    val server = new AmqpRpcServer(holder.connector, queueName, new ProtocolHandler {
      val contentType = "text/plain"

      def handleRequest(req: TransportRequest, res: TransportResponse) = extract(req) match {
        case ("ping", "somedata") => res.withOutputStream(_.write("pong".getBytes)); true
        case (endpoint, payload)   => fail("Unexpected request: %s(%s)".format(endpoint, payload)); true
      }
    })

    val client = new AmqpRpcClient(holder.connector, queueName)

    server.start()
    assertEquals("pong", client.call("ping", "somedata", 1000))
    server.close()
    client.close()
    holder.close()
  }

  @Test
  def receiveErrorCode() {
    val queueName = "testQueue"
    val holder = new ConnectorHolder()

    val server = new AmqpRpcServer(holder.connector, queueName, new ProtocolHandler {
      val contentType = "text/plain"
      
      def handleRequest(req: TransportRequest, res: TransportResponse) = {
        res.setStatusCode(400); true
      }
    })

    val client = new AmqpRpcClient(holder.connector, queueName)

    server.start()
    try {
      client.call("foo", "bar", 1000)
      fail
    } catch {
      case AmqpRemoteException(endpoint, statusCode) =>
        assertEquals("foo", endpoint)
        assertEquals(400, statusCode)
    }
    server.close()
    client.close()
    holder.close()
  }

  @Test
  def emptyResponse() {
    val queueName = "testQueue"
    val holder = new ConnectorHolder()

    val server = new AmqpRpcServer(holder.connector, queueName, new ProtocolHandler {
      val contentType = "text/plain"

      def handleRequest(req: TransportRequest, res: TransportResponse) = true
    })

    val client = new AmqpRpcClient(holder.connector, queueName)

    server.start()
    assertEquals("", client.call("foo", "bar", 1000))

    server.close()
    client.close()
    holder.close()
  }
}