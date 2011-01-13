package net.lshift.diffa.messaging.amqp

import org.apache.commons.io.IOUtils
import org.junit.Assert._
import org.junit.Test
import com.rabbitmq.client.ConnectionFactory
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}

class AmqpRpcTests {

  val connectionFactory = new ConnectionFactory()

  def extract(req: TransportRequest): (String, String) = {
    (req.endpoint, IOUtils.toString(req.is))
  }

  @Test
  def pingPong() {
    val queueName = "testQueue"
    val holder = new ConnectorHolder(connectionFactory)

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
    val holder = new ConnectorHolder(connectionFactory)

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
    val holder = new ConnectorHolder(connectionFactory)

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