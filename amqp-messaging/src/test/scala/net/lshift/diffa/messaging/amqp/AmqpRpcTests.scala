package net.lshift.diffa.messaging.amqp

import org.junit.Assert._
import org.junit.{BeforeClass, Test}
import com.rabbitmq.client.ConnectionFactory

class AmqpRpcTests {

  val connectionFactory = new ConnectionFactory()
  val connector = new ConnectorHolder(connectionFactory).connector

  @Test
  def pingPong() {
    val queueName = "testQueue"

    val server = new AmqpRpcServer(connector, queueName, new RpcRequestHandler {
      def handleRequest(methodName: String, argument: String) = (methodName, argument) match {
        case ("ping", "somedata") => "pong"
        case (method, argument)   => fail; "Unexpected request: %s(%s)".format(method, argument)
      }
    })

    val client = new AmqpRpcClient(connector, queueName)

    server.start()
    assertEquals("pong", client.call("ping", "somedata", 1000))
    server.close()
    client.close()
  }

  @Test
  def exceptions() {
    val queueName = "testQueue"

    val server = new AmqpRpcServer(connector, queueName, new RpcRequestHandler {
      def handleRequest(methodName: String, argument: String) =
        throw new IllegalStateException("dummy exception message")
    })

    val client = new AmqpRpcClient(connector, queueName)

    server.start()
    try {
      client.call("foo", "bar", 1000)
      fail
    } catch {
      case AmqpRemoteException(methodName, message) =>
        assertEquals("foo", methodName)
        assertEquals("java.lang.IllegalStateException: dummy exception message", message)
    }
  }

  @Test
  def bogusRequest() {
    fail
  }
}