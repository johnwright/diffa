package net.lshift.diffa.messaging.amqp

import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import java.io.Closeable
import com.rabbitmq.messagepatterns.unicast.{Message, ChannelSetupListener, Connector, Factory}

class AmqpRpcServer(connector: Connector, queueName: String, handler: RpcRequestHandler)
  extends Closeable {

  private val log = LoggerFactory.getLogger(getClass)

  val receiveTimeout = 100
  val stopTimeout = 10000

  private val queueIsDurable = true
  private val queueIsExclusive = true
  private val queueIsAutoDelete = false

  @volatile private var running = false

  private val messaging = {
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
            val methodName = Option(msg.getProperties.getHeaders.get(AmqpRpc.methodHeader))
              .map(_.toString).getOrElse("")
            val argument = new String(msg.getBody, AmqpRpc.encoding)

            val replyHeaders = new java.util.HashMap[String, Object]
            replyHeaders.put(AmqpRpc.methodHeader, methodName)

            val response = try {
              handler.handleRequest(methodName, argument)
            } catch {
              case e: Exception =>
                replyHeaders.put(AmqpRpc.exceptionMessageHeader,
                                 "%s: %s".format(e.getClass.getName, e.getMessage))
                ""
            }
            val reply = msg.createReply()
            reply.getProperties.setHeaders(replyHeaders)
            reply.setBody(response.getBytes(AmqpRpc.encoding))
            messaging.send(reply)
          }
        } catch {
          case e: Exception => log.error("Failed to handle request: " + e.getMessage, e)
        }
      }
    }
  })

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