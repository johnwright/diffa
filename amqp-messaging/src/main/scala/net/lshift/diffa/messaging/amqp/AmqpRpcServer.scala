package net.lshift.diffa.messaging.amqp

import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import com.rabbitmq.messagepatterns.unicast.{Message, ChannelSetupListener, Connector, Factory}
import java.io.{ByteArrayInputStream, Closeable}
import net.lshift.diffa.kernel.protocol.{TransportResponse, TransportRequest, ProtocolHandler}

class AmqpRpcServer(connector: Connector, queueName: String, handler: ProtocolHandler)
  extends Closeable {

  private val log = LoggerFactory.getLogger(getClass)

  val receiveTimeout = 100
  val stopTimeout = 10000

  private val queueIsDurable = true
  private val queueIsExclusive = false
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