package net.lshift.diffa.messaging.amqp

import com.rabbitmq.messagepatterns.unicast.{ChannelSetupListener, Connector, Factory}
import com.rabbitmq.client.Channel
import java.io.{Closeable, IOException}
import org.slf4j.LoggerFactory

class AmqpRpcClient(connector: Connector, queueName: String)
  extends Closeable {

  val defaultTimeout = 30000

  private val log = LoggerFactory.getLogger(getClass)

  private var replyQueueName: Option[String] = None

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
    m.setQueueName("temp")// will be replaced when the channel is set up
    m.init()
    m
  }

  def call(endpoint: String, payload: String, timeout: Long = defaultTimeout) = {
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
    messaging.send(msg)

    val reply = messaging.receive(timeout)
    if (reply == null)
      throw new ReceiveTimeoutException(timeout)

    val statusCode = try {
      reply.getProperties.getHeaders.get(AmqpRpc.statusCodeHeader).toString.toInt
    } catch {
      case e => throw new MissingResponseCodeException(e)
    }
    if (statusCode != AmqpRpc.defaultStatusCode) {
      throw new AmqpRemoteException(endpoint, statusCode)
    } else {
      new String(reply.getBody, AmqpRpc.encoding)
    }
  }

  def close() {
    messaging.close()
  }
}