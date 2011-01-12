package net.lshift.diffa.messaging.amqp

import com.rabbitmq.messagepatterns.unicast.{ChannelSetupListener, Connector, Factory}
import com.rabbitmq.client.Channel
import java.io.{Closeable, IOException}

class AmqpRpcClient(connector: Connector, queueName: String)
  extends Closeable {

  val defaultTimeout = 30000

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

  def call(methodName: String, argument: String, timeout: Long = defaultTimeout) = {
    val msg = messaging.createMessage()
    msg.setRoutingKey(queueName)
    if (replyQueueName.isEmpty)
      throw new IllegalStateException("Reply queue not set up!")
    msg.setReplyTo(replyQueueName.get)
    val headers = new java.util.HashMap[String, Object]
    headers.put(AmqpRpc.methodHeader, methodName)
    msg.getProperties.setHeaders(headers)
    msg.setBody(argument.getBytes(AmqpRpc.encoding))
    messaging.send(msg)

    val reply = messaging.receive(timeout)
    if (reply == null)
      // TODO TimeoutException
      throw new IOException("AMQP RPC timeout")

    val exceptionMessage = Option(reply.getProperties.getHeaders.get(AmqpRpc.exceptionMessageHeader))
    if (exceptionMessage.isDefined) {
      throw new AmqpRemoteException(methodName,
                                    exceptionMessage.map(_.toString).getOrElse(""))
    } else {
      new String(reply.getBody, AmqpRpc.encoding)
    }
  }

  def close() {
    messaging.close()
  }
}