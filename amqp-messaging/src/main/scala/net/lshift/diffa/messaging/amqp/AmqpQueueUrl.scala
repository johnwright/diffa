package net.lshift.diffa.messaging.amqp

import scala.util.matching.Regex
import com.rabbitmq.client.ConnectionFactory.USE_DEFAULT_PORT

/**
 * Custom URL scheme for AMQP URLs. Extends existing AMQP URL scheme by adding a "queues" resource after the vhost section.
 */
case class AmqpQueueUrl(queue: String,
                        host: String = "localhost",
                        port: Int = USE_DEFAULT_PORT,
                        vHost: String = "") {

  private def portString = if (port == USE_DEFAULT_PORT) "" else ":%d".format(port)

  override def toString = "amqp://%s%s/%s/queues/%s".format(host,
                                                            portString,
                                                            vHost,
                                                            queue)

}

object AmqpQueueUrl {
  private val pattern = new Regex("""amqp://(.*?)(:(\d+))?/(.*?)/queues/(.*?)""")

  def parse(url: String) = {
    val pattern(host, _, port, vHost, queue) = url
    
    AmqpQueueUrl(queue,
                 host,
                 if (port != null) port.toInt else USE_DEFAULT_PORT,
                 vHost)
  }
}