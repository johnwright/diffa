package net.lshift.diffa.messaging.amqp

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.ConnectionFactory._
import com.rabbitmq.messagepatterns.unicast.{Factory, ConnectionBuilder}

class ConnectorHolder(host: String = DEFAULT_HOST,
                      port: Int = USE_DEFAULT_PORT,
                      virtualHost: String = DEFAULT_VHOST,
                      user: String = DEFAULT_USER,
                      password: String = DEFAULT_PASS) {

  val connectionFactory = {
    val cf = new ConnectionFactory()
    cf.setHost(host)
    cf.setPort(port)
    cf.setVirtualHost(virtualHost)
    cf.setUsername(user)
    cf.setPassword(password)
    cf
  }

  private lazy val builder = new ConnectionBuilder {
    def createConnection = connectionFactory.newConnection()
  }

  lazy val connector = Factory.createConnector(builder)

  def close() {
    connector.close()
  }
}