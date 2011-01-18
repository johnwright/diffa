package net.lshift.diffa.messaging.amqp

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.messagepatterns.unicast.{Factory, ConnectionBuilder}

case class ConnectorHolder(connectionFactory: ConnectionFactory = new ConnectionFactory()) {

  private lazy val builder = new ConnectionBuilder {
    def createConnection = connectionFactory.newConnection()
  }

  lazy val connector = Factory.createConnector(builder)

  def close() {
    connector.close()
  }
}