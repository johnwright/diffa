package net.lshift.diffa.agent.itest.support

import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory

object AmqpConnectionChecker {

  val log = LoggerFactory.getLogger(AmqpConnectionChecker.getClass)

  val isConnectionAvailable = try {
    log.info("Checking for AMQP connection")
    val cf = new ConnectionFactory
    val conn = cf.newConnection()

    log.info("AMQP server properties: " + conn.getServerProperties)
    try {
      conn.close
    } catch {
      case e =>
        log.error("Couldn't close AMQP connection after checking", e)
    }

    true
  } catch {
    case e =>
      log.info("No AMQP connection found")
      false
  }
}