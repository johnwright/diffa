package net.lshift.diffa.messaging.amqp

import java.io.IOException

case class ReceiveTimeoutException(timeout: Long) extends IOException {

  override def getMessage = "Receive timed out after %dms".format(timeout)
}