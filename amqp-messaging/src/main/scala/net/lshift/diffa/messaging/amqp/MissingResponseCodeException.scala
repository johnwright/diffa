package net.lshift.diffa.messaging.amqp

case class MissingResponseCodeException(cause: Throwable) extends RuntimeException {

  override def getCause = cause

  override def getMessage = cause.getMessage
}