package net.lshift.diffa.messaging.amqp

object AmqpRpc {
  val encoding = "UTF-8"
  val methodHeader = "rpc-method"
  val exceptionMessageHeader = "rpc-exception-message"
}