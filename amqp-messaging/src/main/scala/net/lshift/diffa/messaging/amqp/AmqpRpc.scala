package net.lshift.diffa.messaging.amqp

object AmqpRpc {
  val encoding = "UTF-8"
  val endpointHeader = "rpc-endpoint"
  val statusCodeHeader = "rpc-status-code"
  val defaultStatusCode = 200
}