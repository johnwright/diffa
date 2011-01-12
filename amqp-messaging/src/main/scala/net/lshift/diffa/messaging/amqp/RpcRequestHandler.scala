package net.lshift.diffa.messaging.amqp

trait RpcRequestHandler {

  def handleRequest(methodName: String, argument: String): String
}