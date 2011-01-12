package net.lshift.diffa.messaging.amqp

case class MissingEndpointException(methodName: String) extends Exception {
  override def getMessage = "No endpoint to handle method with name: " + methodName
}