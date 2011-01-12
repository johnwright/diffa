package net.lshift.diffa.messaging.amqp

case class AmqpRemoteException(methodName: String,
                               exceptionMessage: String)
  extends Exception {

  override def getMessage = exceptionMessage

  override def toString = "Exception thrown calling RPC method [%s]: %s"
    .format(methodName, exceptionMessage)
}