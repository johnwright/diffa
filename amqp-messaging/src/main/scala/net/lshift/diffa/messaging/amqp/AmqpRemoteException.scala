package net.lshift.diffa.messaging.amqp

case class AmqpRemoteException(endpoint: String,
                               statusCode: Int)
  extends RuntimeException {

  override def getMessage = "Received error code [%s] from call to RPC endpoint [%s]"
    .format(statusCode, endpoint)

  override def toString = getMessage
}