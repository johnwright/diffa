package net.lshift.diffa.messaging.amqp

import java.io.{ByteArrayOutputStream, OutputStream}
import net.lshift.diffa.kernel.protocol.TransportResponse

class AmqpTransportResponse extends TransportResponse {

  var status = AmqpRpc.defaultStatusCode

  val os = new ByteArrayOutputStream()

  def setStatusCode(code: Int) { status = code }

  def withOutputStream(f: OutputStream => Unit) = f(os)

}