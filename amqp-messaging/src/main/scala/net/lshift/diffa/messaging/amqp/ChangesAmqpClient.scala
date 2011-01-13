package net.lshift.diffa.messaging.amqp

import com.rabbitmq.messagepatterns.unicast.Connector
import net.lshift.diffa.kernel.client.ChangesClient
import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.frontend.wire.WireEvent
import net.lshift.diffa.messaging.json.JSONEncodingUtils._

class ChangesAmqpClient(connector: Connector,
                        queueName: String,
                        defaultTimeout: Long)

  extends AmqpRpcClient(connector, queueName)
  with ChangesClient {

  def onChangeEvent(evt: ChangeEvent) {
    onChangeEvent(evt, defaultTimeout)
  }

  def onChangeEvent(evt: ChangeEvent, timeout: Long) {
    val wire = evt match {
      case us: UpstreamChangeEvent              => WireEvent.toWire(us)
      case ds: DownstreamChangeEvent            => WireEvent.toWire(ds)
      case dsc: DownstreamCorrelatedChangeEvent => WireEvent.toWire(dsc)
    }
    call("changes", serializeEvent(wire), timeout)
  }
}
