package net.lshift.diffa.messaging.amqp

import net.lshift.diffa.kernel.participants.ParticipantProtocolFactory

class JsonAmqpParticipantProtocolFactory(connectorHolder: ConnectorHolder,
                                         timeout: Long = 10000) extends ParticipantProtocolFactory {

  def supportsAddress(address: String, protocol: String) =
    protocol == "application/json" && address.startsWith("amqp://")

  def createUpstreamParticipant(address: String, protocol: String) =
    new UpstreamParticipantAmqpClient(connectorHolder.connector, AmqpQueueUrl.parse(address).queue, timeout)

  def createDownstreamParticipant(address: String, protocol: String) =
    new DownstreamParticipantAmqpClient(connectorHolder.connector, AmqpQueueUrl.parse(address).queue, timeout)
}