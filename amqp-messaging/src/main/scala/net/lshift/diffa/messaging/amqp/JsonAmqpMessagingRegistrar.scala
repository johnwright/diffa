package net.lshift.diffa.messaging.amqp

import net.lshift.diffa.kernel.frontend.Changes
import net.lshift.diffa.kernel.participants.ParticipantFactory
import net.lshift.diffa.kernel.protocol.ProtocolMapper
import net.lshift.diffa.messaging.json.ChangesHandler

class JsonAmqpMessagingRegistrar(connectorHolder: ConnectorHolder,
                                 protocolMapper: ProtocolMapper,
                                 participantFactory: ParticipantFactory,
                                 changes: Changes) {

  // Register the outbound participant factory for JSON/AMQP
  val factory = new JsonAmqpParticipantProtocolFactory(connectorHolder)
  participantFactory.registerFactory(factory)

  // Register a handler so requests made to the /changes endpoint on the agent with inbound content types of
  // application/json are decoded by our ChangesHandler.
  //protocolMapper.registerHandler("changes", new ChangesHandler(changes))
}