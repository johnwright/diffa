package net.lshift.diffa.messaging.amqp

import com.rabbitmq.messagepatterns.unicast.Connector
import net.lshift.diffa.messaging.json.JSONEncodingUtils._
import net.lshift.diffa.kernel.frontend.wire.{ActionInvocation, InvocationResult, WireDigest, WireResponse}
import net.lshift.diffa.kernel.participants._

abstract class ParticipantAmqpClient(connector: Connector,
                                     queueName: String,
                                     defaultTimeout: Long)
  
  extends AmqpRpcClient(connector, queueName)
  with Participant {

  def queryAggregateDigests(constraints: Seq[QueryConstraint]): Seq[AggregateDigest] =
    queryAggregateDigests(constraints, defaultTimeout)

  def queryAggregateDigests(constraints: Seq[QueryConstraint], timeout: Long): Seq[AggregateDigest] = (
    (constraintsToWire _)
     andThen (serializeConstraints _)
     andThen (call("query_aggregate_digests", _, timeout))
     andThen (deserializeDigests _)
     andThen (digestsFromWire _)
     apply (constraints)
  )

  def queryEntityVersions(constraints: Seq[QueryConstraint]): Seq[EntityVersion] =
    queryEntityVersions(constraints, defaultTimeout)

  def queryEntityVersions(constraints: Seq[QueryConstraint], timeout: Long): Seq[EntityVersion] = (
    (constraintsToWire _)
     andThen (serializeConstraints _)
     andThen (call("query_entity_versions", _, timeout))
     andThen (deserializeDigests _)
     andThen (digestsFromWire _)
     apply (constraints)
  )

  def invoke(actionId: String, entityId: String): InvocationResult =
    invoke(actionId, entityId, defaultTimeout)

  def invoke(actionId: String, entityId: String, timeout: Long): InvocationResult = (
    (serializeActionRequest _)
     andThen (call("invoke", _, timeout))
     andThen (deserializeActionResult _)
     apply (ActionInvocation(actionId, entityId))
  )

  def retrieveContent(identifier: String): String =
    retrieveContent(identifier, defaultTimeout)

  def retrieveContent(identifier: String, timeout: Long): String = (
    (serializeEntityContentRequest _)
     andThen (call("retrieve_content", _, timeout))
     andThen (deserializeEntityContent _)
     apply (identifier)
  )

  private def constraintsToWire(constraints: Seq[QueryConstraint]) =
    constraints.map(_.wireFormat)

  private def digestsFromWire[T](digests: Seq[WireDigest]) =
    digests.map(WireDigest.fromWire _).asInstanceOf[T]
}

class DownstreamParticipantAmqpClient(connector: Connector,
                                      queueName: String,
                                      defaultTimeout: Long)

  extends ParticipantAmqpClient(connector, queueName, defaultTimeout)
  with DownstreamParticipant {


  def generateVersion(entityBody: String): ProcessingResponse =
    generateVersion(entityBody, defaultTimeout)

  def generateVersion(entityBody: String, timeout: Long): ProcessingResponse = (
    (serializeEntityContentRequest _)
     andThen (call("generate_version", _, timeout))
     andThen (deserializeWireResponse _)
     andThen (WireResponse.fromWire _)
     apply (entityBody)
  )
}

class UpstreamParticipantAmqpClient(connector: Connector,
                                    queueName: String,
                                    defaultTimeout: Long)

  extends ParticipantAmqpClient(connector, queueName, defaultTimeout)
  with UpstreamParticipant {}
