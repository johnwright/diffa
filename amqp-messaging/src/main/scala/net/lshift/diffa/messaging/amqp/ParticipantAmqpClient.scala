/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.messaging.amqp

import com.rabbitmq.messagepatterns.unicast.Connector
import net.lshift.diffa.messaging.json.JSONEncodingUtils._
import net.lshift.diffa.kernel.frontend.wire.{ActionInvocation, InvocationResult, WireAggregateRequest, WireDigest, WireResponse}
import net.lshift.diffa.kernel.participants._
import scala.collection.JavaConversions._

/**
 * Base class for RPC clients used for participant communication using JSON over AMQP.
 */
abstract class ParticipantAmqpClient(connector: Connector,
                                     queueName: String,
                                     timeout: Long)
  
  extends AmqpRpcClient(connector, queueName)
  with Participant {

  def queryAggregateDigests(buckets: Map[String, CategoryFunction], constraints: Seq[QueryConstraint]): Seq[AggregateDigest] = {
    val wireBuckets = buckets.map { case (name, cf) => name -> cf.name }.toMap
    val wireConstraints = constraints.map(_.wireFormat).toList

    ((serializeWireAggregateRequest _)
      andThen (call("query_aggregate_digests", _, timeout))
      andThen (deserializeDigests _)
      andThen (digestsFromWire[Seq[AggregateDigest]]_)
      apply (WireAggregateRequest(wireBuckets, wireConstraints)))
  }

  def queryEntityVersions(constraints: Seq[QueryConstraint]): Seq[EntityVersion] = (
    (constraintsToWire _)
     andThen (serializeQueryConstraints _)
     andThen (call("query_entity_versions", _, timeout))
     andThen (deserializeDigests _)
     andThen (digestsFromWire[Seq[EntityVersion]] _)
     apply (constraints)
  )

  def invoke(actionId: String, entityId: String): InvocationResult = (
    (serializeActionRequest _)
     andThen (call("invoke", _, timeout))
     andThen (deserializeActionResult _)
     apply (ActionInvocation(actionId, entityId))
  )

  def retrieveContent(identifier: String): String = (
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

/**
 * RPC client wrapper for a DownstreamParticipant using JSON over AMQP.
 */
class DownstreamParticipantAmqpClient(connector: Connector,
                                      queueName: String,
                                      timeout: Long)

  extends ParticipantAmqpClient(connector, queueName, timeout)
  with DownstreamParticipant {

  def generateVersion(entityBody: String): ProcessingResponse = (
    (serializeEntityBodyRequest _)
     andThen (call("generate_version", _, timeout))
     andThen (deserializeWireResponse _)
     andThen (WireResponse.fromWire _)
     apply (entityBody)
  )
}

/**
 * RPC client wrapper for an UpstreamParticipant using JSON over AMQP.
 */
class UpstreamParticipantAmqpClient(connector: Connector,
                                    queueName: String,
                                    timeout: Long)

  extends ParticipantAmqpClient(connector, queueName, timeout)
  with UpstreamParticipant {}
