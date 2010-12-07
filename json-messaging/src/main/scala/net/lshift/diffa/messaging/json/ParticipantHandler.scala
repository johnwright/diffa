/**
 * Copyright (C) 2010 LShift Ltd.
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

package net.lshift.diffa.messaging.json

import net.lshift.diffa.kernel.participants._
import scala.collection.JavaConversions._
import JSONEncodingUtils._
import net.lshift.diffa.kernel.frontend.ConstraintRegistry
import net.lshift.diffa.kernel.frontend.WireResponse._

/**
 * Handler for participants being queried via JSON.
 */
abstract class ParticipantHandler(val participant:Participant) extends AbstractJSONHandler {

  protected val commonEndpoints = Map(
    "query_aggregate_digests" -> skeleton(wire => serializeDigests(participant.queryAggregateDigests(unpack(wire)))),
    "query_entity_versions" -> skeleton(wire => serializeDigests(participant.queryEntityVersions(unpack(wire)))),
    "invoke" -> defineRpc((s:String) => s)(r => {
      val request = deserializeActionRequest(r)
      serializeActionResult(participant.invoke(request.actionId, request.entityId))
    }),
    "retrieve_content" -> defineRpc((s:String) => s)(req => {      
      serializeEntityContent(participant.retrieveContent(deserializeEntityContentRequest(req)))
    })
  )

  private def unpack(wire:String) = deserialize(wire).map(ConstraintRegistry.resolve(_))

  private def skeleton (f:String => String) = defineRpc((s:String) => s)(f(_))

}

class DownstreamParticipantHandler(val downstream:DownstreamParticipant)
        extends ParticipantHandler(downstream) {

  override protected val endpoints = commonEndpoints ++ Map(
    "generate_version" -> defineRpc((s:String) => s)(req => {
      val response = downstream.generateVersion(deserializeEntityBodyRequest(req))
      serializeWireResponse(toWire(response))
    })
  )
}

class UpstreamParticipantHandler(val upstream:UpstreamParticipant)
        extends ParticipantHandler(upstream) {
  override protected val endpoints = commonEndpoints
}