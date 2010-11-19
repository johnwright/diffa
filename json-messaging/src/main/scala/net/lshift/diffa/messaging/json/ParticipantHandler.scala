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
import org.codehaus.jettison.json.{JSONArray, JSONObject}
import scala.collection.JavaConversions._
import JSONEncodingUtils._
import net.lshift.diffa.kernel.frontend.ConstraintRegistry

/**
 * Handler for participants being queried via JSON.
 */
abstract class ParticipantHandler(val participant:Participant) extends AbstractJSONHandler {

  protected val commonEndpoints = Map(
    "query_aggregate_digests" -> skeleton(wire => serializeDigests(participant.queryAggregateDigests(unpack(wire)))),
    "query_entity_versions" -> skeleton(wire => serializeDigests(participant.queryEntityVersions(unpack(wire)))),
    "invoke" -> defineRpc((s:String) => s)(r => {
      val request = new JSONObject(r)
      val result = participant.invoke(request.getString("actionId"),request.getString("entityId"))
      val json = new JSONObject
      json.put("result", result.result)
      json.put("output", result.output)
      json.toString
    }),
    "retrieve_content" -> defineRpc((s:String) => s)(req => {
      val reqObj = new JSONObject(req)
      val content = participant.retrieveContent(reqObj.getString("id"))

      val responseObj = new JSONObject
      responseObj.put("content", content)
      responseObj.toString
    })
  )

  private def unpack(wire:String) = deserialize(wire).map(ConstraintRegistry.resolve(_))

  private def skeleton (f:String => String) = defineRpc((s:String) => s)(f(_))

}

class DownstreamParticipantHandler(val downstream:DownstreamParticipant)
        extends ParticipantHandler(downstream) {

  override protected val endpoints = commonEndpoints ++ Map(
    "generate_version" -> defineRpc((s:String) => s)(req => {
      val reqObj = new JSONObject(req)
      val response = downstream.generateVersion(reqObj.getString("entityBody"))

      val responseObj = new JSONObject
      responseObj.put("id", response.id)
      responseObj.put("attributes", asList(response.attributes))
      responseObj.put("uvsn", response.uvsn)
      responseObj.put("dvsn", response.dvsn)
      responseObj.toString
    })
  )
}

class UpstreamParticipantHandler(val upstream:UpstreamParticipant)
        extends ParticipantHandler(upstream) {
  override protected val endpoints = commonEndpoints
}