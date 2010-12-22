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
import JSONEncodingUtils._
import collection.mutable.ListBuffer
import net.lshift.diffa.kernel.frontend.WireResponse._
import net.lshift.diffa.kernel.frontend.{WireDigest, ActionInvocation, InvocationResult}

/**
 * Rest client for participant communication.
 */
class ParticipantRestClient(root:String) extends AbstractRestClient(root, "") with Participant {

  override def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion] = {
    executeRpc("query_entity_versions", pack(constraints)) match {
      case Some(r) => unpack(deserializeDigests(r))
      case None    => Seq()
    }
  }

  override def queryAggregateDigests(constraints:Seq[QueryConstraint]) : Seq[AggregateDigest] = {
    executeRpc("query_aggregate_digests", pack(constraints)) match {
      case Some(r) => unpack(deserializeDigests(r))
      case None    => Seq()
    }
  }

  override def retrieveContent(identifier: String): String = {    
    executeRpc("retrieve_content", serializeEntityContentRequest(identifier)) match {
      case Some(r) => {
        val content = deserializeEntityContent(r)
        if (content.equals("")) {
          log.warn("Returning default value for id: " + identifier)
        }
        content
      }
      case None => ""
    }
  }

  override def invoke(actionId:String, entityId:String) : InvocationResult = {
    executeRpc("invoke", serializeActionRequest(ActionInvocation(actionId, entityId))) match {
      case Some(r) => deserializeActionResult(r)
      case None    => null
    }
  }

  private def pack(seq:Seq[QueryConstraint]) = serialize(seq.map(_.wireFormat))
  private def unpack[T](seq:Seq[WireDigest]) = seq.map(WireDigest.fromWire(_).asInstanceOf[T])
}

/**
 * HTTP Client to UpstreamTestParticipant using JSON over HTTP.
 */
class UpstreamParticipantRestClient(root:String) extends ParticipantRestClient(root) with UpstreamParticipant {}

/**
 * HTTP Client to DownstreamTestParticipant using protocol-buffers over HTTP.
 */
class DownstreamParticipantRestClient(root:String) extends ParticipantRestClient(root)
        with DownstreamParticipant {
  override def generateVersion(entityBody: String): ProcessingResponse = {    
    executeRpc("generate_version", serializeEntityBodyRequest(entityBody)) match {
      case None    => null
      case Some(r) => fromWire(deserializeWireResponse(r))
    }
  }
}