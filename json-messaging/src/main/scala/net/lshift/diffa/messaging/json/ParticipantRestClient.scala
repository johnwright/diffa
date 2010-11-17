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
import JSONEncodingUtils._
import collection.mutable.{ListBuffer, HashMap}

/**
 * Rest client for participant communication.
 */
class ParticipantRestClient(root:String) extends AbstractRestClient(root, "") with Participant {

  override def queryEntityVersions(constraints:Seq[QueryConstraint]) : Seq[EntityVersion] = {
    executeRpc("query_entity_versions", serialize(constraints)) match {
      case Some(r) => deserializeEntityVersions(r)
      case None    => Seq()
    }
  }

  override def queryAggregateDigests(constraints:Seq[QueryConstraint]) : Seq[AggregateDigest] = {
    executeRpc("query_aggregate_digests", serialize(constraints)) match {
      case Some(r) => deserializeAggregateDigest(r)
      case None    => Seq()
    }
  }

  override def retrieveContent(identifier: String): String = {
    val requestObj = new JSONObject
    requestObj.put("id", identifier)

    executeRpc("retrieve_content", requestObj.toString) match {
      case Some(r) => {
        val jsonResponse = new JSONObject(r)
        val s = jsonResponse.optString("content")
        if (s.equals("")) {
          log.warn("Returning default value for id: " + identifier)
        }
        s
      }
      case None => ""
    }
  }

  override def invoke(actionId:String, entityId:String) : ActionResult = {
    val request = new JSONObject
    request.put("actionId", actionId)
    request.put("entityId", entityId)
    executeRpc("invoke", request.toString) match {
      case Some(r) => {
        val json = new JSONObject(r)
        ActionResult(json.getString("result"),json.getString("output"))
      }
      case None    => null
    }
  }
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
    val requestObj = new JSONObject
    requestObj.put("entityBody", entityBody)

    executeRpc("generate_version", requestObj.toString) match {
      case None    => null
      case Some(r) => {
        val responseObj = new JSONObject(r)
        val attributes = new ListBuffer[String]
        val attributeArray = responseObj.getJSONArray("attributes")
        for (val i <- 0 to attributeArray.length - 1) {
          attributes += attributeArray.getString(i)
        }
        ProcessingResponse(
          responseObj.getString("id"),
          attributes,
          responseObj.getString("uvsn"), responseObj.getString("dvsn"))
      }
    }
  }
}