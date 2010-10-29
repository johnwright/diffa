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
import collection.mutable.HashMap

/**
 * Rest client for participant communication.
 */
class ParticipantRestClient(root:String) extends AbstractRestClient(root, "") with Participant {

  // old: override def queryDigests(start:DateTime, end:DateTime, granularity:RangeGranularity) = {
  override def queryDigests(constraints:Seq[QueryConstraint]) : Seq[VersionDigest] = {
    val requestObj = new JSONObject
    // TODO [#2]
    //requestObj.put("start", start.toString(JSONEncodingUtils.dateEncoder))
    //requestObj.put("end", end.toString(JSONEncodingUtils.dateEncoder))
    //requestObj.put("granularity", jsonGranularity(granularity))

    executeRpc("query_digests", requestObj.toString) match {
      case Some(r) => {
        val jsonDigests = new JSONArray(r)
        (0 until jsonDigests.length).map(i => {
          val digestObj = jsonDigests.getJSONObject(i)
          VersionDigest(
            // TODO [#2]
            //digestObj.getString("key"), JSONEncodingUtils.dateParser.parseDateTime(digestObj.getString("date")),
            digestObj.getString("key"),
            new HashMap[String,String],
            JSONEncodingUtils.maybeParseableDate(digestObj.optString("lastUpdated")), digestObj.getString("digest"))
        }).toList
      }
      case None => List()
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

  private def jsonGranularity(gran:RangeGranularity) = {
    gran match {
      case IndividualGranularity => "individual"
      case DayGranularity => "day"
      case MonthGranularity => "month"
      case YearGranularity => "year"
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

        ProcessingResponse(
          responseObj.getString("id"),
          // TODO [#2]
          //JSONEncodingUtils.dateParser.parseDateTime(responseObj.getString("date")),
          new HashMap[String,String],
          responseObj.getString("uvsn"), responseObj.getString("dvsn"))
      }
    }
  }
}