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
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.DateRangeConstraint

/**
 * Handler for participants being queried via JSON.
 */
abstract class ParticipantHandler(val participant:Participant) extends AbstractJSONHandler {
  protected val commonEndpoints = Map(
    "query_digests" -> defineRpc((s:String) => s)(req => {
      val reqObj = new JSONObject(req)

      // TODO [#2] parse properly
//      val digests = participant.queryDigests(
//        JSONEncodingUtils.dateParser.parseDateTime(reqObj.getString("start")),
//        JSONEncodingUtils.dateParser.parseDateTime(reqObj.getString("end")),
//        decodeGranularity(reqObj.getString("granularity")))
      val digests = participant.queryDigests(List(DateRangeConstraint.any))

      val resultObj = new JSONArray
      digests foreach (digest => {
        val digestObj = new JSONObject
        digestObj.put("key", digest.key)
        // TODO [#2]
        //digestObj.put("date", digest.date.toString(JSONEncodingUtils.dateEncoder))
        digestObj.put("lastUpdated", JSONEncodingUtils.maybeDateStr(digest.lastUpdated))
        digestObj.put("digest", digest.digest)
        resultObj.put(digestObj)
      })
      resultObj.toString
    }),
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

  private def decodeGranularity(gran:String) = {
    gran match {
      case "individual" => IndividualGranularity
      case "day" => DayGranularity
      case "month" => MonthGranularity
      case "year" => YearGranularity
    }
  }
}

class DownstreamParticipantHandler(val downstream:DownstreamParticipant)
        extends ParticipantHandler(downstream) {

  override protected val endpoints = commonEndpoints ++ Map(
    "generate_version" -> defineRpc((s:String) => s)(req => {
      val reqObj = new JSONObject(req)
      val response = downstream.generateVersion(reqObj.getString("entityBody"))

      val responseObj = new JSONObject
      responseObj.put("id", response.id)
      // TODO [#2]
      //responseObj.put("date", response.date.toString(JSONEncodingUtils.dateEncoder))
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