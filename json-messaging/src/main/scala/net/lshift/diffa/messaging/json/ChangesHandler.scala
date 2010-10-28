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

import net.lshift.diffa.kernel.frontend.Changes
import org.codehaus.jettison.json.JSONObject
import net.lshift.diffa.kernel.events.{UpstreamChangeEvent, DownstreamChangeEvent, DownstreamCorrelatedChangeEvent}

/**
 * JSON protocol handler for change requests.
 */

class ChangesHandler(val frontend:Changes) extends AbstractJSONHandler {

  protected val endpoints = Map(
    "changes" -> defineOnewayRpc((s:String) => s)(s => {
      val jObj = new JSONObject(s)
      val endpoint = jObj.getString("endpoint")
      val id = jObj.getString("id")
      val date = JSONEncodingUtils.dateParser.parseDateTime(jObj.getString("date"))
      val lastUpdated = JSONEncodingUtils.maybeParseableDate(jObj.optString("lastUpdated"))

      val evt = jObj.getString("type") match {
        case "upstream" => new UpstreamChangeEvent(endpoint, id, date, lastUpdated, jObj.optString("vsn", null))
        case "downstream-same" => new DownstreamChangeEvent(endpoint, id, date, lastUpdated, jObj.optString("vsn", null))
        case "downstream-correlated" =>
          new DownstreamCorrelatedChangeEvent(endpoint, id, date, lastUpdated, jObj.optString("uvsn", null), jObj.optString("dvsn", null))
      }

      frontend.onChange(evt)
    })
  )
}