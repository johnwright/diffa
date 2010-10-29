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

import net.lshift.diffa.kernel.client.ChangesClient
import net.lshift.diffa.kernel.events.{UpstreamChangeEvent, DownstreamChangeEvent, DownstreamCorrelatedChangeEvent, ChangeEvent}
import org.codehaus.jettison.json.JSONObject

/**
 * JSON-over-REST client for the changes endpoint.
 */
class ChangesRestClient(serverRootUrl:String)
    extends AbstractRestClient(serverRootUrl, "changes/")
        with ChangesClient {
  def onChangeEvent(evt:ChangeEvent) {
    val jsonEvt = new JSONObject
    jsonEvt.put("endpoint", evt.endpoint)
    jsonEvt.put("id", evt.id)
    // TODO [#2]
    //jsonEvt.put("date", evt.date.toString(JSONEncodingUtils.dateEncoder))
    jsonEvt.put("lastUpdated", JSONEncodingUtils.maybeDateStr(evt.lastUpdate))
    evt match {
      case us:UpstreamChangeEvent =>
        jsonEvt.put("type", "upstream")
        jsonEvt.put("vsn", us.vsn)
      case ds:DownstreamChangeEvent =>
        jsonEvt.put("type", "downstream-same")
        jsonEvt.put("vsn", ds.vsn)
      case dsc:DownstreamCorrelatedChangeEvent =>
        jsonEvt.put("type", "downstream-correlated")
        jsonEvt.put("uvsn", dsc.upstreamVsn)
        jsonEvt.put("dvsn", dsc.downstreamVsn)
    }
    submit("", jsonEvt.toString)
  }
}