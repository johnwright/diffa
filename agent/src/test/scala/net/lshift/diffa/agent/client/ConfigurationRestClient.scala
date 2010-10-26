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

package net.lshift.diffa.agent.client

import net.lshift.diffa.kernel.config.{Endpoint, PairDef, PairGroup}
import net.lshift.diffa.kernel.client.ConfigurationClient
import net.lshift.diffa.messaging.json.AbstractRestClient

class ConfigurationRestClient(serverRootUrl:String)
    extends AbstractRestClient(serverRootUrl, "rest/config/")
        with ConfigurationClient {

  def declareGroup(name: String) = {
    val g = new PairGroup(name)
    create("groups", g)
    g
  }

  def declareInboundEndpoint(name: String, url: String, contentType:String):Endpoint = {
    val e = Endpoint(name, null, contentType, url, false)
    create("endpoints", e)
    e
  }

  def declareEndpoint(name: String, url: String, contentType:String) = {
    val e = Endpoint(name, url, contentType, null, false)
    create("endpoints", e)
    e
  }

  def declarePair(pairKey: String, versionPolicyName: String, matchingTimeout:Int, upstreamName: String, downstreamName: String, groupKey: String) = {
    val p = new PairDef(pairKey, versionPolicyName, matchingTimeout, upstreamName, downstreamName, groupKey)
    create("pairs", p)
    p
  }

}