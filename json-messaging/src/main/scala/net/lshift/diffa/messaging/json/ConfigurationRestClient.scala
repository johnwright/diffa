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

import net.lshift.diffa.kernel.config.{Endpoint, PairDef, PairGroup}
import net.lshift.diffa.kernel.client.ConfigurationClient

class ConfigurationRestClient(serverRootUrl:String)
    extends AbstractRestClient(serverRootUrl, "rest/config/")
        with ConfigurationClient {

  def auth(user:String, pass:String) = {

    val response = rpc("endpoints")

    user match {
      case "admin"  => 1
      case _        => 0
    }
  }

  def declareGroup(name: String) = {
    val g = new PairGroup(name)
    create("groups", g)
    g
  }

  def declareEndpoint(name: String, url: String) = {
    val e = new Endpoint(name, url, false)
    create("endpoints", e)
    e
  }

  def declarePair(pairKey: String, versionPolicyName: String, matchingTimeout:Int, upstreamName: String, downstreamName: String, groupKey: String) = {
    val p = new PairDef(pairKey, versionPolicyName, matchingTimeout, upstreamName, downstreamName, groupKey)
    create("pairs", p)
    p
  }

}