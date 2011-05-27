/**
 * Copyright (C) 2010-2011 LShift Ltd.
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

package net.lshift.diffa.tools.client

import net.lshift.diffa.kernel.client.ConfigurationClient
import net.lshift.diffa.messaging.json.AbstractRestClient
import scala.collection.Map
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.{CategoryDescriptor, Endpoint, PairDef, PairGroup}
import javax.ws.rs.core.MediaType
import com.sun.jersey.api.client.ClientResponse

class ConfigurationRestClient(serverRootUrl:String)
    extends AbstractRestClient(serverRootUrl, "rest/config/")
        with ConfigurationClient {

  def declareGroup(name: String) = {
    val g = new PairGroup(name)
    create("groups", g)
    g
  }

  def declareEndpoint(name: String, url: String, contentType:String, inboundUrl:String, inboundContentType:String, online:Boolean, categories:java.util.Map[String,CategoryDescriptor]) = {
    val e = Endpoint(name, url, contentType, inboundUrl, inboundContentType, online, categories)
    create("endpoints", e)
    e
  }

  def declarePair(pairKey: String, versionPolicyName: String, matchingTimeout:Int, upstreamName: String,
                  downstreamName: String, groupKey: String) = {
    val p = new PairDef(pairKey, versionPolicyName, matchingTimeout, upstreamName, downstreamName, groupKey)
    create("pairs", p)
    p
  }

  def deletePair(pairKey: String) = {
    val response = resource.path("pairs").path(pairKey).delete(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 204     => // Successfully submitted (202 is "No Content")
      case x:Int   => throw new RuntimeException("HTTP " + x + " : " + status.getReasonPhrase)
    }
  }

}