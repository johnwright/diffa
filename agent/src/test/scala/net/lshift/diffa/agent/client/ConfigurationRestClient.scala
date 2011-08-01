package net.lshift.diffa.agent.client

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

import net.lshift.diffa.kernel.client.ConfigurationClient
import net.lshift.diffa.messaging.json.AbstractRestClient
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config._
import com.sun.jersey.api.client.ClientResponse
import net.lshift.diffa.kernel.frontend.{PairDef, EscalationDef, RepairActionDef}

class ConfigurationRestClient(serverRootUrl:String)
    extends AbstractRestClient(serverRootUrl, "rest/config/")
        with ConfigurationClient {

  def declareEndpoint(e:Endpoint) = {
    create("endpoints", e)
    e
  }

  def declarePair(p:PairDef) = {
    create("pairs", p)
    p
  }

  def declareRepairAction(name: String, id: String, scope: String, pairKey: String) = {
    val action = new RepairActionDef(name, id, scope, pairKey)
    create("/pairs/"+pairKey+"/repair-actions", action)
    action
  }

  def removeRepairAction(name: String, pairKey: String) {
    delete("/pairs/"+pairKey+"/repair-actions/"+name)
  }

  def declareEscalation(name: String, pairKey: String, action: String, actionType: String, event: String, origin: String) = {
    val escalation = new EscalationDef(name, pairKey, action, actionType, event, origin)
    create("/pairs/"+pairKey+"/escalations", escalation)
    escalation
  }

  def removeEscalation(name: String, pairKey: String) = {
    delete("/pairs/" + pairKey + "/escalations/" + name)
  }

  def deletePair(pairKey: String) = {
    val response = resource.path("pairs").path(pairKey).delete(classOf[ClientResponse])
    val status = response.getClientResponseStatus
    status.getStatusCode match {
      case 204     => // Successfully submitted (202 is "No Content")
      case x:Int   => throw new RuntimeException("HTTP " + x + " : " + status.getReasonPhrase)
    }
  }

  def getEndpoint(name:String) = rpc("endpoints/" + name, classOf[Endpoint])
}
