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

package net.lshift.diffa.kernel.client

import net.lshift.diffa.kernel.config._

/**
 * Interface supported by clients capable of configuring the diffa agent.
 */
trait ConfigurationClient {
  def declareGroup(name: String):PairGroup
  def declareEndpoint(name: String, url: String, contentType:String, inboundUrl:String, inboundContentType:String, online:Boolean, categories:java.util.Map[String,CategoryDescriptor]) : Endpoint
  def declarePair(pairKey: String, versionPolicyName: String, matchingTimeout:Int,
                  upstreamName: String, downstreamName: String, groupKey: String):PairDef
  def declareRepairAction(name: String, url: String, scope: String, pairKey: String): RepairAction
  def removeRepairAction(name: String, pairKey: String)
  def declareEscalation(name: String, pairKey: String, action: String, actionType: String, event: String, origin: String) : Escalation
  def removeEscalation(name: String, pairKey: String)
  def deletePair(pairKey: String) : Unit
  def getEndpoint(name:String) : Endpoint
}