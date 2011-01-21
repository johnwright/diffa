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

import net.lshift.diffa.kernel.config.{PairDef, Endpoint, PairGroup}
import scala.collection.Map

/**
 * Interface supported by clients capable of configuring the diffa agent.
 */
trait ConfigurationClient {
  def declareGroup(name: String):PairGroup
  def declareEndpoint(name: String, url: String, contentType:String, inboundUrl:String, online:Boolean) : Endpoint
  def declarePair(pairKey: String, versionPolicyName: String, matchingTimeout:Int,
                  upstreamName: String, downstreamName: String, groupKey: String, categories:Map[String,String]):PairDef
}