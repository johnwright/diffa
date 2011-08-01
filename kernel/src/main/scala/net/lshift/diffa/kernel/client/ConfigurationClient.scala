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
import net.lshift.diffa.kernel.frontend.{PairDef, EscalationDef, RepairActionDef}

/**
 * Interface supported by clients capable of configuring the diffa agent.
 */
trait ConfigurationClient {
  def declareEndpoint(e:Endpoint) : Endpoint
  def declarePair(p:PairDef):PairDef
  def declareRepairAction(name: String, url: String, scope: String, pairKey: String): RepairActionDef
  def removeRepairAction(name: String, pairKey: String)
  def declareEscalation(name: String, pairKey: String, action: String, actionType: String, event: String, origin: String) : EscalationDef
  def removeEscalation(name: String, pairKey: String)
  def deletePair(pairKey: String) : Unit
  def getEndpoint(name:String) : Endpoint
}