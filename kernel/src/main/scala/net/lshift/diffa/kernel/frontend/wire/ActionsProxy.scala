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

package net.lshift.diffa.kernel.frontend.wire

import net.lshift.diffa.kernel.participants.ParticipantFactory
import net.lshift.diffa.kernel.client.{Actionable, ActionableRequest, ActionsClient}
import net.lshift.diffa.kernel.config.{RepairAction, Pair, ConfigStore}
import InvocationResult._
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpPost
import io.Source
import net.lshift.diffa.kernel.diag.{DiagnosticLevel, DiagnosticsManager}

/**
 * This is a conduit to the actions that are provided by participants
 */
class ActionsProxy(val config:ConfigStore, val factory:ParticipantFactory, val diagnostics:DiagnosticsManager)
    extends ActionsClient {

  def listActions(pairKey: String): Seq[Actionable] =
    withValidPair(pairKey) { pair =>
      config.listRepairActionsForPair(pair).map(Actionable.fromRepairAction)
    }

  def listEntityScopedActions(pairKey: String) = listActions(pairKey).filter(_.scope == RepairAction.ENTITY_SCOPE)

  def listPairScopedActions(pairKey: String) = listActions(pairKey).filter(_.scope == RepairAction.PAIR_SCOPE)

  def invoke(request: ActionableRequest): InvocationResult =
    withValidPair(request.pairKey) { pair =>
      val client = new DefaultHttpClient
      val repairAction = config.getRepairAction(request.actionId, request.pairKey)
      val url = repairAction.scope match {
        case RepairAction.ENTITY_SCOPE => repairAction.url.replace("{id}", request.entityId)
        case RepairAction.PAIR_SCOPE => repairAction.url
      }
      val actionDescription = "\"" + repairAction.name + "\" on " + (repairAction.scope match {
        case RepairAction.ENTITY_SCOPE => "entity " + request.entityId + " of pair " + request.pairKey
        case RepairAction.PAIR_SCOPE => "pair " + request.pairKey
      })
      diagnostics.logPairEvent(DiagnosticLevel.Info, request.pairKey, "Initiating action " + actionDescription)

      try {
        val httpResponse = client.execute(new HttpPost(url))
        val httpCode = httpResponse.getStatusLine.getStatusCode
        val httpEntity = Source.fromInputStream(httpResponse.getEntity.getContent).mkString

        if (httpCode >= 200 && httpCode < 300) {
          diagnostics.logPairEvent(DiagnosticLevel.Info, request.pairKey, "Action " + actionDescription + " succeeded: " + httpEntity)
        } else {
          diagnostics.logPairEvent(DiagnosticLevel.Error, request.pairKey, "Action " + actionDescription + " failed: " + httpEntity)
        }
        InvocationResult.received(httpCode, httpEntity)
      }
      catch {
        case e =>
          diagnostics.logPairEvent(DiagnosticLevel.Error, request.pairKey, "Action " + actionDescription + " failed: " + e.getMessage)
          InvocationResult.failure(e)
      }
    }

  def withValidPair[T](pairKey: String)(f: Pair => T): T = {
    val pair = config.getPair(pairKey)
    f(pair)
  }

}
