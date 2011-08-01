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

package net.lshift.diffa.kernel.frontend

import net.lshift.diffa.kernel.config.{Domain, Escalation, RepairAction, Endpoint, Pair => DiffaPair}

/**
 * A bunch of converter functions to translate frontend objects from their internal counterparts
 */
object FrontendConversions {

  def toEndpointDef(e:Endpoint) = EndpointDef(
    name = e.name,
    scanUrl = e.scanUrl,
    contentRetrievalUrl = e.contentRetrievalUrl,
    versionGenerationUrl = e.versionGenerationUrl,
    contentType = e.contentType,
    inboundUrl = e.inboundUrl,
    inboundContentType = e.inboundContentType,
    categories = e.categories)

  def fromEndpointDef(domain:Domain, e:EndpointDef) = Endpoint(
    name = e.name,
    domain = domain,
    scanUrl = e.scanUrl,
    contentRetrievalUrl = e.contentRetrievalUrl,
    versionGenerationUrl = e.versionGenerationUrl,
    contentType = e.contentType,
    inboundUrl = e.inboundUrl,
    inboundContentType = e.inboundContentType,
    categories = e.categories)

  def toPairDef(p:DiffaPair) = PairDef(
    key = p.key,
    versionPolicyName = p.versionPolicyName,
    matchingTimeout = p.matchingTimeout,
    upstreamName = p.upstream.name,
    downstreamName = p.downstream.name,
    scanCronSpec = p.scanCronSpec)

  def toRepairActionDef(a:RepairAction) = RepairActionDef(
    name = a.name,
    url = a.url,
    scope = a.scope,
    pair = a.pair.key
  )

  def fromRepairActionDef(pair:DiffaPair, a:RepairActionDef) = RepairAction(
    name = a.name,
    url = a.url,
    scope = a.scope,
    pair = pair
  )

  def toEscalationDef(e:Escalation) = EscalationDef(
    name = e.name,
    pair = e.pair.key,
    action = e.action,
    actionType = e.actionType,
    origin = e.origin,
    event = e.event
  )

  def fromEscalationDef(pair:DiffaPair,e:EscalationDef) = Escalation(
    name = e.name,
    pair = pair,
    action = e.action,
    actionType = e.actionType,
    origin = e.origin,
    event = e.event
  )
}