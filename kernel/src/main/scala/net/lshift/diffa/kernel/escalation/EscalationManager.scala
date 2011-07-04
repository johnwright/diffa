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

package net.lshift.diffa.kernel.escalation

import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import net.lshift.diffa.kernel.differencing.{ScanTrigger, MatchingAntecedent, DifferencingListener}
import net.lshift.diffa.kernel.config.ConfigStore
import net.lshift.diffa.kernel.client.{ActionableRequest, ActionsClient}
import com.sun.xml.internal.ws.developer.MemberSubmissionAddressing.Validation
import org.slf4j.LoggerFactory

/**
 * This deals with escalating mismatches based on configurable escalation policies.
 *
 *
 * TODO Revise this description when the full blown escalation manager lands.
 * ATM the scope of this manager is just to invoke actions that are triggered by mismatches
 * that still exist after a scan.
 *
 * THis means that for now, escalations don't have to be persistent, because the only thing that can
 * can trigger an escalation is a scan and it is assumed that a sane deployment will not have too
 * many scans configured.
 *
 * In future versions, this procedure will make escalations persistent and the process of escalation
 * will not be driven by difference events, rather there will be a poll loop to drive the procedure
 * through configurable steps.
 */
class EscalationManager(val config:ConfigStore,
                        val actionsClient:ActionsClient)
    extends DifferencingListener {

  val log = LoggerFactory.getLogger(getClass)

  /**
   * Since escalations are currently only driven off mismatches, matches can be safely ignored.
   */
  def onMatch(id: VersionID, vsn: String, antecedent: MatchingAntecedent) = ()

  /**
   * Escalate matches that occur as part of a scan.
   */
  def onMismatch(id: VersionID, lastUpdated: DateTime, upstreamVsn: String, downstreamVsn: String,
                 antecedent: MatchingAntecedent) = antecedent match{
    case ScanTrigger => {
      val pair = config.getPair(id.pairKey)
      val escalations = config.listEscalationsForPair(pair)
      escalations.foreach( e => {
        val result = actionsClient.invoke(ActionableRequest(id.pairKey, e.name, id.id))
        log.debug("Escalation result for action [%s] using %s is %s".format(e.name, id, result))
      })
    }
    case _           => // ignore this for now
  }
}