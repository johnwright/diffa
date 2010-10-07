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

package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events._
import collection.mutable.Queue
import net.lshift.diffa.kernel.participants._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.alerting.Alerter

/**
 * Standard behaviours supported by synchronising version policies.
 */
abstract class BaseSynchingVersionPolicy(val store:VersionCorrelationStore, listener:DifferencingListener)
    extends VersionPolicy {
  protected val alerter = Alerter.forClass(getClass)

  /**
   * Handles a participant change. Due to the need to later correlate data, event information is cached to the
   * version correlation store.
   */
  def onChange(evt: PairChangeEvent) = {
    val corr = evt match {
      case UpstreamPairChangeEvent(id, date, lastUpdate, vsn) => vsn match {
        case null => store.clearUpstreamVersion(id)
        case _    => store.storeUpstreamVersion(id, date, maybe(lastUpdate), vsn)
      }
      case DownstreamPairChangeEvent(id, date, lastUpdate, vsn) => vsn match {
        case null => store.clearDownstreamVersion(id)
        case _    => store.storeDownstreamVersion(id, date, maybe(lastUpdate), vsn, vsn)
      }
      case DownstreamCorrelatedPairChangeEvent(id, date, lastUpdate, uvsn, dvsn) => (uvsn, dvsn) match {
        case (null, null) => store.clearDownstreamVersion(id)
        case _            => store.storeDownstreamVersion(id, date, maybe(lastUpdate), uvsn, dvsn)
      }
    }

    if (corr.isMatched.booleanValue) {
      listener.onMatch(evt.id, corr.upstreamVsn)
    } else {
      listener.onMismatch(evt.id, corr.lastUpdate, corr.upstreamVsn, corr.downstreamUVsn)
    }
  }

  def maybe(lastUpdate:DateTime) = {
    lastUpdate match {
      case null => new DateTime
      case d    => d
    }
  }

  def difference(pairKey: String, dates: DateConstraint, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {
    synchroniseParticipants(pairKey, dates, us, ds, l)

    // Run a query for mismatched versions, and report each one
    store.unmatchedVersions(pairKey, dates).foreach(
      corr => l.onMismatch(VersionID(corr.pairing, corr.id), corr.lastUpdate, corr.upstreamVsn, corr.downstreamUVsn))

    true
  }

  /**
   * Allows the policy to perform a synchronisation of participants.
   */
  protected def synchroniseParticipants(pairKey: String, dates: DateConstraint, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener)

  /**
   * The basic functionality for a synchronisation strategy.
   */
  protected abstract class SyncStrategy {
    def syncHalf(pairKey:String, dates:DateConstraint, p:Participant) {
      val syncActions = new Queue[QueryAction]
      syncActions += QueryAction(dates.start, dates.end, YearGranularity)

      while (syncActions.length > 0) {
        val action = syncActions.dequeue
        val usDigests = p.queryDigests(action.start, action.end, action.gran)
        val cachedDigests = getDigests(pairKey, DateConstraint(action.start, action.end), action.gran)

        DigestDifferencingUtils.differenceDigests(usDigests, cachedDigests, action.gran).foreach(o => o match {
          case newAction:QueryAction => syncActions += newAction
          case mm:VersionMismatch => handleMismatch(pairKey, mm)
        })
      }
    }

    def getDigests(pairKey:String, dates:DateConstraint, gran:RangeGranularity):Seq[VersionDigest]
    def handleMismatch(pairKey:String, vm:VersionMismatch)
  }

  protected class UpstreamSyncStrategy extends SyncStrategy {
    def getDigests(pairKey:String, dates:DateConstraint, gran:RangeGranularity) = {
      val aggregator = new Aggregator(gran)
      store.queryUpstreams(pairKey, dates, aggregator.collectUpstream)
      aggregator.digests
    }
    def handleMismatch(pairKey:String, vm:VersionMismatch) = {
      vm match {
        case VersionMismatch(id, date, lastUpdate,  usVsn, _) =>
          if (usVsn != null) {
            store.storeUpstreamVersion(VersionID(pairKey, id), lastUpdate, date, usVsn)
          } else {
            store.clearUpstreamVersion(VersionID(pairKey, id))
          }
      }
    }
  }

  protected class Aggregator(val gran:RangeGranularity) {
    val builder = new DigestBuilder(gran)

    def collectUpstream(id:VersionID, date:DateTime, lastUpdate:DateTime, vsn:String) =
      builder.add(id, date, lastUpdate, vsn)
    def collectDownstream(id:VersionID, date:DateTime, lastUpdate:DateTime, uvsn:String, dvsn:String) =
      builder.add(id, date, lastUpdate, dvsn)

    def digests:Seq[VersionDigest] = builder.digests
  }
}