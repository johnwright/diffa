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
import net.lshift.diffa.kernel.participants._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.alerting.Alerter
import scala.collection.Map
import collection.mutable.{HashMap, Queue}

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

  def difference(pairKey: String, constraints:Seq[QueryConstraint], us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {
    synchroniseParticipants(pairKey, constraints, us, ds, l)

    // Run a query for mismatched versions, and report each one
    store.unmatchedVersions(pairKey, constraints).foreach(
      corr => l.onMismatch(VersionID(corr.pairing, corr.id), corr.lastUpdate, corr.upstreamVsn, corr.downstreamUVsn))

    true
  }

  /**
   * Allows the policy to perform a synchronisation of participants.
   */
  protected def synchroniseParticipants(pairKey: String, constraints:Seq[QueryConstraint], us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener)

  /**
   * The basic functionality for a synchronisation strategy.
   */
  protected abstract class SyncStrategy {
    def syncHalf(pairKey:String, constraints:Seq[QueryConstraint], p:Participant) {

      val usDigests = p.queryAggregateDigests(constraints)
      val cachedDigests = getDigests(pairKey, constraints)

      def resolve(d:Digest) = new HashMap[String,String]

      DigestDifferencingUtils.differenceDigests(usDigests, cachedDigests, resolve, constraints).foreach(o => o match {
        case newAction:QueryAction => // Go directly to go, do not collect 200 //syncActions += newAction
        case mm:VersionMismatch => handleMismatch(pairKey, mm)
      })

    }

    def getDigests(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[Digest]
    def handleMismatch(pairKey:String, vm:VersionMismatch)
  }

  protected class UpstreamSyncStrategy extends SyncStrategy {

    def getDigests(pairKey:String, constraints:Seq[QueryConstraint]) = {
      val aggregator = new Aggregator()
      store.queryUpstreams(pairKey, constraints, aggregator.collectUpstream)
      aggregator.digests
    }

    def handleMismatch(pairKey:String, vm:VersionMismatch) = {
      vm match {
        case VersionMismatch(id, attributes, lastUpdate,  usVsn, _) =>
          if (usVsn != null) {
            store.storeUpstreamVersion(VersionID(pairKey, id), attributes, lastUpdate, usVsn)
          } else {
            store.clearUpstreamVersion(VersionID(pairKey, id))
          }
      }
    }
  }

  protected class Aggregator() {
    val builder = new DigestBuilder(new DateCategoryFunction)

    def collectUpstream(id:VersionID, attributes:Map[String,String], lastUpdate:DateTime, vsn:String) =
      builder.add(id, attributes, lastUpdate, vsn)
    def collectDownstream(id:VersionID, attributes:Map[String,String], lastUpdate:DateTime, uvsn:String, dvsn:String) =
      builder.add(id, attributes, lastUpdate, dvsn)

    def digests:Seq[Digest] = builder.digests
  }
}