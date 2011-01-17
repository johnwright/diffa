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
import net.lshift.diffa.kernel.config.ConfigStore
import scala.collection.JavaConversions._

/**
 * Standard behaviours supported by synchronising version policies.
 */
abstract class BaseSynchingVersionPolicy(val store:VersionCorrelationStore,
                                         listener:DifferencingListener,
                                         configStore:ConfigStore)
    extends VersionPolicy {
  protected val alerter = Alerter.forClass(getClass)

  /**
   * Handles a participant change. Due to the need to later correlate data, event information is cached to the
   * version correlation store.
   */
  def onChange(evt: PairChangeEvent) = {

    val pair = configStore.getPair(evt.id.pairKey)
    val attributes = pair.schematize(evt.attributes)

    val corr = evt match {
      case UpstreamPairChangeEvent(id, _, lastUpdate, vsn) => vsn match {
        case null => store.clearUpstreamVersion(id)
        case _    => store.storeUpstreamVersion(id, attributes, maybe(lastUpdate), vsn)
      }
      case DownstreamPairChangeEvent(id, _, lastUpdate, vsn) => vsn match {
        case null => store.clearDownstreamVersion(id)
        case _    => store.storeDownstreamVersion(id, attributes, maybe(lastUpdate), vsn, vsn)
      }
      case DownstreamCorrelatedPairChangeEvent(id, _, lastUpdate, uvsn, dvsn) => (uvsn, dvsn) match {
        case (null, null) => store.clearDownstreamVersion(id)
        case _            => store.storeDownstreamVersion(id, attributes, maybe(lastUpdate), uvsn, dvsn)
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

  def difference(pairKey: String, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {
    val pair = configStore.getPair(pairKey)
    val constraints = pair.defaultConstraints

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

      def resolve(d:Digest) = {
        val pair = configStore.getPair(pairKey)
        pair.schematize(d.attributes)
      }

      val remoteDigests = p.queryAggregateDigests(constraints)
      val cachedAggregates = getAggregates(pairKey, constraints)

      DigestDifferencingUtils.differenceAggregates(remoteDigests, cachedAggregates, resolve, constraints).foreach(o => o match {
        case a:AggregateQueryAction => syncHalf(pairKey, Seq(a.constraint), p)
        case e:EntityQueryAction    => {
          val narrowed = Seq(e.constraint)
          val remoteVersions = p.queryEntityVersions(narrowed)
          val cachedVersions = getEntities(pairKey, narrowed)
          DigestDifferencingUtils.differenceEntities(remoteVersions, cachedVersions, resolve, narrowed).foreach(handleMismatch(pairKey, _))
        }
      })

    }

    def getAggregates(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[AggregateDigest]
    def getEntities(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[EntityVersion]
    def handleMismatch(pairKey:String, vm:VersionMismatch)
  }

  protected class UpstreamSyncStrategy extends SyncStrategy {

    def getAggregates(pairKey:String, constraints:Seq[QueryConstraint]): Seq[AggregateDigest] = {
      assert(constraints.length < 2, "See ticket #148")
      constraints flatMap { constraint =>
        val aggregator = new Aggregator(constraint.function)
        store.queryUpstreams(pairKey, constraints, aggregator.collectUpstream)
        aggregator.digests
      }
    }

    def getEntities(pairKey:String, constraints:Seq[QueryConstraint]) = {
      store.queryUpstreams(pairKey, constraints).map(x => {
        EntityVersion(x.id, x.upstreamAttributes.values.toSeq, x.lastUpdate, x.upstreamVsn)
      })
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

  protected class Aggregator(val function:CategoryFunction) {
    val builder = new DigestBuilder(function)

    def collectUpstream(id:VersionID, attributes:Seq[String], lastUpdate:DateTime, vsn:String) =
      builder.add(id, attributes, lastUpdate, vsn)
    def collectDownstream(id:VersionID, attributes:Seq[String], lastUpdate:DateTime, uvsn:String, dvsn:String) =
      builder.add(id, attributes, lastUpdate, dvsn)

    def digests:Seq[AggregateDigest] = builder.digests
  }
}