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

package net.lshift.diffa.kernel.differencing

import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.participants._
import org.joda.time.DateTime
import net.lshift.diffa.kernel.alerting.Alerter
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config.{Endpoint, Pair, ConfigStore}

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

    val corr = evt match {
      case UpstreamPairChangeEvent(id, _, lastUpdate, vsn) => vsn match {
        case null => store.clearUpstreamVersion(id)
        case _    => store.storeUpstreamVersion(id, pair.upstream.schematize(evt.attributes), maybe(lastUpdate), vsn)
      }
      case DownstreamPairChangeEvent(id, _, lastUpdate, vsn) => vsn match {
        case null => store.clearDownstreamVersion(id)
        case _    => store.storeDownstreamVersion(id, pair.downstream.schematize(evt.attributes), maybe(lastUpdate), vsn, vsn)
      }
      case DownstreamCorrelatedPairChangeEvent(id, _, lastUpdate, uvsn, dvsn) => (uvsn, dvsn) match {
        case (null, null) => store.clearDownstreamVersion(id)
        case _            => store.storeDownstreamVersion(id, pair.downstream.schematize(evt.attributes), maybe(lastUpdate), uvsn, dvsn)
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

    synchroniseParticipants(pair, us, ds, l)

    // Run a query for mismatched versions, and report each one
    store.unmatchedVersions(pairKey, pair.upstream.defaultConstraints, pair.downstream.defaultConstraints).foreach(
      corr => l.onMismatch(VersionID(corr.pairing, corr.id), corr.lastUpdate, corr.upstreamVsn, corr.downstreamUVsn))

    true
  }

  /**
   * Allows the policy to perform a synchronisation of participants.
   */
  protected def synchroniseParticipants(pair: Pair, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener)

  /**
   * The basic functionality for a synchronisation strategy.
   */
  protected abstract class SyncStrategy {
    def syncHalf(pair:Pair, endpoint:Endpoint, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint], p:Participant) {
      val remoteDigests = p.queryAggregateDigests(bucketing, constraints)
      val localDigests = getAggregates(pair.key, bucketing, constraints)

      DigestDifferencingUtils.differenceAggregates(remoteDigests, localDigests, bucketing, constraints).foreach(o => o match {
        case AggregateQueryAction(narrowBuckets, narrowConstraints) =>
          syncHalf(pair, endpoint, narrowBuckets, narrowConstraints, p)
        case EntityQueryAction(narrowed)    => {
          val remoteVersions = p.queryEntityVersions(narrowed)
          val cachedVersions = getEntities(pair.key, narrowed)
          DigestDifferencingUtils.differenceEntities(endpoint.categories.toMap, remoteVersions, cachedVersions, narrowed).foreach(handleMismatch(pair.key, _))
        }
      })
    }

    def getAggregates(pairKey:String, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint]) : Seq[AggregateDigest]
    def getEntities(pairKey:String, constraints:Seq[QueryConstraint]) : Seq[EntityVersion]
    def handleMismatch(pairKey:String, vm:VersionMismatch)
  }

  protected class UpstreamSyncStrategy extends SyncStrategy {

    def getAggregates(pairKey:String, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint]) = {
      val aggregator = new Aggregator(bucketing)
      store.queryUpstreams(pairKey, constraints, aggregator.collectUpstream)
      aggregator.digests
    }

    def getEntities(pairKey:String, constraints:Seq[QueryConstraint]) = {
      store.queryUpstreams(pairKey, constraints).map(x => {
        EntityVersion(x.id, AttributesUtil.toSeq(x.upstreamAttributes.toMap), x.lastUpdate, x.upstreamVsn)
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

  protected class Aggregator(bucketing:Map[String, CategoryFunction]) {
    val builder = new DigestBuilder(bucketing)

    def collectUpstream(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, vsn:String) =
      builder.add(id, attributes, lastUpdate, vsn)
    def collectDownstream(id:VersionID, attributes:Map[String, String], lastUpdate:DateTime, uvsn:String, dvsn:String) =
      builder.add(id, attributes, lastUpdate, dvsn)

    def digests:Seq[AggregateDigest] = builder.digests
  }
}