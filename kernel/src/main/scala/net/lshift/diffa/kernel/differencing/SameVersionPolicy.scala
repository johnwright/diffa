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


import java.lang.String
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.config.{Endpoint, ConfigStore, Pair}

/**
 * Version policy where two events are considered the same only when the upstream and downstream provide the
 * same version information. The downstream is expected to have the same interpretation of versions as the upstream,
 * and hashing mechanisms are assumed to be portable.
 *
 * Compliance with this policy could also be achieved by the downstream simply recording the versions of received
 * upstream events.
 */
class SameVersionPolicy(stores:VersionCorrelationStoreFactory, listener:DifferencingListener, configStore:ConfigStore)
    extends BaseSynchingVersionPolicy(stores, listener, configStore:ConfigStore) {

  /**
   * Sync the two halves
   */
  def synchroniseParticipants(pair: Pair, writer: VersionCorrelationWriter, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {

    val syncUp = (c:Seq[QueryConstraint]) => (new UpstreamSyncStrategy).syncHalf(pair, writer, pair.upstream, pair.upstream.defaultBucketing, c, us)
    val syncDown = (c:Seq[QueryConstraint]) => (new DownstreamSameSyncStrategy).syncHalf(pair, writer, pair.downstream, pair.downstream.defaultBucketing, c, ds)

    filterSetConstraints(pair.upstream, syncUp)
    filterSetConstraints(pair.downstream, syncDown)
  }

  // #203: By default, set elements should be sent out individually - in the future, this may be configurable
  def filterSetConstraints(endpoint:Endpoint, f:Function1[Seq[QueryConstraint],Unit]) = {
    def isSet = (q:QueryConstraint) => q.isInstanceOf[SetQueryConstraint]
    val setBased = endpoint.defaultConstraints.filter(isSet)
    val others = endpoint.defaultConstraints.filterNot(isSet)

    setBased.isEmpty match {
      case false  => {
        setBased.foreach(s => {
          val packed = s.asInstanceOf[SetQueryConstraint]
          val unpacked = packed.values.map(v => SetQueryConstraint(packed.category, Set(v)))
          unpacked.foreach(u => f(others :+ u))
        })
      }
      case true  => f(others)
    }
  }

  protected class DownstreamSameSyncStrategy extends SyncStrategy {
    def getAggregates(pairKey:String, bucketing:Map[String, CategoryFunction], constraints:Seq[QueryConstraint]) = {
      val aggregator = new Aggregator(bucketing)
      stores(pairKey).queryDownstreams(constraints, aggregator.collectDownstream)
      aggregator.digests
    }

    def getEntities(pairKey:String, constraints:Seq[QueryConstraint]) = {
      stores(pairKey).queryDownstreams(constraints).map(x => {
        EntityVersion(x.id, AttributesUtil.toSeq(x.downstreamAttributes.toMap), x.lastUpdate, x.downstreamDVsn)
      })
    }

    def handleMismatch(pairKey:String, writer: VersionCorrelationWriter, vm:VersionMismatch) = {
      vm match {
        case VersionMismatch(id, categories, lastUpdated, partVsn, _) =>
          if (partVsn != null) {
            handleUpdatedCorrelation(writer.storeDownstreamVersion(VersionID(pairKey, id), categories, lastUpdated, partVsn, partVsn))
          } else {
            handleUpdatedCorrelation(writer.clearDownstreamVersion(VersionID(pairKey, id)))
          }
      }
    }
  }
}