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
import net.lshift.diffa.kernel.config.{ConfigStore,Pair}

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

  def synchroniseParticipants(pair: Pair, writer: VersionCorrelationWriter, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {
    // Sync the two halves
    (new UpstreamSyncStrategy).syncHalf(pair, writer, pair.upstream, pair.upstream.defaultBucketing, pair.upstream.defaultConstraints, us)
    (new DownstreamSameSyncStrategy).syncHalf(pair, writer, pair.downstream, pair.downstream.defaultBucketing, pair.downstream.defaultConstraints, ds)
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
            writer.storeDownstreamVersion(VersionID(pairKey, id), categories, lastUpdated, partVsn, partVsn)
          } else {
            writer.clearDownstreamVersion(VersionID(pairKey, id))
          }
      }
    }
  }
}