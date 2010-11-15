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


import java.lang.String
import net.lshift.diffa.kernel.participants._
import net.lshift.diffa.kernel.events._
import net.lshift.diffa.kernel.config.ConfigStore

/**
 * Version policy where two events are considered the same only when the upstream and downstream provide the
 * same version information. The downstream is expected to have the same interpretation of versions as the upstream,
 * and hashing mechanisms are assumed to be portable.
 *
 * Compliance with this policy could also be achieved by the downstream simply recording the versions of received
 * upstream events.
 */
class SameVersionPolicy(store:VersionCorrelationStore, listener:DifferencingListener, configStore:ConfigStore)
    extends BaseSynchingVersionPolicy(store, listener, configStore:ConfigStore) {

  def synchroniseParticipants(pairKey: String, constraints:Seq[QueryConstraint], us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {
    // Sync the two halves
    (new UpstreamSyncStrategy).syncHalf(pairKey, constraints, us)
    (new DownstreamSameSyncStrategy).syncHalf(pairKey, constraints, ds)
  }

  protected class DownstreamSameSyncStrategy extends SyncStrategy {
    def getAggregates(pairKey:String, constraints:Seq[QueryConstraint]) = {
      // TODO [#2] this is a big bug as it won't work with multiple constraints
      val aggregator = new Aggregator(constraints(0).function)
      store.queryDownstreams(pairKey, constraints, aggregator.collectDownstream)
      aggregator.digests
    }

    def getEntities(pairKey:String, constraints:Seq[QueryConstraint]) = {
      store.queryDownstreams(pairKey, constraints).map(x => {
        EntityVersion(x.id, x.downstreamAttributes.values.toSeq, x.lastUpdate, x.downstreamDVsn)
      })
    }

    def handleMismatch(pairKey:String, vm:VersionMismatch) = {
      vm match {
        case VersionMismatch(id, categories, lastUpdated, partVsn, _) =>
          if (partVsn != null) {
            store.storeDownstreamVersion(VersionID(pairKey, id), categories, lastUpdated, partVsn, partVsn)
          } else {
            store.clearDownstreamVersion(VersionID(pairKey, id))
          }
      }
    }
  }
}