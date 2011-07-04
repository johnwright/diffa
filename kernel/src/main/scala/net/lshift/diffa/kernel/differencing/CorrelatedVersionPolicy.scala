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
import net.lshift.diffa.kernel.config.{ConfigStore,Pair}

/**
 * Version policy where two events are considered the same based on the downstream reporting the same upstream
 * version upon processing. The downstream is not expected to reproduce the same digests as the upstream on demand,
 * and matching recovery will require messages to be reprocessed via a differencing back-channel to determine
 * whether they are identical.
 */
class CorrelatedVersionPolicy(stores:VersionCorrelationStoreFactory,
                              listener:DifferencingListener,
                              configStore:ConfigStore)
    extends BaseSynchingVersionPolicy(stores, listener, configStore) {

  def downstreamStrategy(us:UpstreamParticipant, ds:DownstreamParticipant) = new DownstreamCorrelatingSyncStrategy(us,ds)
  
  protected class DownstreamCorrelatingSyncStrategy(val us:UpstreamParticipant, val ds:DownstreamParticipant)
      extends SyncStrategy {
    
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

    def handleMismatch(pairKey:String, writer: LimitedVersionCorrelationWriter, vm:VersionMismatch, listener:DifferencingListener) = {
      vm match {
        case VersionMismatch(id, categories, _, null, storedVsn) =>
          handleUpdatedCorrelation(writer.clearDownstreamVersion(VersionID(pairKey, id)))
        case VersionMismatch(id, categories, lastUpdated, partVsn, _) =>
          val content = us.retrieveContent(id)
          val response = ds.generateVersion(content)

          if (response.dvsn == partVsn) {
            // This is the same destination object, so we're safe to store the correlation
            handleUpdatedCorrelation(writer.storeDownstreamVersion(VersionID(pairKey, id), categories, lastUpdated, response.uvsn, response.dvsn))
          } else {
            // We can't update our datastore, so we just have to generate a mismatch
            listener.onMismatch(VersionID(pairKey, id), lastUpdated, response.dvsn, partVsn, LiveWindow)
          }
      }
    }
  }
}