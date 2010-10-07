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

/**
 * Version policy where two events are considered the same based on the downstream reporting the same upstream
 * version upon processing. The downstream is not expected to reproduce the same digests as the upstream on demand,
 * and matching recovery will require messages to be reprocessed via a differencing back-channel to determine
 * whether they are identical.
 */
class CorrelatedVersionPolicy(store:VersionCorrelationStore, listener:DifferencingListener)
    extends BaseSynchingVersionPolicy(store, listener) {

  def synchroniseParticipants(pairKey: String, dates: DateConstraint, us: UpstreamParticipant, ds: DownstreamParticipant, l:DifferencingListener) = {
    // Sync the two halves
    (new UpstreamSyncStrategy).syncHalf(pairKey, dates, us)
    (new DownstreamCorrelatingSyncStrategy(us, ds, l)).syncHalf(pairKey, dates, ds)
  }
  
  private class DownstreamCorrelatingSyncStrategy(val us:UpstreamParticipant, val ds:DownstreamParticipant, val l:DifferencingListener)
      extends SyncStrategy {
    
    def getDigests(pairKey:String, dates:DateConstraint, gran:RangeGranularity) = {
      val aggregator = new Aggregator(gran)
      store.queryDownstreams(pairKey, dates, aggregator.collectDownstream)
      aggregator.digests
    }
    def handleMismatch(pairKey:String, vm:VersionMismatch) = {
      vm match {
        case VersionMismatch(id, date, _, null, storedVsn) =>
          store.clearDownstreamVersion(VersionID(pairKey, id))
        case VersionMismatch(id, date, lastUpdated, partVsn, _) =>
          val content = us.retrieveContent(id)
          val response = ds.generateVersion(content)

          if (response.dvsn == partVsn) {
            // This is the same destination object, so we're safe to store the correlation
            store.storeDownstreamVersion(VersionID(pairKey, id), date, lastUpdated, response.uvsn, response.dvsn)
          } else {
            // We can't update our datastore, so we just have to generate a mismatch            
            l.onMismatch(VersionID(pairKey, id), lastUpdated, response.dvsn, partVsn)
          }
      }
    }
  }
}