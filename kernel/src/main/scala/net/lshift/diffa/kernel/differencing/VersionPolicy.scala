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

import net.lshift.diffa.kernel.events.PairChangeEvent
import net.jcip.annotations.NotThreadSafe
import net.lshift.diffa.kernel.participants.{UpstreamParticipant, DownstreamParticipant}
import net.lshift.diffa.kernel.config.Pair

/**
 * Policy implementations of this trait provide different mechanism for handling the matching of upstream
 * and downstream events. This functionality is pluggable since different systems may have different views
 * on how to compare version information between participants.
 *
 * Please note that this trait is by design <em>NOT</em> thread safe and hence any access to it must be
 * serialized by the caller.
 *
 */
@NotThreadSafe
trait VersionPolicy {

  /**
   * Indicates to the policy that a change has occurred within a participant.
   */
  def onChange(writer: VersionCorrelationWriter, evt:PairChangeEvent) : Unit

  /**
   * Requests that the policy generate a series of events describing the differences between the endpoints
   * within the given pair. Will not perform any endpoint synchronisation, and will operate entirely from
   * local data stores.
   */
  def difference(pairKey:String, listener:DifferencingListener)

  /**
   * Requests that the policy synchronize then difference the given participants. Differences that are
   * detected will be reported to the listener provided.
   */
  @Deprecated
  def syncAndDifference(pairKey:String,
                        writer: VersionCorrelationWriter,
                        us:UpstreamParticipant,
                        ds:DownstreamParticipant,
                        listener:DifferencingListener) : Boolean

//  def handleMismatch(pairKey:String,
//                     writer: VersionCorrelationWriter,
//                     vm:VersionMismatch,
//                     listener:DifferencingListener,
//                     eventHandler:CorrelationEventHandler) : Unit

  def scanUpstream(pairKey:String, writer: VersionCorrelationWriter, participant:UpstreamParticipant, listener:DifferencingListener) : Unit
  def scanDownstream(pairKey:String, writer: VersionCorrelationWriter, participant:DownstreamParticipant, listener:DifferencingListener) : Unit

}

/**
   * A delegate that knows how to process a mismatched version that is emitted from a policy
   */
//trait VersionMismatchHandler {
//  def handleMismatch(pair:Pair, mismatch:VersionMismatch) : Unit
//}

//trait CorrelationEventHandler {
//  def handleUpdate(correlation:Correlation) : Unit
//}