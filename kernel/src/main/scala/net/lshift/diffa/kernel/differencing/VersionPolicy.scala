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
  def onChange(writer: LimitedVersionCorrelationWriter, evt:PairChangeEvent) : Unit

  /**
   * Invokes the underlying <code>replayUnmatchedDifferences</code> call,
   * defaulting the origin to <code>TriggeredByScan</code>.
   */
  def replayUnmatchedDifferences(pairKey:String, listener:DifferencingListener) : Unit =
    replayUnmatchedDifferences(pairKey, listener, TriggeredByScan)

  /**
   * Requests that the policy generate a series of events describing the differences between the endpoints
   * within the given pair. This does not perform any endpoint scanning, it operates entirely from
   * local data stores.
   */
  def replayUnmatchedDifferences(pairKey:String, listener:DifferencingListener, origin:MatchOrigin) : Unit

  /**
   * Requests that the policy scan the upstream participants for the given pairing. Differences that are
   * detected will be reported to the listener provided.
   * @throws If the shouldRun variable is set to false, this will throw a ScanCancelledException
   */
  def scanUpstream(pairKey:String, writer: LimitedVersionCorrelationWriter,
                   participant:UpstreamParticipant, listener:DifferencingListener,
                   handle:FeedbackHandle)

  /**
   * Requests that the policy scan the downstream participants for the given pairing. Differences that are
   * detected will be reported to the listener provided.
   * @throws If the shouldRun variable is set to false, this will throw a ScanCancelledException
   */
  def scanDownstream(pairKey:String, writer: LimitedVersionCorrelationWriter,
                     us:UpstreamParticipant, ds:DownstreamParticipant,
                     listener:DifferencingListener, handle:FeedbackHandle)

}

/**
 * This provides an invoker with the ability to notify an invokee that a submitted task should be cancelled.
 * In addition to this, the invokee can report the current status back to the invoker.
 */
trait FeedbackHandle {
  /**
   * This cancels the current running task.
   */
  def cancel()

  /**
   * This indicates whether the current running task has been cancelled.
   */
  def isCancelled : Boolean

  // TODO [#249] This is just a definition ATM - this should be plumbed
  /**
   * This is a conduit that allows the invoker to receive the latest status as a string.
   */
  def logStatus(status:String)

}

/**
 * Thrown when a scan has been cancelled.
 */
class ScanCancelledException(pairKey:String) extends Exception(pairKey)