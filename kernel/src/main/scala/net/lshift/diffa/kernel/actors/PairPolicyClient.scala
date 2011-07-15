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

package net.lshift.diffa.kernel.actors

import net.jcip.annotations.ThreadSafe
import net.lshift.diffa.kernel.events.PairChangeEvent
import net.lshift.diffa.kernel.differencing.{PairSyncListener, DifferencingListener}

/**
 * This is a thread safe entry point to an underlying version policy.
 */
@ThreadSafe
trait PairPolicyClient {

  /**
   * Propagates the change event to the underlying policy implementation in a serial fashion.
   */
  def propagateChangeEvent(event:PairChangeEvent) : Unit

  /**
   * Runs a replayUnmatchedDifferences report based on stored data for the given pair. Does not synchronise with the participants
   * beforehand - use <code>scanPair</code> to do the sync first.
   */
  def difference(pairKey:String)

  /**
   * Synchronises the participants belonging to the given pair, then generates a different report.
   * Activities are performed on the underlying policy in a thread safe manner, allowing multiple
   * concurrent operations to be submitted safely against the same pair concurrently.
   */
  def scanPair(pairKey:String) : Unit

  /**
   * Cancels any scan operation that may be in process.
   * This is a blocking call, so it will only return after all current and pending scans have been cancelled.
   */
  def cancelScans(pairKey:String) : Boolean
}