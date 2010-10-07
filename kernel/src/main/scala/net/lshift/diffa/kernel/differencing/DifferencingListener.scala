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

import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime

/**
 * Trait implemented by classes that wish to be informed about differencing events that occur.
 */
trait DifferencingListener {
  /**
   * Raised when an upstream and downstream version disagree (either by the values being different, or one being
   * missing).
   */
  def onMismatch(id:VersionID, lastUpdated:DateTime, upstreamVsn:String, downstreamVsn:String)
  
  /**
   * Raised when an upstream and downstream have entered a matched version state. This callback will only be
   * triggered during the monitoring phase of a live report - matches determined during the initial differencing
   * phase will not be reported.
   *
   * Note that the lastUpdated timestamp is not provided because this information would irrelevant to an end consumer:
   * - They would only receive a match event that follows a previously report mismatch, so the only action is to
   *   remove their representation of that mismatch
   * - If there were older events when a client starts a session, only the mismatch events would be reported anyway. 
   *
   */
  def onMatch(id:VersionID, vsn:String)
}