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

package net.lshift.diffa.kernel.frontend

import net.lshift.diffa.kernel.matching.{MatchingStatusListener, MatchingManager}
import java.lang.String
import net.lshift.diffa.kernel.events.VersionID

/**
 * Frontend for monitoring matches that occur.
 */
class Matches(val matchingManager: MatchingManager) {
  /**
   * Starts watching for matching events being emitted by the given pair.
   */
  def watch(pairKey:String, filter:MatchingFilter, listener:MatchingStatusListener) {
    matchingManager.getMatcher(pairKey) match {
      case None =>
        throw new MatchingNotEnabledException(pairKey)
      case Some(m) =>
        m.addListener(new ListenerProxy(filter, listener))
    }
  }

  private class ListenerProxy(filter:MatchingFilter, l:MatchingStatusListener) extends MatchingStatusListener {
    def onPaired(id: VersionID, vsn: String) =
      if (filter.watchPairing) l.onPaired(id, vsn)
    def onUpstreamExpired(id: VersionID, vsn: String) =
      if (filter.watchUpstreamExpiry) l.onUpstreamExpired(id, vsn)
    def onDownstreamExpired(id: VersionID, vsn: String) =
      if (filter.watchDownstreamExpiry) l.onDownstreamExpired(id, vsn)
  }
}

class MatchingFilter(
    val watchPairing:Boolean = false,
    val watchUpstreamExpiry:Boolean = false,
    val watchDownstreamExpiry:Boolean = false
)

class MatchingNotEnabledException(val pair:String)
    extends Exception("Matching is not enabled for the pair " + pair)