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

package net.lshift.diffa.kernel.matching

import net.lshift.diffa.kernel.events.VersionID

/**
 * Trait supported by classes that wish to receive matching events.
 */
trait MatchingStatusListener {
  type AckCallback = Function0[Unit]

  /**
   * Called when a source and destination event have been successfully paired.
   */
  def onPaired(id:VersionID, vsn:String)

  /**
   * Called when a source message has expired without being matched.
   */
  def onUpstreamExpired(id:VersionID, vsn:String)

  /**
   * Called when a destination message has expired without being matched.
   */
  def onDownstreamExpired(id:VersionID, vsn:String)
}
