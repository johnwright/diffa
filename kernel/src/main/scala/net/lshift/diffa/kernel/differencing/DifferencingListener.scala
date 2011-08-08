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
  def onMismatch(id:VersionID, lastUpdated:DateTime, upstreamVsn:String, downstreamVsn:String, origin:MatchOrigin, level:DifferenceFilterLevel)
  
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
  def onMatch(id:VersionID, vsn:String, origin:MatchOrigin)
}

/**
 * Utilities for understanding differences.
 */
object DifferenceUtils {
  /**
   * Utility function to establish whether a mismatch is due to either
   * <li>
   *   <ul>The upstream version is missing</ul>
   *   <ul>The downstream version is missing</ul>
   *   <ul>The two versions are different</ul>
   * </li>
   */
  def differenceType(upstreamVsn: String, downstreamVsn: String) : DifferenceType = {
    if (null == upstreamVsn || upstreamVsn.isEmpty) {
      UpstreamMissing
    }
    else if (null == downstreamVsn || downstreamVsn.isEmpty) {
      DownstreamMissing
    }
    else {
      ConflictingVersions
    }
  }
}

/**
 * Defines the type of a difference.
 */
abstract class DifferenceType

/**
 * Occurs when the upstream  version for an entity does not exist.
 */
case object UpstreamMissing extends DifferenceType

/**
 * Occurs when the upstream version for an entity does not exist.
 */
case object DownstreamMissing extends DifferenceType

/**
 * Occurs when the versions for the upstream and downstream for an entity are different.
 */
case object ConflictingVersions extends DifferenceType


/**
 * Describes what level of filtering has been applied to the difference.
 */
abstract class DifferenceFilterLevel

/**
 * This difference is unfiltered, and has been emitted as the direct result of a system action.
 */
case object Unfiltered extends DifferenceFilterLevel

/**
 * This difference has been filtered by the event matcher, and should be considered of high quality.
 */
case object MatcherFiltered extends DifferenceFilterLevel


/**
 * Defines the type of origin of a match event.
 */
abstract class MatchOrigin

/**
 * The match event originates from the expiration of a live sliding window.
 */
case object LiveWindow extends MatchOrigin

/**
 * The match event originates from a scan
 */
case object TriggeredByScan extends MatchOrigin

/**
 * The match event originates due to system boot.
 */
case object TriggeredByBoot extends MatchOrigin