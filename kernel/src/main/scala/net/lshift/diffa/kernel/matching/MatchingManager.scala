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

import java.io.Closeable

/**
 * Keeps track of event matchers and updates them upon receiving a notification of changes in ConfigStore.
 */
trait MatchingManager extends Closeable {
  /**
   * Adds a listener that will be informed of all matching status events.
   */
  def addListener(l:MatchingStatusListener)

  /**
   * Returns a matcher for the given pair key if it exists; None otherwise..
   */
  def getMatcher(pairKey:String):Option[EventMatcher]

  /**
   * Handler for new pair creation or update of an existing one. This method should
   * recreate/keep intact all set-up matcher listeners before the update.
   */
  def onUpdatePair(pairKey:String):Unit

  /**
   * Handler for pair deletion.
   */
  def onDeletePair(pairKey:String):Unit
}