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

import net.lshift.diffa.kernel.config.{ConfigStore, Pair}
import collection.mutable.{ListBuffer, HashMap}

/**
 * Keeps track of and updates Local event matchers for pair entries from ConfigStore.
 */
class LocalEventMatchingManager(configStore: ConfigStore) extends MatchingManager {
  private val reaper = new LocalEventMatcherReaper
  private val matchers = new HashMap[String, LocalEventMatcher]
  private val listeners = new ListBuffer[MatchingStatusListener]

  // Create a matcher for each pre-existing pair
  configStore.listGroups.foreach(g => g.pairs.foreach(p => {
    updateMatcher(p.key, p.matchingTimeout)
  }))

  def getMatcher(pairKey:String) = matchers.get(pairKey)

  def onUpdatePair(pairKey:String):Unit = {
    val pair = configStore.getPair(pairKey)

    pair.matchingTimeout match {
      case Pair.NO_MATCHING => removeMatcher(pairKey)
      case timeout => updateMatcher(pairKey, pair.matchingTimeout)
    }
  }

  def onDeletePair(pairKey:String) = {
    removeMatcher(pairKey)
  }

  def close: Unit = {
    matchers.values foreach (m => m.dispose)
    reaper.dispose
  }

  def addListener(l:MatchingStatusListener) {
    listeners.synchronized {
      listeners += l
      matchers.values.foreach(m => m.addListener(l))
    }
  }

  private def updateMatcher(pairKey:String, timeout:Int):Unit = {
    val newMatcher = new LocalEventMatcher(pairKey, timeout, reaper)

    matchers.remove(pairKey) match {
      case Some(matcher) => {
        // Recreate matcher with new window length but original listeners
        val listeners = matcher.listeners
        matcher.dispose
        listeners foreach (l => newMatcher.addListener(l))
      }
      case None => {
        // Apply all of our default listeners to the new matcher
        listeners.synchronized {
          listeners.foreach(l => newMatcher.addListener(l))
        }
      }
    }
    matchers(pairKey) = newMatcher
  }

  private def removeMatcher(pairKey:String):Unit = {
    val matcher = matchers.get(pairKey) match {
      case Some(matcher) => {
        matcher.dispose
        matchers -= pairKey
      }
      case None => // nothing to do
    }
  }
}