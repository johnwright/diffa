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

import collection.immutable.HashSet

/**
 * Provides the details of the scope of a session.
 */
class SessionScope(private val pairs:HashSet[String]) {
  def includedPairs = pairs.toSeq

  def includes(pair:String) = {
    if (pairs.size == 0) {
      true
    } else {
      pairs.contains(pair)
    }
  }

  override def toString = "pairs:" + pairs.toArray.foldLeft("") {
    case ("", p)  => p
    case (acc, p) => acc + "," + p
  }
}
object SessionScope {
  val all = new SessionScope(HashSet.empty)
  def forPairs(pairs:String*) = new SessionScope(HashSet.empty ++ pairs)
}