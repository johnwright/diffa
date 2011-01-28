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

package net.lshift.diffa.kernel.util

import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import scala.collection.Map
import scala.collection.JavaConversions._
import net.lshift.diffa.kernel.config._
import net.lshift.diffa.kernel.differencing.{ConstraintType, MatchState, SessionEvent}

/**
 * Factory that returns a map of example usages of classes for doc generation.
 */
class DocExamplesFactory {

  val categoryDescriptor = new CategoryDescriptor("date", ConstraintType.RANGE)
  val up = new Endpoint("upstream-system", "http://acme.com/upstream", "application/json", null, null, true, Map("bizDate" -> categoryDescriptor))
  val down = new Endpoint("downstream-system", "http://acme.com/downstream", "application/json", null, null, true, Map("bizDate" -> categoryDescriptor))

  val group = PairGroup("important-group")
  var pair = Pair("pair-id", up, down, group, "correlated", 120)

  def getExamples : java.util.Map[Class[_ <: Object], Object] = {
    val map = new java.util.HashMap[Class[_ <: Object], Object]

    map.put(classOf[Endpoint], up)
    map.put(classOf[Pair], pair)
    map.put(classOf[PairGroup], group)
    map.put(classOf[GroupContainer], GroupContainer(group, Array(pair)))
    map.put(classOf[PairDef], new PairDef("pairKey","versionPolicyName",120,"upstreamName","downstreamName","groupKey"))
    map.put(classOf[SessionEvent], SessionEvent("6f72b9",VersionID("pairKey", "4f8a99"), new DateTime(), MatchState.UNMATCHED, "upstreamV", "downstreamV"))

    map
  }

}