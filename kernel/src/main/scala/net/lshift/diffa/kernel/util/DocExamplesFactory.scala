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
import net.lshift.diffa.kernel.differencing.{MatchState, SessionEvent}
import net.lshift.diffa.kernel.client.Actionable
import net.lshift.diffa.kernel.frontend.wire.InvocationResult

/**
 * Factory that returns a map of example usages of classes for doc generation.
 */
class DocExamplesFactory {

  val categoryDescriptor = new RangeCategoryDescriptor("datetime")
  val up = new Endpoint(name = "upstream-system", scanUrl = "http://acme.com/upstream/scan", contentType = "application/json", categories = Map("bizDate" -> categoryDescriptor))
  val down = new Endpoint(name = "downstream-system", scanUrl = "http://acme.com/downstream/scan", contentType = "application/json", categories = Map("bizDate" -> categoryDescriptor))

  val pair = Pair("pair-id", up, down, "correlated", 120)

  val repair = RepairAction(name = "resend", url = "http://acme.com/repairs/resend/{id}", scope = "entity", pairKey = "pairKey")

  val escalation = Escalation(name = "some-escalation", pairKey = "pairKey", action = "resend", actionType = "repair", event = "downstream-missing", origin = "scan")

  val actionable = Actionable(name = "resend", scope = "entity", path = "/actions/pairKey/resend/entity/${id}", pairKey = "pairKey")

  val result = InvocationResult(code = "200", output = "OK")

  val user = User(name = "joe.public", "joe.public@acme.com")

  def getExamples: java.util.Map[Class[_], Object] = {
    val map = new java.util.HashMap[Class[_], Object]

    map.put(classOf[Endpoint], up)
    map.put(classOf[Pair], pair)
    map.put(classOf[PairDef], new PairDef("pairKey","versionPolicyName",120,"upstreamName","downstreamName","0 15 10 ? * *"))
    map.put(classOf[SessionEvent], SessionEvent("6f72b9",VersionID("pairKey", "4f8a99"), new DateTime(), MatchState.UNMATCHED, "upstreamV", "downstreamV"))
    map.put(classOf[RepairAction], repair)
    map.put(classOf[Escalation], escalation)
    map.put(classOf[Actionable], actionable)
    map.put(classOf[InvocationResult], result)
    map.put(classOf[User], user)

    map
  }

}