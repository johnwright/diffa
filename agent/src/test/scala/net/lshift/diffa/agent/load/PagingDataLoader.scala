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
package net.lshift.diffa.agent.load

import net.lshift.diffa.messaging.json.ChangesRestClient
import net.lshift.diffa.kernel.events.UpstreamChangeEvent
import org.joda.time.DateTime
import net.lshift.diffa.kernel.config.RangeCategoryDescriptor
import scala.collection.JavaConversions._
import net.lshift.diffa.tools.client.{DifferencesRestClient, ConfigurationRestClient}
import org.junit.Assert._
import net.lshift.diffa.kernel.client.DifferencesClient
import net.lshift.diffa.kernel.differencing.{SessionEvent, SessionScope}
import com.eaio.uuid.UUID

/**
 * Utility class to load lots of unmatched events into the agent.
 */
object PagingDataLoader {

  def main(args: Array[String]) {
    val (size, hours, pair) = args match {
      case Array(s,h,p) => (s.toInt, h.toInt,p)
      case _            => (100, 2, new UUID().toString)
    }
    loadData(size, hours, pair)
  }

  def loadData(size:Int, hours:Int, pair:String) = {
    val host = "http://localhost:19093/diffa-agent/"

    println("Loading %s events onto %s".format(size,host))

    val configClient = new ConfigurationRestClient(host)
    val changesClient = new ChangesRestClient(host)
    val diffsClient = new DifferencesRestClient(host)

    val up = "up"
    val down = "down"

    val group = "group"
    val content = "application/json"

    val categories = Map("bizDate" -> new RangeCategoryDescriptor("datetime"))

    configClient.declareEndpoint(up, host, content, null, null, true, categories)
    configClient.declareEndpoint(down, host, content, null, null, true, categories)
    configClient.declareGroup(group)
    configClient.declarePair(pair, "same", 0, up, down, group)

    val start = new DateTime().minusHours(hours)

    for (i <- 1 to size) {
      val timestamp = start.plusMinutes(i)
      val id = "id_" + i
      val version = "vsn_" + i
      changesClient.onChangeEvent(UpstreamChangeEvent(up, id, List(start.toString), timestamp, version))
    }

    Thread.sleep(1000)

    val from = start.minusHours(1)
    val until = start.plusHours(1)

    val sessionId = diffsClient.subscribe(SessionScope.forPairs(pair), from, until)

    def firstPage(client:DifferencesClient) = client.page(sessionId, start, start.plusHours(1), 0, 10).toSeq
    def secondPage(client:DifferencesClient) = client.page(sessionId, start, start.plusHours(1), 10, 10).toSeq

    println("First page:")
    tryAgain(diffsClient, firstPage).foreach(println(_))

    println("Second page:")
    tryAgain(diffsClient, secondPage).foreach(println(_))

  }

  def tryAgain(client:DifferencesClient, poll:DifferencesClient => Seq[SessionEvent], n:Int = 10, wait:Int = 100) : Seq[SessionEvent]= {
    var i = n
    var diffs = poll(client)
    while(diffs.isEmpty && i > 0) {
      Thread.sleep(wait)

      diffs = poll(client)
      i-=1
    }
    assertNotNull(diffs)
    diffs
  }

}