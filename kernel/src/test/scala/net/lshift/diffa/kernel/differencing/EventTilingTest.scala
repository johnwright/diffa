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

import net.lshift.diffa.kernel.differencing.EventTilingTest.TileScenario
import net.lshift.diffa.kernel.config.DiffaPairRef
import net.lshift.diffa.kernel.events.VersionID
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.junit.experimental.theories.{Theories, Theory, DataPoint}
import org.junit.Assert._

@RunWith(classOf[Theories])
class EventTilingTest {
  
  val diffStore:DomainDifferenceStore = HibernateDomainDifferenceStoreTest.differenceStore



  @Theory
  def shouldTileEvents(scenario:TileScenario) = {    
    scenario.events.foreach(e => diffStore.addReportableUnmatchedEvent(e.id, e.timestamp, "", "", e.timestamp))
    val tiles = diffStore.retrieveTiledEvents("domain1", scenario.zoomLevel)
    assertEquals(scenario.tiles, tiles)
  }
  
}

object EventTilingTest {

  val d1 = "d1"
  
  val rawEvents = Seq(
    ReportableEvent(id = VersionID(DiffaPairRef("pair2", d1), "id2"), timestamp = new DateTime())
  )
  
  @DataPoint def level0 = TileScenario(d1, 0, rawEvents, Map())

  case class ReportableEvent(
    id:VersionID,
    timestamp:DateTime
  )
  
  case class TileScenario(
    domain:String,
    zoomLevel:Int,
    events:Seq[ReportableEvent],
    tiles:Map[String,TileSet]
  )
  
}