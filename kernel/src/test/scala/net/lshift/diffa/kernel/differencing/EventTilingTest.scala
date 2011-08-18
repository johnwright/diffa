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
import scala.collection.JavaConversions._

@RunWith(classOf[Theories])
class EventTilingTest {
  
  val diffStore:DomainDifferenceStore = HibernateDomainDifferenceStoreTest.differenceStore



  @Theory
  def shouldTileEvents(scenario:TileScenario) = {    
    scenario.events.foreach(e => diffStore.addReportableUnmatchedEvent(e.id, e.timestamp, "", "", e.timestamp))
    scenario.zoomLevels.foreach{ case (zoom, expected) => {
      val tiles = diffStore.retrieveTiledEvents("domain1", zoom)
      assertEquals(tiles, expected)
    }}
  }
  
}

object EventTilingTest {


  @DataPoint def level0 = TileScenario("domain",
      Seq(
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id1"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id2"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id3"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id4"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id5"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id6"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id7"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id8"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id9"), timestamp = new DateTime()),
        ReportableEvent(id = VersionID(DiffaPairRef("pair1", "domain1"), "id10"), timestamp = new DateTime())
      ),
      Map(0 -> Map("pair1" -> TileSet(Map(0 -> 1, 1 -> 2, 2 -> 3, 4 -> 4))
    ))
  )

  case class ReportableEvent(
    id:VersionID,
    timestamp:DateTime
  )
  
  case class TileScenario(
    domain:String,
    events:Seq[ReportableEvent],
    zoomLevels:Map[Int,Map[String,TileSet]]
  )
  
}