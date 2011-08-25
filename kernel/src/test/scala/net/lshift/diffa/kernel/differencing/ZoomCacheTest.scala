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

import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.experimental.theories.{DataPoints, Theories, Theory}
import ZoomCache._
import net.lshift.diffa.kernel.differencing.ZoomCacheTest.IndexScenario
import net.lshift.diffa.kernel.differencing.ObservationDateTest.ObservationScenario
import org.joda.time.{Interval, DateTime, Period}
import org.junit.{After, Before, Test}
import net.lshift.diffa.kernel.differencing.QueryIntervalTest.QueryScenario


@RunWith(classOf[Theories])
class ZoomCacheTest {

  @Test(expected = classOf[InvalidZoomLevelException])
  def shouldRejectZoomLevelThatIsTooFine { index(QUARTER_HOURLY + 1) }

  @Test(expected = classOf[InvalidZoomLevelException])
  def shouldRejectZoomLevelThatIsTooCoarse { index(DAILY - 1) }

  @Test(expected = classOf[InvalidObservationDateException])
  def shouldRejectInvalidObservationDate {
    val timestamp = new DateTime()
    ZoomCache.indexOf(timestamp, timestamp.plusSeconds(1), QUARTER_HOURLY)
  }

  @Theory
  def eventTimeShouldProduceIndex(s:IndexScenario) = {
    val observationTime = new DateTime()
    val eventTime = observationTime.minus(s.eventLag)
    assertEquals(s.index, ZoomCache.indexOf(observationTime, eventTime, s.zoomLevel))
  }


  private def index(level:Int) = ZoomCache.indexOf(new DateTime(), new DateTime(), level)
}

object ZoomCacheTest {

  @DataPoints def expectedIndexes = Array(
    IndexScenario(QUARTER_HOURLY, new Period(0,14,59,999), 0),
    IndexScenario(QUARTER_HOURLY, new Period(0,15,0,0), 1),
    IndexScenario(HALF_HOURLY, new Period(0,29,59,999), 0),
    IndexScenario(HALF_HOURLY, new Period(0,30,0,0), 1),
    IndexScenario(HOURLY, new Period(0,59,59,999), 0),
    IndexScenario(HOURLY, new Period(1,0,0,0), 1),
    IndexScenario(TWO_HOURLY, new Period(1,59,59,999), 0),
    IndexScenario(TWO_HOURLY, new Period(2,0,0,0), 1),
    IndexScenario(FOUR_HOURLY, new Period(3,59,59,999), 0),
    IndexScenario(FOUR_HOURLY, new Period(4,0,0,0), 1),
    IndexScenario(DAILY, new Period(23,59,59,999), 0),
    IndexScenario(DAILY, new Period(24,0,0,0), 1)
  )

  case class IndexScenario(zoomLevel:Int, eventLag:Period, index:Int)

}

@RunWith(classOf[Theories])
class ObservationDateTest {

  @Theory
  def observationDateShouldAlign(s:ObservationScenario) = {
    assertEquals(s.nearestObservationDate, ZoomCache.nearestObservationDate(s.pointInTime))
  }

}

object ObservationDateTest {

  @DataPoints def expectedObservationDates = Array(
    ObservationScenario(new DateTime(1982,4,19,10,0,0,0), new DateTime(1982,4,19,10,15,0,0)),
    ObservationScenario(new DateTime(1982,4,19,10,23,0,0), new DateTime(1982,4,19,10,30,0,0)),
    ObservationScenario(new DateTime(1982,4,19,10,44,59,999), new DateTime(1982,4,19,10,45,0,0)),
    ObservationScenario(new DateTime(1982,4,19,10,53,0,0), new DateTime(1982,4,19,11,0,0,0))
  )

  case class ObservationScenario(pointInTime:DateTime, nearestObservationDate:DateTime)
}

@RunWith(classOf[Theories])
class QueryIntervalTest {

  @Theory
  def observationDateShouldAlign(s:QueryScenario) = {
    assertEquals(s.interval, ZoomCache.intervalFromIndex(s.index, s.level, s.pointInTime) )
  }

}

object QueryIntervalTest {

  @DataPoints def expectedQueries = Array(
    QueryScenario(new DateTime(2078,11,3,18,19,34,882), 4, QUARTER_HOURLY,
      new Interval(new DateTime(2078,11,3,17,15,0,0), new DateTime(2078,11,3,17,30,0,0)))
  )

  case class QueryScenario(pointInTime:DateTime, index:Int, level:Int, interval:Interval)
}