package net.lshift.diffa.kernel.differencing

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

import org.junit.runner.RunWith
import org.junit.Assert._
import net.lshift.diffa.kernel.differencing.SpanningTileGroupTest.Scenario
import ZoomCache._
import org.junit.experimental.theories.{DataPoint, Theory, Theories}
import org.joda.time.{Interval, DateTime}

@RunWith(classOf[Theories])
class SpanningTileGroupTest {

  @Theory
  def shouldContainTileGroup(s:Scenario) = {
    assertEquals(s.timestamps, ZoomCache.containingTileGroupEdges(s.interval, s.zoomLevel))
  }
}

object SpanningTileGroupTest {

  @DataPoint def daily = Scenario(DAILY,
                                  new Interval(new DateTime(2004,3,6,17,16,58,888),new DateTime(2004,3,8,9,10,11,222)),
                                  new DateTime(2004,3,6,0,0,0,0),new DateTime(2004,3,7,0,0,0,0),new DateTime(2004,3,8,0,0,0,0))

  @DataPoint def eightHourly = Scenario(EIGHT_HOURLY,
                                        new Interval(new DateTime(1996,10,5,4,14,18,745),new DateTime(1996,10,5,13,45,22,496)),
                                        new DateTime(1996,10,5,0,0,0,0))

  @DataPoint def fourHourly = Scenario(FOUR_HOURLY,
                                       new Interval(new DateTime(1987,1,24,21,23,34,632),new DateTime(1987,1,25,1,19,29,712)),
                                       new DateTime(1987,1,24,0,0,0,0),new DateTime(1987,1,25,0,0,0,0))

  @DataPoint def twoHourly = Scenario(TWO_HOURLY,
                                      new Interval(new DateTime(2033,12,12,2,19,45,125),new DateTime(2033,12,14,4,13,44,333)),
                                      new DateTime(2033,12,12,0,0,0,0),new DateTime(2033,12,13,0,0,0,0), new DateTime(2033,12,14,0,0,0,0))

  @DataPoint def hourly = Scenario(HOURLY,
                                   new Interval(new DateTime(2008,2,28,19,56,33,231),new DateTime(2008,3,1,21,6,17,843)),
                                   new DateTime(2008,2,28,0,0,0,0),new DateTime(2008,2,29,0,0,0,0),new DateTime(2008,3,1,0,0,0,0))

  @DataPoint def halfHourly = Scenario(HALF_HOURLY,
                                       new Interval(new DateTime(1999,11,29,9,31,45,123),new DateTime(1999,12,1,10,33,45,997)),
                                       new DateTime(1999,11,29,0,0,0,0), new DateTime(1999,11,30,0,0,0,0), new DateTime(1999,12,1,0,0,0,0))

  @DataPoint def quarterHourly = Scenario(QUARTER_HOURLY,
                                          new Interval(new DateTime(2007,8,18,23,46,7,463),new DateTime(2007,8,20,13,14,44,732)),
                                          new DateTime(2007,8,18,0,0,0,0), new DateTime(2007,8,19,0,0,0,0), new DateTime(2007,8,20,0,0,0,0))

  case class Scenario (zoomLevel:Int, interval:Interval, timestamps:DateTime*)

}