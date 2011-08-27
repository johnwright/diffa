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

import org.junit.runner.RunWith
import org.junit.Assert._
import net.lshift.diffa.kernel.differencing.ContainingIntervalTest.Scenario
import ZoomCache._
import org.junit.experimental.theories.{DataPoint, Theory, Theories}
import org.joda.time.{Interval, DateTime}

@RunWith(classOf[Theories])
class ContainingIntervalTest {

  @Theory
  def shouldContain(s:Scenario) = {
    assertEquals(s.interval, ZoomCache.containingInterval(s.timestamp, s.zoomLevel))
  }
}

object ContainingIntervalTest {

  @DataPoint def daily = Scenario(new DateTime(2004,3,6,17,16,58,888), DAILY,
                                  new Interval(new DateTime(2004,3,6,0,0,0,0),new DateTime(2004,3,7,0,0,0,0)))

  @DataPoint def eightHourly = Scenario(new DateTime(1996,10,5,4,14,18,745), EIGHT_HOURLY,
                                  new Interval(new DateTime(1996,10,5,0,0,0,0),new DateTime(1996,10,5,8,0,0,0)))

  @DataPoint def fourHourly = Scenario(new DateTime(1987,1,24,21,23,34,632), FOUR_HOURLY,
                                  new Interval(new DateTime(1987,1,24,20,0,0,0),new DateTime(1987,1,25,0,0,0,0)))

  @DataPoint def twoHourly = Scenario(new DateTime(2033,12,12,2,19,45,125), TWO_HOURLY,
                                  new Interval(new DateTime(2033,12,12,2,0,0,0),new DateTime(2033,12,12,4,0,0,0)))

  @DataPoint def hourly = Scenario(new DateTime(2008,2,28,19,7,6,198), HOURLY,
                                  new Interval(new DateTime(2008,2,28,19,0,0,0),new DateTime(2008,2,28,20,0,0,0)))

  @DataPoint def halfHourly = Scenario(new DateTime(1999,11,30,9,51,52,392), HALF_HOURLY,
                                  new Interval(new DateTime(1999,11,30,9,30,0,0),new DateTime(1999,11,30,10,0,0,0)))

  @DataPoint def quarterHourly = Scenario(new DateTime(2007,8,18,23,59,58,234), QUARTER_HOURLY,
                                  new Interval(new DateTime(2007,8,18,23,45,0,0),new DateTime(2007,8,19,0,0,0,0)))

  case class Scenario (timestamp:DateTime, zoomLevel:Int, interval:Interval)

}