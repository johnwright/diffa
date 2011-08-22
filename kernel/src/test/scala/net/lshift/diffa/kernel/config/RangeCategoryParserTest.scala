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

package net.lshift.diffa.kernel.config

import org.junit.Test
import org.junit.Assert._
import net.lshift.diffa.participant.scanning.TimeRangeConstraint
import org.joda.time.{DateTimeZone, DateTime}

class RangeCategoryParserTest {

  @Test
  def localDatesShouldWiden = {
    val descriptor = new RangeCategoryDescriptor("datetime", "2004-06-01", "2004-06-30")
    val lower = new DateTime(2004,6,1,0,0,0,0, DateTimeZone.UTC)
    val upper = new DateTime(2004,6,30,23,59,59,999, DateTimeZone.UTC)
    compare(descriptor, lower, upper)
  }

  @Test
  def dateTimesShouldNotWiden = {
    val descriptor = new RangeCategoryDescriptor("datetime", "1982-03-27T15:19:23.283Z", "1982-04-11T09:47:11.519Z")
    val lower = new DateTime(1982,3,27,15,19,23,283, DateTimeZone.UTC)
    val upper = new DateTime(1982,4,11,9,47,11,519, DateTimeZone.UTC)
    compare(descriptor, lower, upper)
  }

  def compare(descriptor:RangeCategoryDescriptor, lower:DateTime, upper:DateTime) = {
    val constraint = RangeCategoryParser.buildConstraint("foo", descriptor)
    assertEquals(new TimeRangeConstraint("foo", lower, upper), constraint)
  }
}