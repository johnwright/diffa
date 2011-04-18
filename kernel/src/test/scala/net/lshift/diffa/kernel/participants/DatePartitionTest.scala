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

package net.lshift.diffa.kernel.participants

import org.junit.Test
import org.junit.Assert._
import org.joda.time.DateTime

class DatePartitionTest {

  @Test
  def dailyPartition = {
    val date = new DateTime(2012, 5, 5 ,8, 4, 2, 0)
    val function = DailyCategoryFunction
    val partition = function.owningPartition(date.toString)
    assertEquals("2012-05-05", partition)
  }

  @Test
  def monthlyPartition = {
    val date = new DateTime(2017, 9, 17 , 9, 9, 6, 0)
    val function = MonthlyCategoryFunction
    val partition = function.owningPartition(date.toString)
    assertEquals("2017-09", partition)
  }
  
  @Test
  def yearlyPartition = {
    val date = new DateTime(1998, 11, 21 , 15, 49, 55, 0)
    val function = YearlyCategoryFunction
    val partition = function.owningPartition(date.toString)
    assertEquals("1998", partition)
  }

  @Test
  def shouldParseFullIsoDate = {
    val date = "1998-11-21T15:49:55.000Z"
    val function = DailyCategoryFunction
    val partition = function.owningPartition(date)
    assertEquals("1998-11-21", partition)
  }

  @Test
  def shouldParseYYYYMMddDate = {
    val date = "1998-11-21"
    val function = DailyCategoryFunction
    val partition = function.owningPartition(date)
    assertEquals("1998-11-21", partition)
  }

  @Test(expected=classOf[InvalidAttributeValueException])
  def shouldThrowInvalidCategoryExceptionIfValueIsNotDate {
    DailyCategoryFunction.owningPartition("NOT_A_DATE")
  }

  @Test
  def descendFromYearlyCategoryFunction {
    val expectedStart = new DateTime(1986, 01, 01, 0, 0, 0, 0)
    val expectedEnd = new DateTime(1987, 01, 01, 0, 0, 0, 0).minusMillis(1)
    assertEquals(Some(MonthlyCategoryFunction), YearlyCategoryFunction.descend)
    assertEquals(DateTimeRangeConstraint("someDate", expectedStart, expectedEnd),
      YearlyCategoryFunction.constrain("someDate", "1986"))
  }

}