/**
 * Copyright (C) 2010 LShift Ltd.
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

import org.joda.time.DateTime
import net.lshift.diffa.kernel.participants.{DateCategoryFunction, RangeQueryConstraint}

/**
 * TODO [#2]
 * This is an overflow class from the refactoring - it shouldn't live here permanently
 */
case class DateRangeConstraint2(start:DateTime, end:DateTime) extends RangeQueryConstraint {

  override def function = DateCategoryFunction()
  override def category = "date"
  override def values = List[String](start.toString, end.toString)

  //override def nextQueryAction(partition:String) = {
//    val point = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(partition).toLocalDate
//    val (start,end) = (point,point)
//    val (s,e) = (start.toDateTimeAtStartOfDay, end.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))
    //function.evaluate(partition)
  //}
}