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

package net.lshift.diffa.kernel.participants

import org.joda.time.LocalDate
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}

// TODO [#2] How does this function know what argument type it can work with?

case class IntermediateResult(lower:AnyRef, upper:AnyRef, next:CategoryFunction) {
  def toSeq : Seq[String] = Seq(lower.toString, upper.toString)
}

trait CategoryFunction {

  def evaluate(partition:String) : Option[IntermediateResult]
  def shouldBucket() : Boolean
  def partition(value:String) : String
}

/**
 * A special type of function that indicates that no further
 * partitioning should take place.
 * TODO [#2] Can this be an object rather than an instance?
 */
case class IndividualCategoryFunction extends CategoryFunction {
  def evaluate(partition:String) = None
  def shouldBucket() = false
  def partition(value:String) = {
    value
  }
}

// TODO [#2] Potentially deprecate this function
case class DateCategoryFunction extends CategoryFunction {

  def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(partition).toLocalDate
    val (start,end) = (point,point)
    val (s,e) = align(start,end)//(start.toDateTimeAtStartOfDay, end.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))
    Some(IntermediateResult(s, e, DateCategoryFunction()))
  }

  def align(s:LocalDate, e:LocalDate) = (s.toDateTimeAtStartOfDay, e.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))

  def shouldBucket() = true
  def partition(value:String) = value
}

case class DailyCategoryFunction() extends DateCategoryFunction {
  override def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(partition).toLocalDate
    val (start,end) = (point,point)
    val (s,e) = align(start,end)
    Some(IntermediateResult(s, e, IndividualCategoryFunction()))
  }

  override def shouldBucket() = true

  override def partition(value:String) = {
    val fmt = ISODateTimeFormat.dateTime()
    val date = fmt.parseDateTime(value)
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    formatter.print(date)
  }
}

case class MonthlyCategoryFunction() extends DateCategoryFunction {

  val formatter = DateTimeFormat.forPattern("yyyy-MM")
  val iso = ISODateTimeFormat.dateTime()

  override def evaluate(partition:String) = {
    val point = formatter.parseDateTime(partition).toLocalDate
    val (start,end) = (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
    val (s,e) = align(start,end)
    Some(IntermediateResult(s, e, DailyCategoryFunction()))
  }

  override def shouldBucket() = true
  override def partition(value:String) = {
    val date = iso.parseDateTime(value)
    formatter.print(date)
  }
}

case class YearlyCategoryFunction() extends DateCategoryFunction {

  val formatter = DateTimeFormat.forPattern("yyyy")
  val iso = ISODateTimeFormat.dateTime()

  override def evaluate(partition:String) = {
    val point = formatter.parseDateTime(partition).toLocalDate
    val (start,end) = (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
    val (s,e) = align(start,end)
    Some(IntermediateResult(s, e, MonthlyCategoryFunction()))
  }

  override def shouldBucket() = true
  override def partition(value:String) = {
    val date = iso.parseDateTime(value)
    formatter.print(date)
  }
}