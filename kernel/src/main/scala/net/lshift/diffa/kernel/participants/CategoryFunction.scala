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

import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalDate

// TODO [#2] How does this function know what argument type it can work with?

case class IntermediateResult(lower:AnyRef, upper:AnyRef, next:CategoryFunction) {
  def toSeq : Seq[String] = Seq(lower.toString, upper.toString)
}

trait CategoryFunction {

  def evaluate(partition:String) : Option[IntermediateResult]
  def shouldBucket() : Boolean
}

case class IndividualCategoryFunction extends CategoryFunction {
  def evaluate(partition:String) = None
  def shouldBucket() = false
}

case class DateCategoryFunction extends CategoryFunction {

  def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(partition).toLocalDate
    val (start,end) = (point,point)
    val (s,e) = align(start,end)//(start.toDateTimeAtStartOfDay, end.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))
    Some(IntermediateResult(s, e, DateCategoryFunction()))
  }

  def align(s:LocalDate, e:LocalDate) = (s.toDateTimeAtStartOfDay, e.toDateTimeAtStartOfDay.plusDays(1).minusMillis(1))

  def shouldBucket() = true
}

case class DailyCategoryFunction() extends DateCategoryFunction {
  override def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(partition).toLocalDate
    val (start,end) = (point,point)
    val (s,e) = align(start,end)
    Some(IntermediateResult(s, e, IndividualCategoryFunction()))
  }
  override def shouldBucket() = true
}

case class MonthlyCategoryFunction() extends DateCategoryFunction {

  override def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy-MM").parseDateTime(partition).toLocalDate
    val (start,end) = (point.withDayOfMonth(1), point.plusMonths(1).minusDays(1))
    val (s,e) = align(start,end)
    Some(IntermediateResult(s, e, DailyCategoryFunction()))
  }

  override def shouldBucket() = true
}

case class YearlyCategoryFunction() extends DateCategoryFunction {

  override def evaluate(partition:String) = {
    val point = DateTimeFormat.forPattern("yyyy").parseDateTime(partition).toLocalDate
    val (start,end) = (point.withMonthOfYear(1).withDayOfMonth(1), point.withMonthOfYear(12).withDayOfMonth(31))
    val (s,e) = align(start,end)
    Some(IntermediateResult(s, e, MonthlyCategoryFunction()))
  }

  override def shouldBucket() = true
}